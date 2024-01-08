import asyncio
import aiohttp.web
from typing import Optional, Iterable
from os import umask, stat, geteuid
from sys import stderr
from pathlib import Path
from ssl import create_default_context, Purpose, TLSVersion, CERT_REQUIRED
from json import loads
from traceback import print_exc
from struct import Struct
from socket import SOL_SOCKET, SO_PEERCRED
from pwd import getpwuid

from .rpc import RPC
from .utils import *
from .common import loadconfig, DuplicateConfigfile


web = aiohttp.web

peercred_struct = Struct('3i')


class BaseClient(Initializer):
    ws: web.WebSocketResponse

    @weakproperty
    def server(self):
        raise RuntimeError("server property not initialized")


class Admin(BaseClient):
    @weakproperty
    def rpc(self):
        return RPC(
            self.ws,
            apply=self.apply,
            online=self.online,
            quit=self.quit,
        )

    async def online(self):
        return list(self.server.clients)

    async def apply(self, *names):
        clients = self.server.clients
        if names:
            targets = (clients[name] for name in names)
        else:
            targets = clients.values()

        await asyncio.gather(*(client.apply() for client in targets))

    async def quit(self):
        await self.server.done.set_result(None)

    async def __call__(self):
        async for _ in self.rpc:
            warn("binary blob received from client, disconnecting")
            break


class Client(BaseClient):
    facts: Optional[dict] = None
    name: str
    confpath: Path | str

    @weakproperty
    def rpc(self) -> RPC:
        return RPC(
            self.ws,
            facts=self.accept_facts,
        )

    @initializer
    def remotely_known_files(self) -> dict[str, tuple[int, int]]:
        return {}

    async def accept_facts(self, facts) -> None:
        self.facts = facts
        await self.apply()

    async def apply(self) -> None:
        warn("apply()")
        name = self.name
        rpc = self.rpc
        ws = self.ws
        confpath = self.confpath
        old_files: dict[str, tuple[int, int]] = {}
        new_files: dict[str, tuple[int, int]] = {}
        new_content: dict[str, bytes] = {}
        remotely_known_files = self.remotely_known_files

        def load_file(filename: str | Path):
            full_filename = confpath / normalize_path(filename)
            warn(full_filename)
            filename = str(filename)
            try:
                return new_content[filename]
            except KeyError:
                pass
            with open(full_filename, 'rb') as fh:
                st = stat(fh.fileno())
                st_mtime_size = (st.st_mtime_ns, st.st_size)
                content = fh.read()
            if remotely_known_files.get(filename) == st_mtime_size:
                old_files[filename] = st_mtime_size
            else:
                new_files[filename] = st_mtime_size
                new_content[filename] = content
            return content

        decree = loadconfig(name, load_file, facts=Object(self.facts or {}))

        required_resources: Iterable[str] = decree._required_resources
        for resource in required_resources:
            load_file(normalize_path(resource))

        stale_config_files = (
            remotely_known_files.keys() - old_files.keys() - new_files.keys()
        )
        if stale_config_files:
            await rpc.remote_command(
                'discard_files', *sorted(stale_config_files), rsvp=False
            )
        if new_content:
            await rpc.remote_command('accept_files', *new_content, rsvp=False)
            for content in new_content.values():
                await ws.send_bytes(content)

        remotely_known_files.clear()
        remotely_known_files.update(old_files)
        remotely_known_files.update(new_files)

        await rpc.remote_command('apply', name, rsvp=False)

    async def __call__(self) -> None:
        name = self.name
        clients = self.server.clients
        clients[name] = self
        try:
            async for _ in self.rpc:
                warn("binary blob received from client, disconnecting")
                break
        finally:
            clients.pop(name, None)


class Server(Initializer):
    clients: dict[str, Client] = {}
    confpath: Path

    async def fetch(self) -> None:
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            while True:
                await asyncio.sleep(1)

    async def client(self, request) -> web.StreamResponse:
        warn("new connection")

        # socket = request.get_extra_info('ssl_object')
        # cert_binary = socket.getpeercert(True)
        # from cryptography.x509 load load_der_x509_certificate
        # cert_x509 = load_der_x509_certificate(cert_binary)

        peercert = request.get_extra_info('peercert')
        if peercert is None:
            socket = request.get_extra_info('socket')
            creds = socket.getsockopt(SOL_SOCKET, SO_PEERCRED, peercred_struct.size)
            pid, uid, gid = peercred_struct.unpack(creds)
            if pid and uid == geteuid():
                warn(f"admin connected: {getpwuid(uid).pw_name}")
                ws = web.WebSocketResponse(heartbeat=60, compress=False)
                await ws.prepare(request)
                admin = Admin(ws=ws, server=self)
                await admin()
        else:
            commonNames = frozenset(
                value
                for rdn in peercert['subject']
                for key, value in rdn
                if key == 'commonName'
            )
            if len(commonNames) != 1:
                names = sorted(commonNames)
                warn(
                    f"confusing client certificate: {names=}",
                )
                return web.Response(status=403)
            (commonName,) = commonNames
            warn(f"got connection from {commonName}")

            clients = self.clients

            try:
                existing_client = clients[commonName]
            except KeyError:
                pass
            else:
                await existing_client.ws.close()
                return web.Response(status=409)

            ws = web.WebSocketResponse(heartbeat=60, compress=True)
            await ws.prepare(request)

            client = Client(name=commonName, ws=ws, server=self, confpath=self.confpath)
            await client()

        return web.Response(status=201)

    @initializer
    def done(self):
        return asyncio.get_running_loop().create_future()

    async def __call__(self, base_dir: Path):
        pki_dir = base_dir / 'pki'

        tls = create_default_context(
            Purpose.CLIENT_AUTH, cafile=pki_dir / 'ca' / 'certificate'
        )
        if tls.minimum_version < TLSVersion.TLSv1_3:
            tls.minimum_version = TLSVersion.TLSv1_3
        tls.load_cert_chain(base_dir / 'server.crt', base_dir / 'server.key')
        tls.verify_mode = CERT_REQUIRED

        app = web.Application()
        app.add_routes([web.get('/', self.client)])
        runner = web.AppRunner(app)
        await runner.setup()

        sites = [
            web.UnixSite(runner, str(base_dir / 'control')),
            web.TCPSite(runner, port=9999, ssl_context=tls),
        ]

        for site in sites:
            await site.start()

        await self.done

        for site in sites:
            await site.stop()


async def main(base_dir: Path):
    server = Server(confpath=base_dir)
    await server(base_dir)
