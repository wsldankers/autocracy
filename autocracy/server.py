import asyncio
from os import (
    environ,
    fwalk,
    geteuid,
    getuid,
    initgroups,
    open as os_open,
    setresgid,
    setresuid,
    stat,
    stat_result,
)
from pathlib import Path
from pwd import getpwnam, getpwuid
from socket import SO_PEERCRED, SOL_SOCKET
from ssl import CERT_REQUIRED, Purpose, TLSVersion, create_default_context
from stat import S_ISREG
from struct import Struct
from sys import setswitchinterval
from typing import Iterable, Optional, Union
from weakref import ref as weakref

import aiohttp.web

from .aiohttp import TCPSite, UnixSite
from .common import load_config, load_policy, load_tags
from .decrees.base import BaseRepository
from .rpc import RPC
from .utils import *

web = aiohttp.web

peercred_struct = Struct('3i')


def throw(exception):
    raise exception


class Repository(BaseRepository):
    root: Path

    @initializer
    def files(self) -> dict[str, tuple[bytes, stat_result]]:
        return {}

    def get_file(self, path: Union[Path, str]) -> bytes:
        normalized_path = str(normalize_path(path))
        files = self.files
        try:
            content, _ = files[normalized_path]
            return content
        except KeyError:
            pass
        with (self.root / normalized_path).open('rb') as fh:
            st = stat(fh.fileno())
            if not S_ISREG(st.st_mode):
                raise RuntimeError(f"{normalized_path} is not a file")
            content = fh.read()
        files[normalized_path] = (content, st)
        return content

    def get_files(self, path: Union[Path, str]) -> dict[str, bytes]:
        normalized_path = str(normalize_path(path))
        root = self.root
        files = self.files
        result: dict[str, bytes] = {}
        for parent, _, file_entries, dir_fd in fwalk(
            root / normalized_path, onerror=throw
        ):
            parent_path = Path(parent).relative_to(root)

            def opener(path, flags):
                return os_open(path, flags, dir_fd=dir_fd)

            for file_entry in file_entries:
                file_path = str(parent_path / file_entry)
                try:
                    content, _ = files[file_path]
                except KeyError:
                    with open(file_entry, 'rb', opener=opener) as fh:
                        st = stat(fh.fileno())
                        if not S_ISREG(st.st_mode):
                            warn(f"{file_path} is not a file, skipping")
                            continue
                        content = fh.read()
                    files[file_path] = (content, st)
                result[file_path] = content

        return result


class BaseClient(Initializer):
    ws: web.WebSocketResponse
    config: Object

    @weakproperty
    def server(self):
        raise RuntimeError("server property not initialized")

    @initializer
    def base_dir(self) -> Path:
        return Path(self.config['base_dir'])

    @initializer
    def repository_root(self) -> Path:
        return Path(self.config.get('repository_root', self.base_dir))


class Admin(BaseClient):
    @weakproperty
    def rpc(self):
        return RPC(
            self.ws,
            apply=self.apply,
            online=self.online,
            quit=self.quit,
            report=self.report,
        )

    async def report(self, name):
        return (self.server.clients[name].report,)

    async def online(self):
        return list(self.server.clients)

    async def apply(self, *names):
        clients = self.server.clients
        if names:
            target_names = set()
            tags = None
            for name in names:
                if name.startswith('@'):
                    if tags is None:
                        repository = Repository(root=self.repository_root)
                        tags = load_tags(repository.get_file, 'tags')
                    try:
                        tag = tags[name[1:]]
                    except KeyError:
                        warn(f"unknown tag {name!r}, skipping")
                    else:
                        target_names.update(tag)

            targets = (
                client for name, client in clients.items() if name in target_names
            )
        else:
            targets = clients.values()

        return [
            dict(
                zip(
                    (client.name for client in targets),
                    await asyncio.gather(*(client.apply() for client in targets)),
                )
            )
        ]

    async def quit(self):
        await self.server.done.set_result(None)

    async def __call__(self):
        async for _ in self.rpc:
            warn("binary blob received from client, disconnecting")
            break


class Client(BaseClient):
    report: Optional[dict] = None
    name: str

    @weakproperty
    def rpc(self) -> RPC:
        return RPC(self.ws, report=self.accept_report)

    @initializer
    def remotely_known_files(self) -> dict[str, stat_result]:
        return {}

    async def accept_report(self, report) -> None:
        self.report = report
        # await self.apply()

    async def apply(self) -> None:
        # warn("apply()")
        name = self.name

        repository = Repository(root=self.repository_root)

        report = Object(self.report or {})

        policy = load_policy(repository.get_file, name, report=report)
        policy._provision(repository)

        rpc = self.rpc

        remotely_known_files = self.remotely_known_files
        repository_files = repository.files

        stale_config_files = remotely_known_files.keys() - repository_files.keys()
        if stale_config_files:
            await rpc.remote_command(
                'discard_files', *sorted(stale_config_files), rsvp=False
            )

        new_content = {
            file: content
            for file, (content, st) in repository_files.items()
            if remotely_known_files.get(file) != st
        }

        if new_content:
            new_content_keys = sorted(new_content)
            await rpc.remote_command('accept_files', *new_content_keys, rsvp=False)
            ws = self.ws
            for key in new_content_keys:
                await ws.send_bytes(new_content[key])

        remotely_known_files.clear()
        for file, (_, st) in repository_files.items():
            remotely_known_files[file] = st

        (update_needed,) = await rpc.remote_command('apply', name)

        return update_needed

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
    config: Object
    sites: Iterable[web.BaseSite]

    @initializer
    def base_dir(self) -> Path:
        return Path(self.config['base_dir'])

    @initializer
    def port(self) -> int:
        return int(self.config.get('port', 443))

    @initializer
    def admin_users(self) -> frozenset[int]:
        return frozenset(
            user if isinstance(user, int) else getpwnam(user).pw_uid
            for user in self.config.get('admin_users', (geteuid(),))
        )

    @initializer
    def control_socket_path(self) -> str:
        return str(self.config.get('control_socket_path', self.base_dir / 'control'))

    async def fetch(self) -> None:
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            while True:
                await asyncio.sleep(1)

    async def client(self, request) -> web.StreamResponse:
        # warn("new connection")

        # socket = request.get_extra_info('ssl_object')
        # cert_binary = socket.getpeercert(True)
        # from cryptography.x509 import load_der_x509_certificate
        # cert_x509 = load_der_x509_certificate(cert_binary)

        peercert = request.get_extra_info('peercert')
        if peercert is None:
            socket = request.get_extra_info('socket')
            creds = socket.getsockopt(SOL_SOCKET, SO_PEERCRED, peercred_struct.size)
            pid, uid, gid = peercred_struct.unpack(creds)
            if pid and uid in self.admin_users:
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
                warn(f"confusing client certificate: {names=}")
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

            client = Client(name=commonName, ws=ws, server=self, config=self.config)
            await client()

        return web.Response(status=201)

    @initializer
    def done(self):
        return asyncio.get_running_loop().create_future()

    async def start(self):
        base_dir = self.base_dir
        pki_dir = base_dir / 'pki'

        tls = create_default_context(
            Purpose.CLIENT_AUTH, cafile=pki_dir / 'ca' / 'certificate'
        )
        if tls.minimum_version < TLSVersion.TLSv1_3:
            tls.minimum_version = TLSVersion.TLSv1_3
        tls.load_cert_chain(base_dir / 'server.crt', base_dir / 'server.key')
        tls.verify_mode = CERT_REQUIRED

        weakself = weakref(self)

        async def client(request):
            self = weakself()
            if self is None:
                return web.Response(status=500)
            else:
                return await self.client(request)

        app = web.Application()
        app.add_routes([web.get('/', client)])
        runner = web.AppRunner(app)
        await runner.setup()

        sites = [
            UnixSite(runner, self.control_socket_path),
            TCPSite(runner, port=self.port, ssl_context=tls, reuse_port=True),
        ]

        await asyncio.gather(*(site.start() for site in sites))

        self.sites = sites

    async def __call__(self):
        sites = self.sites

        await asyncio.gather(*(site.start_serving() for site in sites))

        await self.done

        asyncio.gather(*(site.stop() for site in sites))


async def main(procname, config_file, *args, **env):
    setswitchinterval(1)

    config = load_config(config_file)
    server = Server(config=config)

    await server.start()

    user = config.get('user')
    if user is not None:
        pw = getpwnam(user)
        uid = pw.pw_uid
        if uid != getuid():
            name = pw.pw_name
            gid = pw.pw_gid

            initgroups(name, gid)
            setresgid(gid, gid, gid)
            setresuid(uid, uid, uid)

            environ.update(
                dict(USER=name, LOGNAME=name, HOME=pw.pw_dir, SHELL=pw.pw_shell)
            )

    await server()
