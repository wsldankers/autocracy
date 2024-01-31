import asyncio
import aiohttp.web
from os import umask, uname
from sys import stderr, platform
from pathlib import Path
from ssl import create_default_context, Purpose, TLSVersion
from socket import gethostname, getaddrinfo, AI_CANONNAME, AF_INET, AF_INET6, AF_PACKET
from collections import defaultdict, deque
from json import dumps
from typing import Optional, Iterable

from .common import loadconfig, BaseRepository
from .rpc import RPC, immediate
from .facts import get_facts
from .utils import *

web = aiohttp.web


max_connect_interval = 1
max_facts_interval = 30


class Repository(BaseRepository):
    files: dict[Path, bytes]

    def get_file(self, path: str | Path) -> bytes:
        return self.files[normalize_path(path)]

    def get_files(self, path: str | Path) -> Iterable[bytes]:
        normalized_path = normalize_path(path)
        return (
            value
            for key, value in self.files.items()
            if key.is_relative_to(normalized_path)
        )


class Client(Initializer):
    ws: web.WebSocketResponse
    facts: Optional[dict]

    @weakproperty
    def rpc(self):
        return RPC(
            self.ws,
            apply=self.apply,
            accept_files=immediate(self.accept_files),
            discard_files=immediate(self.discard_files),
        )

    @initializer
    def files(self):
        return {}

    @initializer
    def pending_files(self):
        return deque()

    async def apply(self, name):
        files = self.files

        def get_file(filename):
            return files[filename]

        decree = loadconfig(name, get_file, facts=Object(self.facts))
        decree._provided_resources = files
        await asyncio.to_thread(decree._apply)

    async def accept_files(self, *filenames):
        self.pending_files.extend(filenames)

    async def discard_files(self, *filenames):
        files = self.files
        for filename in filenames:
            del files[filename]

    async def fact_collector(self):
        previous_facts = object()
        facts_sleep = 0
        while True:
            # warn("getting facts")
            try:
                facts = await asyncio.to_thread(get_facts)
            except Exception as e:
                # print_exc()
                warn(str(e))
                facts_sleep = max_facts_interval
            else:
                if facts != previous_facts:
                    self.facts = previous_facts = facts
                    facts_sleep = 0
                    warn("sending facts")
                    await self.rpc.remote_command('facts', facts, rsvp=False)
            facts = None
            facts_sleep = min(facts_sleep + 1, max_facts_interval)
            await asyncio.sleep(facts_sleep)

    async def __call__(self):
        files = self.files
        fact_collector_task = asyncio.create_task(self.fact_collector())
        pending_files = self.pending_files
        try:
            async for blob in self.rpc:
                filename = pending_files.popleft()
                files[filename] = blob
                warn(f"client got data for file {filename!r}")
        finally:
            fact_collector_task.cancel()
            try:
                await fact_collector_task
            except asyncio.CancelledError:
                pass


async def main(base_dir):
    umask(0o027)

    tls = create_default_context(Purpose.SERVER_AUTH, cafile=base_dir / 'server.crt')
    if tls.minimum_version < TLSVersion.TLSv1_3:
        tls.minimum_version = TLSVersion.TLSv1_3
    tls.load_cert_chain(base_dir / 'client.crt', base_dir / 'client.key')

    async with aiohttp.ClientSession(raise_for_status=True) as session:
        connect_sleep = 0
        connect_errors = set()

        while True:
            try:
                async with session.ws_connect(
                    'https://localhost:9999',
                    compress=11,
                    ssl=tls,
                ) as ws:
                    connect_errors.clear()
                    client = Client(ws=ws)
                    await client()

            except aiohttp.client_exceptions.ClientConnectorError as e:
                connect_error = str(e)
                if connect_error not in connect_errors:
                    connect_errors.add(connect_error)
                    warn(connect_error)

            connect_sleep = min(connect_sleep + 1, max_connect_interval)
            await asyncio.sleep(connect_sleep)
