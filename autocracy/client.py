import asyncio
import aiohttp.web
from os import umask
from pathlib import Path
from ssl import create_default_context, Purpose, TLSVersion
from collections import deque
from typing import Optional, Any, Union
from sys import setswitchinterval
from traceback import print_exc

from .common import load_config, load_policy
from .edicts.base import BaseRepository
from .rpc import RPC, immediate
from .facts import get_facts
from .utils import *

web = aiohttp.web


class Repository(BaseRepository):
    files: dict[Path, bytes]

    def get_file(self, path: Union[str, Path]) -> bytes:
        return self.files[normalize_path(path)]

    def get_files(self, path: Union[str, Path]) -> dict[str, bytes]:
        normalized_path = normalize_path(path)
        return {
            str(key): value
            for key, value in self.files.items()
            if key.is_relative_to(normalized_path)
        }


class Client(Initializer):
    ws: web.WebSocketResponse
    facts: Optional[dict] = None
    config: dict[str, Any]

    @weakproperty
    def rpc(self) -> RPC:
        return RPC(
            self.ws,
            apply=self.apply,
            accept_files=immediate(self.accept_files),
            discard_files=immediate(self.discard_files),
        )

    @initializer
    def files(self) -> dict[Path, bytes]:
        return {}

    @initializer
    def pending_files(self) -> deque:
        return deque()

    @initializer
    def max_facts_interval(self) -> int:
        return self.config.get('max_facts_interval', 60)

    async def apply(self, name) -> None:
        repository = Repository(files=self.files)

        facts = Object(self.facts or {})
        policy = load_policy(repository.get_file, name, facts=facts)
        policy._provision(repository)
        try:
            await asyncio.to_thread(policy._apply)
        except Exception:
            print_exc()

    async def accept_files(self, *filenames) -> None:
        self.pending_files.extend(filenames)

    async def discard_files(self, *filenames) -> None:
        files = self.files
        for filename in filenames:
            del files[filename]

    async def fact_collector(self) -> None:
        previous_facts = object()
        facts_sleep = 0
        max_facts_interval = self.max_facts_interval
        facts: Optional[dict[str, Any]]
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

    async def __call__(self) -> None:
        files = self.files
        fact_collector_task = asyncio.create_task(self.fact_collector())
        pending_files = self.pending_files
        try:
            async for blob in self.rpc:
                filename = pending_files.popleft()
                files[Path(filename)] = blob
                # warn(f"client got data for file {filename!r}")
        finally:
            fact_collector_task.cancel()
            try:
                await fact_collector_task
            except asyncio.CancelledError:
                pass


async def main(procname, config_file, *args, **env):
    setswitchinterval(1)
    umask(0o022)

    config = load_config(config_file)
    base_dir = Path(config['base_dir'])

    server_crt = config.get('cafile', base_dir / 'server.crt')
    client_crt = config.get('certfile', base_dir / 'client.crt')
    client_key = config.get('keyfile', base_dir / 'client.key')

    tls = create_default_context(Purpose.SERVER_AUTH, cafile=server_crt)
    if tls.minimum_version < TLSVersion.TLSv1_3:
        tls.minimum_version = TLSVersion.TLSv1_3
    tls.load_cert_chain(client_crt, client_key)

    server = config.get('server', 'https://localhost')
    max_connect_interval = config.get('max_connect_interval', 60)

    async with aiohttp.ClientSession(raise_for_status=True) as session:
        connect_sleep = 0
        connect_errors = set()

        while True:
            try:
                async with session.ws_connect(server, compress=11, ssl=tls) as ws:
                    connect_errors.clear()
                    client = Client(config=config, ws=ws)
                    await client()

            except aiohttp.client_exceptions.ClientConnectorError as e:
                connect_error = str(e)
                if connect_error not in connect_errors:
                    connect_errors.add(connect_error)
                    warn(connect_error)

            connect_sleep = min(connect_sleep + 1, max_connect_interval)
            await asyncio.sleep(connect_sleep)
