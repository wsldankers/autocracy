import asyncio
from collections import deque
from os import umask
from pathlib import Path
from ssl import Purpose, TLSVersion, create_default_context
from sys import setswitchinterval
from traceback import format_exc
from typing import Any, Optional, Union

import aiohttp.web

from .common import load_config, load_policy
from .decrees.base import BaseRepository
from .feints import get_feints
from .rpc import RPC, immediate
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
    feints: Optional[dict] = None
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
    def max_feints_interval(self) -> int:
        return self.config.get('max_feints_interval', 60)

    @initializer
    def dry_run(self) -> bool:
        return self.config.get('dry_run', True)

    async def apply(self, name, dry_run=False) -> None:
        repository = Repository(files=self.files)

        feints = Object(self.feints or {})
        policy = load_policy(repository.get_file, name, feints=feints)
        policy._provision(repository)
        try:
            return [
                await asyncio.to_thread(policy._apply, dry_run=dry_run or self.dry_run)
            ]
        except Exception:
            return [{'error': format_exc()}]

    async def accept_files(self, *filenames) -> None:
        self.pending_files.extend(filenames)

    async def discard_files(self, *filenames) -> None:
        files = self.files
        for filename in filenames:
            del files[filename]

    async def feints_collector(self) -> None:
        previous_feints = object()
        feints_sleep = 0
        max_feints_interval = self.max_feints_interval
        feints: Optional[dict[str, Any]]
        while True:
            # warn("getting feints")
            try:
                feints = await asyncio.to_thread(get_feints)
            except Exception as e:
                # print_exc()
                warn(str(e))
                feints_sleep = max_feints_interval
            else:
                if feints != previous_feints:
                    self.feints = previous_feints = feints
                    feints_sleep = 0
                    # warn("sending feints")
                    await self.rpc.remote_command('feints', feints, rsvp=False)
            feints = None
            feints_sleep = min(feints_sleep + 1, max_feints_interval)
            await asyncio.sleep(feints_sleep)

    async def __call__(self) -> None:
        files = self.files
        pending_files = self.pending_files
        async with helper_task(self.feints_collector()):
            async for blob in self.rpc:
                filename = pending_files.popleft()
                files[Path(filename)] = blob
                # warn(f"client got data for file {filename!r}")


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
