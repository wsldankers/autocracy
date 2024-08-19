import asyncio
import aiohttp.web
from os import getenv
from itertools import chain
from json import dump
from sys import stdout

from .rpc import RPC
from .utils import warn


async def main(procname, *args, **env):
    exit_code = None

    async with aiohttp.ClientSession(
        raise_for_status=True,
        connector=aiohttp.UnixConnector(
            path=getenv('AUTOCRACY_CONTROL_SOCKET', '/run/autocracy/control'),
        ),
    ) as session:
        async with session.ws_connect('http://localhost', compress=False) as ws:
            rpc = RPC(ws)

            async def run_command():
                for x in await rpc.remote_command(*args):
                    dump(x, stdout, indent=2)

            async def rpc_loop():
                async for _ in rpc:
                    raise RuntimeError("binary blob received")

            done, pending = await asyncio.wait(
                [asyncio.create_task(a()) for a in [run_command, rpc_loop]],
                return_when=asyncio.FIRST_COMPLETED,
            )

        for task in pending:
            task.cancel()

        await asyncio.wait(pending)

        for task in chain(done, pending):
            exception = task.exception()
            if exception is not None:
                warn(exception)
                exit_code = 1

    return exit_code
