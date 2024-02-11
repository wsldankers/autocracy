import asyncio
import aiohttp.web
from sys import argv

from .common import warn
from .rpc import RPC


async def main(procname, *args, **env):
    async with aiohttp.ClientSession(
        raise_for_status=True,
        connector=aiohttp.UnixConnector(path='control'),
    ) as session:
        async with session.ws_connect('http://localhost', compress=False) as ws:
            rpc = RPC(ws)

            async def run_command():
                for x in await rpc.remote_command(*args):
                    print(x)

            async def rpc_loop():
                async for _ in rpc:
                    raise RuntimeError("binary blob received")

            run_task = asyncio.create_task(run_command())
            rpc_task = asyncio.create_task(rpc_loop())

            done, pending = await asyncio.wait(
                [run_task, rpc_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

        for task in pending:
            task.cancel()

        await asyncio.wait(pending)
