import asyncio
from itertools import chain
from json import dump as dump_json, dumps as dumps_json
from os import getenv
from re import compile as regcomp
from sys import stdout

import aiohttp

from .rpc import RPC
from .utils import warn

_isplainkey = regcomp(r'[0-9a-zA-Z_]+(?:-[0-9a-zA-Z_])*').fullmatch


def ghetto_yaml(o, _indent="", _sep=''):
    if isinstance(o, dict) and len(o):
        if _sep:
            print()
        for key, value in o.items():
            print(
                _indent,
                key if _isplainkey(key) else dumps_json(key, ensure_ascii=False),
                ":",
                sep="",
                end="",
            )
            ghetto_yaml(
                value, _indent if isinstance(value, list) else _indent + "  ", _sep=" "
            )
    elif isinstance(o, list) and len(o):
        if _sep:
            print()
        for value in o:
            print(_indent, "-", sep="", end="")
            ghetto_yaml(value, _indent + "  ", _sep=" ")
    elif isinstance(o, str) and _sep and "\n" in o:
        print(_sep, "|", sep="")
        for line in o.splitlines():
            print(_indent, line, sep="")
    else:
        print(_sep, end="")
        dump_json(o, stdout, ensure_ascii=False)
        print()


async def main(procname, *args, **env):
    exit_code = None

    async with aiohttp.ClientSession(
        raise_for_status=True,
        connector=aiohttp.UnixConnector(
            path=getenv('AUTOCRACY_CONTROL_SOCKET', '/run/autocracy/control')
        ),
    ) as session:
        async with session.ws_connect('http://localhost', compress=False) as ws:
            rpc = RPC(ws)

            async def run_command():
                for x in await rpc.remote_command(*args):
                    ghetto_yaml(x)

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
