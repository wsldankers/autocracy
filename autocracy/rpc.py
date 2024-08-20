"""
RPC protocol based around JSON message passing.

Send a command foo with arguments "quux" and "xyzyy", which does not require
an answer:

>   ["foo", null, "quux", "qyzzy]

Send a command foo with arguments "quux" and "xyzyy", which should result in
an answer:

>   ["foo", 123, "quux", "xyzzy"]

Send an answer indicating success to the previous command:

<   [true, 123, "xyzzy"]

Send an answer indicating failure to the previous command:

<   [false, 123, "oops"]
"""

import asyncio
import aiohttp
import aiohttp.web as web
from typing import Callable
from traceback import format_exc
from itertools import count

from .utils import Initializer, initializer, warn


class immediate:
    """Wrapping a handler in this class indicates that it must
    not be run in the background."""

    __slots__ = ('handler',)

    def __init__(self, handler):
        self.handler = handler


class CommandError(Exception):
    pass


class RPC(Initializer):
    ws: web.WebSocketResponse
    routes: dict[str, Callable]
    next_cid = count().__next__

    @initializer
    def pending_commands(self):
        return {}

    def __init__(self, ws, /, **kwargs):
        super().__init__(ws=ws, routes=kwargs)

    async def remote_command(self, name, *args, rsvp=True, timeout=30):
        """timeout can be None if you want it to wait forever"""
        ws = self.ws
        # warn(f"sending command {name!r} {args=}")
        if rsvp:
            cid = self.next_cid()
            pending_commands = self.pending_commands
            pending_command = asyncio.get_running_loop().create_future()
            pending_commands[cid] = pending_command

            try:
                await ws.send_json([name, cid, *args])

                return await asyncio.wait_for(pending_command, timeout)
            finally:
                pending_commands.pop(cid, None)
        else:
            await ws.send_json([name, None, *args])

    async def local_command(self, name, *args):
        handler = self.routes[name]
        return await handler(*args)

    @initializer
    def background_tasks(self):
        return set()

    def background_task(self, *args, **kwargs):
        background_tasks = self.background_tasks
        task = asyncio.create_task(*args, **kwargs)
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        return task

    async def handle_response(self, cid, args, handler):
        ws = self.ws
        try:
            result = await handler(*args)
        except Exception as e:
            warn(format_exc())
            await ws.send_json([False, cid, str(e)])
        else:
            if result is None:
                result = ()
            await ws.send_json([True, cid, *result])

    async def __aiter__(self):
        ws = self.ws
        routes = self.routes
        pending_commands = self.pending_commands

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    warn(f"ws connection closed with exception {ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    call = msg.json()
                    command, cid, *args = call
                    if command is True or command is False:
                        try:
                            pending_command = pending_commands[cid]
                        except KeyError:
                            warn(f"got a response for unknown command id {cid!r}")
                        else:
                            if command:
                                pending_command.set_result(args)
                            else:
                                pending_command.set_exception(CommandError(*args))
                    else:
                        try:
                            handler = routes[command]
                        except KeyError:
                            warn(f"server got unknown command {command!r}")
                            if cid is not None:
                                ws.send_json(
                                    [False, cid, f"unknown command {command!r}"]
                                )
                        else:
                            warn(f"running command {command!r}")
                            if isinstance(handler, immediate):
                                handler = handler.handler
                                if cid is None:
                                    try:
                                        await handler(*args)
                                    except Exception as e:
                                        warn(str(e))
                                else:
                                    await self.handle_response(cid, args, handler)
                            else:
                                if cid is None:
                                    self.background_task(handler(*args))
                                else:
                                    self.background_task(
                                        self.handle_response(cid, args, handler)
                                    )
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    yield msg.data
                else:
                    warn(f"got unexpected message {msg!r}")
            else:
                pass
                # warn("websocket connection closed")

        finally:
            background_tasks = self.background_tasks
            while background_tasks:
                await asyncio.gather(*background_tasks)
