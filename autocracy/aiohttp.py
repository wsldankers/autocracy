"""Variants of TCPSite and UnixSite that decouple opening the listening
socket and starting the listening on that socket."""

from asyncio import get_event_loop
from aiohttp.web_runner import TCPSite as BaseTCPSite, UnixSite as BaseUnixSite

# Copyright: 2013-2019 Nikolay Kim and Andrew Svetlov
# Copyright: 2024 Wessel Dankers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# On Debian systems, the complete text of the Apache version 2.0 license
# can be found in "/usr/share/common-licenses/Apache-2.0".


class TCPSite(BaseTCPSite):
    async def start(self) -> None:
        # skip one level in the class hierarchy:
        await super(BaseTCPSite, self).start()
        assert self._server is None
        loop = get_event_loop()
        server = self._runner.server
        assert server is not None
        self._server = await loop.create_server(
            server,
            self._host,
            self._port,
            ssl=self._ssl_context,
            backlog=self._backlog,
            reuse_address=self._reuse_address,
            reuse_port=self._reuse_port,
            start_serving=False,
        )

    async def start_serving(self) -> None:
        assert self._server is not None
        await self._server.start_serving()


class UnixSite(BaseUnixSite):
    async def start(self) -> None:
        # skip one level in the class hierarchy:
        await super(BaseUnixSite, self).start()
        assert self._server is None
        loop = get_event_loop()
        server = self._runner.server
        assert server is not None
        self._server = await loop.create_unix_server(
            server,
            self._path,
            ssl=self._ssl_context,
            backlog=self._backlog,
            start_serving=False,
        )

    async def start_serving(self) -> None:
        assert self._server is not None
        await self._server.start_serving()
