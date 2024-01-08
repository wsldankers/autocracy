import asyncio
import aiohttp.web
from os import umask, uname
from sys import stderr, platform
from pathlib import Path
from ssl import create_default_context, Purpose, TLSVersion
from socket import gethostname, getaddrinfo, AI_CANONNAME, AF_INET, AF_INET6, AF_PACKET
from collections import defaultdict, deque
from json import dumps
from psutil import (
    net_if_addrs,
    cpu_count,
    cpu_freq,
    virtual_memory,
    swap_memory,
)
from ipaddress import ip_address, ip_network
from decimal import Decimal
from typing import Optional

from .common import loadconfig
from .rpc import RPC, immediate
from .utils import *

web = aiohttp.web


def get_interfaces(facts):
    interfaces = {}

    for name, addrs in net_if_addrs().items():
        interface = defaultdict(set)
        for addr in addrs:
            family = addr.family
            if family in (AF_INET, AF_INET6):
                netmask = addr.netmask
                ip = ip_address(addr.address)
                if netmask is None:
                    # to sort this after prefixed addresses:
                    ip = (ip, ip.max_prefixlen + 1)
                else:
                    ip = (ip, int(ip_address(netmask)).bit_count())
                if family == AF_INET:
                    interface['ipv4'].add(ip)
                else:
                    interface['ipv6'].add(ip)
            elif family == AF_PACKET:
                interface['mac'] = addr.address
        interfaces[name] = interface

    facts['interfaces'] = {
        name: {
            family: [
                str(addr) if prefixlen > addr.max_prefixlen else f"{addr}/{prefixlen}"
                for addr, prefixlen in sorted(addrs)
            ]
            if isinstance(addrs, set)
            else addrs
            for family, addrs in families.items()
        }
        for name, families in interfaces.items()
    }


def get_hostname(facts):
    facts['hostname'] = gethostname()


def get_fqdn(facts):
    addrinfo = getaddrinfo(facts['hostname'], 0, flags=AI_CANONNAME)
    primary_address = defaultdict(set)
    for addr in addrinfo:
        family, _, _, name, sockaddr = addr
        if name:
            facts['fqdn'] = name
        address = ip_address(sockaddr[0])
        if family == AF_INET:
            primary_address['ipv4'].add(address)
        elif family == AF_INET6:
            primary_address['ipv6'].add(address)

    for family in primary_address:
        primary_address[family] = sorted(primary_address[family])

    if primary_address:
        facts['primary_address'] = {
            family: [str(address) for address in sorted(values)]
            for family, values in primary_address.items()
        }


def get_platform(facts):
    facts['platform'] = platform


_uname_fields = ('sysname', 'nodename', 'release', 'version', 'machine')


def get_uname(facts):
    facts['uname'] = dict(zip(_uname_fields, uname()))


def get_cpu(facts):
    cpu = {
        'cores': cpu_count(logical=False),
        'threads': cpu_count(logical=True),
    }
    try:
        cpu['frequency'] = int((Decimal(cpu_freq().max) * 1000000).to_integral_value())
    except NotImplementedError:
        pass
    facts['cpu'] = cpu


def get_memory(facts):
    facts['memory'] = {
        'ram': virtual_memory().total,
        'swap': swap_memory().total,
    }


def get_facts():
    facts = {}
    for f in (
        get_interfaces,
        get_hostname,
        get_fqdn,
        get_platform,
        get_uname,
        get_cpu,
        get_memory,
    ):
        f(facts)

        # try:
        #     f(facts)
        # except Exception as e:
        #     print(str(e), file=stderr, flush=True)
    return facts


max_connect_interval = 1
max_facts_interval = 30


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
