from psutil import (
    net_if_addrs,
    cpu_count,
    cpu_freq,
    virtual_memory,
    swap_memory,
)
from ipaddress import ip_address
from decimal import Decimal
from collections import defaultdict
from socket import gethostname, getaddrinfo, AI_CANONNAME, AF_INET, AF_INET6, AF_PACKET
from sys import platform
from os import uname
from typing import Any

from .utils import get_file


def get_interfaces(facts) -> None:
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
                    cidr = (ip, ip.max_prefixlen + 1)
                else:
                    cidr = (ip, int(ip_address(netmask)).bit_length())
                if family == AF_INET:
                    interface['ipv4'].add(cidr)
                else:
                    interface['ipv6'].add(cidr)
            elif family == AF_PACKET:
                interface['mac'] = addr.address
        interfaces[name] = interface

    facts['interfaces'] = {
        name: {
            family: (
                [
                    (
                        str(addr)
                        if prefixlen > addr.max_prefixlen
                        else f"{addr}/{prefixlen}"
                    )
                    for addr, prefixlen in sorted(addrs)
                ]
                if isinstance(addrs, set)
                else addrs
            )
            for family, addrs in families.items()
        }
        for name, families in interfaces.items()
    }


def get_hostname(facts) -> None:
    facts['hostname'] = gethostname()


def get_fqdn(facts) -> None:
    addrinfo = getaddrinfo(facts['hostname'], 0, flags=AI_CANONNAME)
    primary_address = defaultdict(set)
    for addr in addrinfo:
        af, _, _, name, sockaddr = addr
        if name:
            facts['fqdn'] = name
        address = ip_address(sockaddr[0])
        if af == AF_INET:
            primary_address['ipv4'].add(address)
        elif af == AF_INET6:
            primary_address['ipv6'].add(address)

    if primary_address:
        facts['primary_address'] = {
            family: [str(address) for address in sorted(values)]
            for family, values in primary_address.items()
        }


def get_platform(facts) -> None:
    facts['platform'] = platform


_uname_fields = ('sysname', 'nodename', 'release', 'version', 'machine')


def get_uname(facts) -> None:
    facts['uname'] = dict(zip(_uname_fields, uname()))


def get_cpu(facts) -> None:
    cpu = {
        'cores': cpu_count(logical=False),
        'threads': cpu_count(logical=True),
    }
    try:
        cpu['frequency'] = int((Decimal(cpu_freq().max) * 1000000).to_integral_value())
    except NotImplementedError:
        pass
    facts['cpu'] = cpu


def get_memory(facts) -> None:
    facts['memory'] = {
        'ram': virtual_memory().total,
        'swap': swap_memory().total,
    }


def get_qemu(facts) -> None:
    try:
        if get_file('/sys/class/dmi/id/sys_vendor').strip() == 'QEMU':
            facts['qemu'] = True
    except FileNotFoundError:
        pass


def get_facts() -> dict[str, Any]:
    facts: dict[str, Any] = {}
    for f in (
        get_interfaces,
        get_hostname,
        get_fqdn,
        get_platform,
        get_uname,
        get_cpu,
        get_memory,
        get_qemu,
    ):
        f(facts)

        # try:
        #     f(facts)
        # except Exception as e:
        #     print(str(e), file=stderr, flush=True)

    return facts


__all__ = ('get_facts',)
