from collections import defaultdict
from decimal import Decimal
from ipaddress import ip_address
from os import uname
from socket import AF_INET, AF_INET6, AF_PACKET, AI_CANONNAME, getaddrinfo, gethostname
from sys import platform
from typing import Any

from psutil import cpu_count, cpu_freq, net_if_addrs, swap_memory, virtual_memory

from .utils import get_file


def get_interfaces(reports) -> None:
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
                    # cidr = (ip, int(ip_address(netmask)).bit_count())
                    cidr = (ip, bin(int(ip_address(netmask))).count('1'))
                if family == AF_INET:
                    interface['ipv4'].add(cidr)
                else:
                    interface['ipv6'].add(cidr)
            elif family == AF_PACKET:
                interface['mac'] = addr.address
        interfaces[name] = interface

    reports['interfaces'] = {
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


def get_hostname(reports) -> None:
    reports['hostname'] = gethostname()


def get_fqdn(reports) -> None:
    addrinfo = getaddrinfo(reports['hostname'], 0, flags=AI_CANONNAME)
    primary_address = defaultdict(set)
    for addr in addrinfo:
        af, _, _, name, sockaddr = addr
        if name:
            reports['fqdn'] = name
        address = ip_address(sockaddr[0])
        if af == AF_INET:
            primary_address['ipv4'].add(address)
        elif af == AF_INET6:
            primary_address['ipv6'].add(address)

    if primary_address:
        reports['primary_address'] = {
            family: [str(address) for address in sorted(values)]
            for family, values in primary_address.items()
        }


def get_platform(reports) -> None:
    reports['platform'] = platform


_uname_fields = ('sysname', 'nodename', 'release', 'version', 'machine')


def get_uname(reports) -> None:
    reports['uname'] = dict(zip(_uname_fields, uname()))


def get_cpu(reports) -> None:
    cpu = {
        'cores': cpu_count(logical=False),
        'threads': cpu_count(logical=True),
    }
    try:
        cpu['frequency'] = int((Decimal(cpu_freq().max) * 1000000).to_integral_value())
    except NotImplementedError:
        pass
    reports['cpu'] = cpu


def get_memory(reports) -> None:
    reports['memory'] = {
        'ram': virtual_memory().total,
        'swap': swap_memory().total,
    }


def get_qemu(reports) -> None:
    try:
        if get_file('/sys/class/dmi/id/sys_vendor').strip() == 'QEMU':
            reports['qemu'] = True
    except FileNotFoundError:
        pass


def get_reports() -> dict[str, Any]:
    reports: dict[str, Any] = {}
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
        f(reports)

        # try:
        #     f(reports)
        # except Exception as e:
        #     print(str(e), file=stderr, flush=True)

    return reports


__all__ = ('get_reports',)
