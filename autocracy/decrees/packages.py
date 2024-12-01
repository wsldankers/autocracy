from collections.abc import Mapping, Set
from os import environ
from subprocess import DEVNULL, run
from typing import Optional

from ..utils import *
from .base import Decree


class Packages(Initializer, Decree):
    install: Mapping[str, bool] = frozendict()
    purge: Optional[bool] = None
    recommends: Optional[bool] = None
    update = True
    clean = False
    # quick = False
    gentle = False

    @property
    def _install(self) -> Set[str]:
        install, _ = self._install_remove
        return install

    @property
    def _remove(self) -> Set[str]:
        _, remove = self._install_remove
        return remove

    @initializer
    def _install_remove(self):
        result = run(
            ['dpkg', '--print-architecture'],
            capture_output=True,
            text=True,
            check=True,
            stdin=DEVNULL,
        )

        (native_arch,) = result.stdout.splitlines()
        default_archs = frozenset((native_arch, 'all'))

        found: set[str] = set()

        if self.gentle:
            result = run(
                ['apt-mark', 'showmanual'],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )

            for fullname in result.stdout.splitlines():
                name, sep, arch = fullname.partition(':')
                if sep:
                    found.add(fullname)
                    if arch in default_archs:
                        found.add(name)
                else:
                    found.add(name)
                    for arch in default_archs:
                        found.add(f"{name}:{arch}")
        else:
            result = run(
                [
                    'dpkg-query',
                    '-f',
                    r'${Package} ${Architecture} ${Version} ${Status} ${Essential}\n',
                    '-W',
                ],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )

            for line in result.stdout.splitlines():
                name, arch, version, want, error, status, essential = line.split()
                if error != 'ok':
                    raise RuntimeError(
                        f"package {name}:{arch} is in error state {error}"
                    )
                if status == 'installed':
                    found.add(f"{name}:{arch}")
                    if arch in default_archs:
                        found.add(name)
                elif status != 'config-files':
                    raise RuntimeError(
                        f"package {name}:{arch} has unknown status '{status}'"
                    )

        install = set()
        remove = set()
        for package, action in self.install.items():
            if action is None:
                continue
            if action:
                if package not in found:
                    install.add(package)
            else:
                if package in found:
                    remove.add(package)

        return (install, remove)

    @property
    def _update_needed(self) -> bool:
        install, remove = self._install_remove
        return bool(install or remove)

    @property
    def _summary(self):
        summary = super()._summary
        update_summary = {}
        install, remove = self._install_remove
        if install:
            update_summary['install'] = sorted(install)
        if remove:
            update_summary['remove'] = sorted(remove)
        if update_summary:
            summary['updated'] = update_summary
        return summary

    def _update(self) -> None:
        install, remove = self._install_remove

        if self.clean:
            run(
                ['apt-get', 'clean'],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )

        if install and self.update:
            run(
                ['apt-get', '-qq', 'update'],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )

        env = {
            'UCF_FORCE_CONFFOLD': '1',
            'DEBIAN_FRONTEND': 'noninteractive',
            **environ,
        }

        apt_get_options = {'--option=Dpkg::Options::=--force-confold', '-qy'}
        if remove:
            if self.purge:
                apt_get_options.add('--purge')
            elif self.purge is not None:
                apt_get_options.add('--no-purge')

        if self.recommends:
            apt_get_options.add('--install-recommends')
        elif self.recommends is not None:
            apt_get_options.add('--no-install-recommends')

        if self.gentle:
            if remove:
                run(
                    ['apt-mark', 'auto', *remove],
                    capture_output=True,
                    text=True,
                    check=True,
                    stdin=DEVNULL,
                )
                apt_get_options.add('--auto-remove')
            run(
                ['apt-get', *apt_get_options, 'install', *install],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
                env=env,
            )
        else:
            run(
                [
                    'apt-get',
                    *apt_get_options,
                    'install',
                    *install,
                    *(f"{package}-" for package in remove),
                ],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
                env=env,
            )

        if install and self.clean:
            run(
                ['apt-get', 'clean'],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )


__all__ = ('Packages',)
