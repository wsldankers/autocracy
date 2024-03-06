from subprocess import run
from typing import Optional
from collections.abc import Collection, Set
from subprocess import run, DEVNULL

from ..utils import *
from .base import Decree


class Packages(Initializer, Decree):
    install: Collection[str] = ()
    remove: Collection[str] = ()
    purge: Optional[bool] = None
    recommends: Optional[bool] = None
    update = True
    clean = False
    # quick = False
    gentle = False
    _install: Set[str]
    _remove: Set[str]

    @property
    def _needs_update(self) -> bool:
        result = run(
            ['dpkg', '--print-architecture'],
            capture_output=True,
            text=True,
            check=True,
            stdin=DEVNULL,
        )

        (native_arch,) = result.stdout.splitlines()
        default_archs = frozenset((native_arch, 'all'))

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

        installed: set[str] = set()

        for line in result.stdout.splitlines():
            name, arch, version, want, error, status, essential = line.split()
            if error != 'ok':
                raise RuntimeError(f"package {name}:{arch} is in error state {error}")
            if status == 'installed':
                installed.add(f"{name}:{arch}")
                if arch in default_archs:
                    installed.add(name)
            elif status != 'config-files':
                raise RuntimeError(
                    f"package {name}:{arch} has unknown status '{status}'"
                )

        install = frozenset(self.install) - installed
        remove = frozenset(self.remove) & installed

        self._install = install
        self._remove = remove

        return bool(install or remove)

    def _update(self) -> None:
        install = self._install
        remove = self._remove

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

        apt_get_options: set[str] = {'-qy'}
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
