from abc import ABC, abstractmethod
from inspect import currentframe
from pathlib import Path
from subprocess import run
from typing import TYPE_CHECKING, Optional, Sequence, Union

from ..utils import *


class loadfilename(str):
    """To recognize loaded filenames by"""

    __slots__ = ()


class BaseRepository(ABC, Initializer):
    @abstractmethod
    def get_file(self, path: Union[str, Path]) -> bytes:
        pass

    @abstractmethod
    def get_files(self, path: Union[str, Path]) -> dict[str, bytes]:
        pass


class Decree:
    activate_if = True
    name = ""
    applied = False

    if TYPE_CHECKING:

        @property
        def _update_needed(self) -> bool:
            return False

    else:
        _update_needed = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        frame = currentframe()
        while True:
            frame = frame.f_back
            if frame is None:
                self._file = None
                self._line = None
                break
            file = frame.f_code.co_filename
            if isinstance(file, loadfilename):
                self._file = file
                self._line = frame.f_lineno
                break

    def __set_name__(self, objtype, name):
        if not self.name:
            self.name = name

    def _prepare(self, name: Optional[str] = None):
        if name and not self.name:
            self.name = name

    def _provision(self, repository: BaseRepository):
        pass

    @fallback
    def updated(self):
        raise RuntimeError(f"{self}: not applied yet (did you forget a lambda?)")

    @fallback
    def activated(self):
        raise RuntimeError(f"{self}: not applied yet (did you forget a lambda?)")

    @property
    def _should_activate(self):
        return call_if_callable(self.activate_if)

    def _apply(self, dry_run=False):
        if self.applied:
            raise RuntimeError(f"{self}: refused attempt to run twice")
        try:
            self.updated = (
                self._update_needed
                and hasattr(self, '_update')
                and (dry_run or self._update())
            )
            self.activated = (
                self._should_activate
                and hasattr(self, '_activate')
                and (dry_run or self._activate())
            )
        finally:
            self.applied = True

    def __str__(self):
        name = self.name
        if name:
            name = f"{name=}, "
        else:
            name = ""
        return f"<{type(self).__name__}({name}{self._file!r}:{self._line})>"


def extract_decrees(mapping: dict[str, Decree]) -> dict[str, Decree]:
    return {
        name: decree
        for name, decree in mapping.items()
        if not name.startswith('_') and isinstance(decree, Decree)
    }


class Group(Initializer, Decree):
    @fallback
    def _decrees(self) -> Sequence[Decree]:
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    def _prepare(self, name=None):
        super()._prepare(name)
        decrees = extract_decrees(vars(self))
        for subname, decree in decrees.items():
            decree._prepare(subname)
        self._decrees = decrees.values()

    def _provision(self, repository: BaseRepository):
        for decree in self._decrees:
            decree._provision(repository)

    @initializer
    def updated(self):
        # warn(f"{self}:")
        # for decree in self._decrees:
        #     warn(f"\t{decree}: {decree.updated!r} {decree.activated!r}")

        return any(decree.updated for decree in self._decrees)

    @initializer
    def activated(self):
        return any(decree.activated for decree in self._decrees)

    @initializer
    def _update_needed(self):
        return {
            name: update
            for name, update in (
                (decree.name, decree._update_needed) for decree in self._decrees
            )
            if update
        }

    def _apply(self, *args, **kwargs):
        if self.applied:
            raise RuntimeError(f"{self}: refused attempt to run twice")
        try:
            for decree in self._decrees:
                decree._apply(*args, **kwargs)
        finally:
            self.applied = True

    # To appease mypy:
    def __getattr__(self, name: str) -> Decree:
        return getattr(super(), name)

    def __setattr__(self, name: str, value: Decree):
        setattr(super(), name, value)

    def __delattr__(self, name: str):
        delattr(super(), name)

    del __getattr__
    del __setattr__
    del __delattr__


class Policy(Group):
    pass


class Run(Initializer, Decree):
    @fallback
    def command(self):
        raise RuntimeError(f"{self.name}: no command configured")

    def _activate(self):
        command = self.command
        try:
            command = (
                b'/bin/sh',
                b'-ec',
                ensure_bytes(command),
                ensure_bytes(self.name),
            )
        except TypeError:
            command = tuple(map(ensure_bytes, command))
        completed = run(command)
        completed.check_returncode()


__all__ = ('Group', 'Run')
