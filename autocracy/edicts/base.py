from pathlib import Path
from subprocess import run
from inspect import currentframe
from typing import Optional, Union, TYPE_CHECKING
from abc import ABC, abstractmethod
from subprocess import run

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
    only_if = True
    name = ""

    if TYPE_CHECKING:

        @property
        def _needs_update(self) -> bool:
            return True

    else:
        _needs_update = True

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
        self.applied = False
        self.updated = False
        self.activated = False

    def _provision(self, repository: BaseRepository):
        pass

    @fallback
    def applied(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @fallback
    def updated(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @fallback
    def activated(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @property
    def _should_activate(self):
        return call_if_callable(self.only_if)

    def _apply(self):
        if self.applied:
            raise RuntimeError(f"{self}: refused attempt to run twice")
        if self._needs_update:
            self._update()
            self.updated = True
        if self._should_activate:
            self._activate()
            self.activated = True
        self.applied = True

    def _update(self):
        pass

    def _activate(self):
        pass

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
    def decrees(self) -> dict[str, Decree]:
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    def _provision(self, repository: BaseRepository):
        for decree in self.decrees.values():
            decree._provision(repository)

    @initializer
    def updated(self):
        return any(decree.updated for decree in self.decrees.values())

    @initializer
    def activated(self):
        return any(decree.activated for decree in self.decrees.values())

    def _apply(self):
        for decree in self.decrees.values():
            decree._apply()

    def _prepare(self, name=None):
        super()._prepare(name)
        decrees = extract_decrees(vars(self))
        for subname, decree in decrees.items():
            decree._prepare(subname)
        self.decrees = decrees

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
