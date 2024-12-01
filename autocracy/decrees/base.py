from abc import ABC, abstractmethod
from inspect import currentframe
from pathlib import Path
from subprocess import DEVNULL, run
from types import FrameType, TracebackType
from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence, Tuple, Union

from ..utils import *


class loadfilename(str):
    """To recognize loaded filenames by"""

    __slots__ = ()


def extract_loadfilename_from_frame(
    frame: FrameType,
) -> Union[Tuple[str, int], Tuple[None, None]]:
    while frame is not None:
        file = frame.f_code.co_filename
        if isinstance(file, loadfilename):
            return (file, frame.f_lineno)
        frame = frame.f_back
    return (None, None)


def extract_loadfilename_from_traceback(
    traceback: TracebackType,
) -> Union[Tuple[str, int], Tuple[None, None]]:
    while True:
        next_traceback = traceback.tb_next
        if next_traceback is None:
            return extract_loadfilename_from_frame(traceback.tb_frame)
        traceback = next_traceback


def extract_loadfilename_from_exception(
    exception: BaseException,
) -> Union[Tuple[str, int], Tuple[None, None]]:
    return extract_loadfilename_from_traceback(exception.__traceback__)


def format_loadfilename_exception(exception: BaseException) -> str:
    error = str(exception)
    file, line = extract_loadfilename_from_exception(exception)
    if file is not None and line is not None:
        error = f"{file}:{line}: {error}"
    return error


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
        (self._file, self._line) = extract_loadfilename_from_frame(currentframe())

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
    def _summary(self):
        summary = {}
        if self.updated:
            summary['updated'] = True
        if self.activated:
            summary['activated'] = True
        return summary

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
                and (dry_run or xyzzy(self._update()) or True)
            )
            self.activated = (
                self._should_activate
                and hasattr(self, '_activate')
                and (dry_run or xyzzy(self._activate()) or True)
            )
        finally:
            self.applied = True

        return self._summary

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

    def _prepare(self, name: Optional[str] = None):
        super()._prepare(name)
        decrees = extract_decrees(vars(self))
        for subname, decree in decrees.items():
            decree._prepare(subname)
        self._decrees = decrees.values()

    def _provision(self, repository: BaseRepository) -> None:
        for decree in self._decrees:
            decree._provision(repository)

    @initializer
    def updated(self) -> bool:
        return any(decree.updated for decree in self._decrees)

    @initializer
    def activated(self) -> bool:
        return any(decree.activated for decree in self._decrees)

    @initializer
    def _update_needed(self) -> bool:
        return any(decree._update_needed for decree in self._decrees)

    @property
    def _summary(self) -> dict[str, Any]:
        return {
            name: summary
            for name, summary in (
                (decree.name, decree._summary) for decree in self._decrees
            )
            if summary
        }

    def _apply(self, *args, **kwargs) -> dict[str, Any]:
        if self.applied:
            raise RuntimeError(f"{self}: refused attempt to run twice")
        try:
            for decree in self._decrees:
                decree._apply(*args, **kwargs)
        finally:
            self.applied = True

        return self._summary

    if TYPE_CHECKING:
        # To appease mypy

        def __getattr__(self, name: str) -> Decree:
            return getattr(super(), name)

        def __setattr__(self, name: str, value: Decree):
            setattr(super(), name, value)

        def __delattr__(self, name: str):
            delattr(super(), name)


class Policy(Group):
    pass


class Run(Initializer, Decree):
    @fallback
    def command(self) -> Union[str, bytes, Iterable]:
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
        # FIXME: capture stdout/stderr
        run(command, check=True, stdin=DEVNULL)


__all__ = ('Group', 'Run')
