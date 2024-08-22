from errno import ENOTEMPTY
from grp import getgrgid, getgrnam
from itertools import chain
from os import (
    F_OK,
    access,
    chmod,
    chown,
    mkdir,
    open as os_open,
    readlink,
    rmdir,
    stat,
    symlink,
    unlink,
)
from pathlib import Path
from pwd import getpwnam, getpwuid
from shutil import rmtree
from stat import S_IMODE, S_ISDIR, S_ISLNK
from types import MappingProxyType
from typing import Any, MutableMapping, Optional, Union, cast

from ..utils import *
from .base import BaseRepository, Decree


class _FileHandlingAction:
    __slots__ = ('target', 'create', 'chown', 'chmod', 'contents', 'summary')

    target: Union[Path, str]
    # create is just informational but used to see if parent directories
    # need to be created.
    create: bool
    chown: Optional[tuple[int, int]]
    chmod: Optional[int]
    contents: Optional[bytes]

    summary: dict

    def __bool__(self):
        return (
            self.create
            or self.chown is not None
            or self.chmod is not None
            or self.contents is not None
        )

    def __call__(self):
        do_contents = self.contents
        if do_contents is None:
            open_mode = 'w+b'
        else:
            open_mode = 'wb'

        do_chmod = self.chmod

        def opener(path, flags):
            open_mode = 0o666 if do_chmod is None else 0o600
            return os_open(path, flags, mode=open_mode)

        with open(self.target, open_mode, opener=opener) as fh:
            if do_contents:
                print('contents')
                fh.write(do_contents)
            fd = fh.fileno()
            do_chown = self.chown
            if do_chown is not None:
                print('chown')
                chown(fd, *do_chown)
            if do_chmod is not None:
                print('chmod')
                chmod(fd, do_chmod)


def _parse_owner(owner: Union[str, int, None]) -> tuple[Optional[int], Optional[int]]:
    if owner is None:
        return (None, None)
    owner, sep, group = f"{owner}".partition(':')
    if owner.isdecimal() and owner.isascii():
        pwent = None
        uid = int(owner)
    elif owner == '':
        pwent = None
        uid = None
    else:
        pwent = getpwnam(owner)
        uid = pwent.pw_uid
    if sep:
        if group.isdecimal() and group.isascii():
            gid = int(group)
        elif group == '':
            if uid is None:
                gid = None
            else:
                if pwent is None:
                    pwent = getpwuid(uid)
                gid = pwent.pw_gid
        else:
            gid = getgrnam(group).gr_gid
    else:
        gid = None
    return (uid, gid)


def _parse_mode(mode: Union[str, int, None]) -> Optional[int]:
    if mode is None:
        return None
    if isinstance(mode, str):
        mode = int(mode, base=8)
    return S_IMODE(mode)


class _FileHandlingMixin:
    owner: Union[str, int, None] = None
    mode: Union[str, int, None] = 0o644

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @initializer
    def _mode(self) -> Optional[int]:
        return _parse_mode(self.mode)

    def _check_file(self, target, new_contents):
        uid, gid = self._owner
        mode = self._mode
        action = _FileHandlingAction()
        action.target = target
        summary = {}
        try:
            with open(target, 'rb') as fh:
                st = stat(fh.fileno())
                old_contents = fh.read()
        except FileNotFoundError:
            action.create = True
            needs_chown = uid is not None or gid is not None
            needs_chmod = mode is not None
            needs_contents = True
            old_contents = b''
        else:
            action.create = False
            needs_chown = (uid is not None and st.st_uid != uid) or (
                gid is not None and st.st_gid != gid
            )
            needs_chmod = mode is not None and S_IMODE(st.st_mode) != mode
            needs_contents = old_contents != new_contents

        if needs_chown:
            if uid is not None:
                owner_summary = {'new': getpwuid(uid).pw_name}
                if not action.create:
                    owner_summary['old'] = getpwuid(st.st_uid).pw_name
                summary['owner'] = owner_summary

            if gid is not None:
                group_summary = {'new': getgrgid(gid).gr_name}
                if not action.create:
                    group_summary['old'] = getgrgid(st.st_gid).gr_name
                summary['group'] = group_summary

            action.chown = (-1 if uid is None else uid, -1 if gid is None else gid)
        else:
            action.chown = None

        if needs_chmod:
            mode_summary = {'new': mode}
            if not action.create:
                mode_summary['old'] = S_IMODE(st.st_mode)
            summary['mode'] = mode_summary

            action.chmod = mode
        else:
            action.chmod = None

        if needs_contents:
            action.contents = new_contents
            if b'\0' in old_contents or b'\0' in new_contents:
                diff = "binary files differ"
            else:
                try:
                    old_string_contents = str(old_contents, encoding='UTF-8')
                    new_string_contents = str(new_contents, encoding='UTF-8')
                except UnicodeDecodeError:
                    diff = "non-UTF-8 files differ"
                else:
                    # warn(f"{target}: {old_contents!r} {new_contents!r}")
                    diff = (
                        string_diff(
                            old_string_contents,
                            new_string_contents,
                            fromfile=target,
                            tofile=target,
                            fromfiledate=(
                                '' if action.create else isoformat_ns(st.st_mtime_ns)
                            ),
                        )
                        or "empty file"
                    )
            summary['contents'] = diff
        else:
            action.contents = None

        action.summary = summary

        return action


class File(Initializer, _FileHandlingMixin, Decree):
    target: Union[Path, str]
    makedirs = False

    _contents: Optional[bytes] = None
    _needs_chown = False
    _needs_chmod = False
    _needs_contents = False

    def _provision(self, repository: BaseRepository):
        source = self.source
        if source is not None:
            self._contents = repository.get_file(source)

    @property
    def contents(self):
        return self.__dict__.get('contents', None)

    @contents.setter
    def contents(self, value):
        if self.source is not None:
            raise RuntimeError(f"must set either contents or source, not both")
        self.__dict__['contents'] = value

    content = contents

    @property
    def source(self):
        return self.__dict__.get('source', None)

    @source.setter
    def source(self, value):
        if self.contents is not None:
            raise RuntimeError(f"must set either contents or source, not both")
        self.__dict__['source'] = value

    @initializer
    def _computed_contents(self):
        contents = self.contents

        if contents is None:
            return self._contents

        if is_byteslike(contents):
            return contents

        try:
            return bytes(contents)
        except TypeError:
            pass

        return str(contents).encode('UTF-8')

    @initializer
    def _action(self):
        return self._check_file(self.target, self._computed_contents)

    @property
    def _update_needed(self):
        return bool(self._action)

    @property
    def _summary(self):
        summary = super()._summary
        update_summary = self._action.summary
        if update_summary:
            summary['updated'] = update_summary
        return summary

    def _update(self):
        print(f"{self.name}: running")

        if self.makedirs:
            try:
                self._action()
            except FileNotFoundError:
                pass
            else:
                return

            Path(self.target).parent.mkdir(parents=True)

        self._action()


class RecursiveFiles(Initializer, _FileHandlingMixin, Decree):
    _files: MutableMapping[str, bytes] = cast(MutableMapping, MappingProxyType({}))
    target: Union[Path, str]

    def _provision(self, repository: BaseRepository):
        source = self.source
        if source is not None:
            self._files = repository.get_files(source).copy()

    @property
    def source(self) -> Optional[str]:
        return self.__dict__.get('source', None)

    @source.setter
    def source(self, value: Union[Path, str]):
        self.__dict__['source'] = str(value)

    @initializer
    def _existing_parents(self) -> set[Path]:
        return {
            Path(self.target).parent,
            *chain.from_iterable(
                action.target.parents for action in self._actions if not action.create
            ),
        }

    @initializer
    def _actions(self) -> list[_FileHandlingAction]:
        source_str = self.source
        if source_str is None:
            return []
        source = Path(source_str)
        target = Path(self.target)
        check_file = self._check_file
        return list(
            filter(
                None,
                (
                    check_file(target / Path(filename).relative_to(source), contents)
                    for filename, contents in self._files.items()
                ),
            )
        )

    @property
    def _update_needed(self):
        return bool(self._actions)

    @property
    def _summary(self) -> dict[str, Any]:
        summary = super()._summary
        summary['updated'] = {actions.target: action.summary for action in self._actions}
        return summary

    def _update(self) -> None:
        # print(f"{self.name}: running")

        existing_parents = self._existing_parents

        for action in self._actions:
            # print(f"updating {action.target}")
            try:
                action()
            except FileNotFoundError:
                pass
            else:
                continue
            full_path = cast(Path, action.target)
            parent = full_path.parent
            create = []
            for parent in full_path.parents:
                if parent in existing_parents:
                    break
                create.append(parent)
            for parent in reversed(create):
                try:
                    mkdir(parent)
                except FileExistsError:
                    pass
            existing_parents.update(create)
            action()


class Symlink(Initializer, Decree):
    target: Union[Path, str]
    owner: Union[str, int, None] = None
    contents: Union[str]
    force = False

    _needs_remove = False
    _needs_chown = False
    _needs_create = False

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @property
    def _update_needed(self) -> bool:
        target = self.target
        uid, gid = self._owner
        try:
            st = stat(target, follow_symlinks=False)
        except FileNotFoundError:
            self._needs_create = True
        else:
            if S_ISLNK(st.st_mode) and readlink(target) == self.contents:
                self._needs_chown = (uid is not None and st.st_uid != uid) or (
                    gid is not None and st.st_gid != gid
                )
            else:
                self._needs_remove = True

        if self._needs_remove:
            self._needs_create = True

        if self._needs_create:
            self._needs_chown = uid is not None or gid is not None

        return self._needs_chown

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        if self._needs_remove:
            try:
                unlink(target)
            except IsADirectoryError:
                try:
                    rmdir(target)
                except OSError as e:
                    if e.errno == ENOTEMPTY and self.force:
                        rmtree(target)
                    else:
                        raise e from None

        if self._needs_create:
            symlink(self.contents, target)

        if self._needs_chown:
            uid, gid = self._owner
            chown(
                target,
                -1 if uid is None else uid,
                -1 if gid is None else gid,
                follow_symlinks=False,
            )


class Directory(Initializer, Decree):
    target: Union[Path, str]
    owner: Union[str, int, None] = None
    mode: Union[str, int, None] = 0o755

    _needs_remove = False
    _needs_create = False
    _needs_chown = False
    _needs_chmod = False

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @initializer
    def _mode(self) -> Optional[int]:
        return _parse_mode(self.mode)

    @property
    def _update_needed(self) -> bool:
        target = self.target
        uid, gid = self._owner
        mode = self._mode
        try:
            st = stat(target, follow_symlinks=False)
        except FileNotFoundError:
            self._needs_create = True
        else:
            if S_ISDIR(st.st_mode):
                self._needs_chown = (uid is not None and st.st_uid != uid) or (
                    gid is not None and st.st_gid != gid
                )
                self._needs_chmod = mode is not None and S_IMODE(st.st_mode) != mode
            else:
                self._needs_remove = True
                self._needs_create = True

        if self._needs_create:
            self._needs_chown = uid is not None or gid is not None
            self._needs_chmod = mode is not None

        return self._needs_create or self._needs_chown or self._needs_chmod

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        if self._needs_remove:
            unlink(target)

        if self._needs_create:
            if self._needs_chmod:
                mkdir(target, mode=0o700)
            else:
                mkdir(target)

        if self._needs_chown:
            uid, gid = self._owner
            chown(
                target,
                -1 if uid is None else uid,
                -1 if gid is None else gid,
                follow_symlinks=False,
            )

        if self._needs_chmod:
            mode = self._mode
            assert mode is not None
            chmod(target, mode, follow_symlinks=False)


class Permissions(Initializer, Decree):
    target: Union[Path, str]
    owner: Union[str, int, None] = None
    mode: Union[str, int, None] = None
    missing_ok = False

    _needs_chown = False
    _needs_chmod = False

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @initializer
    def _mode(self) -> Optional[int]:
        return _parse_mode(self.mode)

    @property
    def _update_needed(self) -> bool:
        target = self.target
        uid, gid = self._owner
        mode = self._mode
        try:
            st = stat(target, follow_symlinks=False)
        except FileNotFoundError:
            if not self.missing_ok:
                raise
        else:
            self._needs_chown = (uid is not None and st.st_uid != uid) or (
                gid is not None and st.st_gid != gid
            )
            self._needs_chmod = mode is not None and S_IMODE(st.st_mode) != mode

        return self._needs_chown or self._needs_chmod

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        if self._needs_chown:
            uid, gid = self._owner
            chown(
                target,
                -1 if uid is None else uid,
                -1 if gid is None else gid,
                follow_symlinks=False,
            )

        if self._needs_chmod:
            assert self._mode is not None
            chmod(target, self._mode, follow_symlinks=False)


class Delete(Initializer, Decree):
    target: Union[Path, str]
    force = False

    @property
    def _update_needed(self) -> bool:
        return access(self.target, F_OK)

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        try:
            unlink(target)
        except IsADirectoryError:
            try:
                rmdir(target)
            except OSError as e:
                if e.errno == ENOTEMPTY and self.force:
                    rmtree(target)
                else:
                    raise e from None


__all__ = ('File', 'RecursiveFiles', 'Symlink', 'Directory', 'Permissions', 'Delete')
