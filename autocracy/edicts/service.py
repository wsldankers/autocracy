from subprocess import run
from typing import Optional
from subprocess import run, DEVNULL

from ..utils import *
from .base import Decree


class Service(Initializer, Decree):
    unit: str
    reload = False
    restart = False
    enable: Optional[bool] = None
    active: Optional[bool] = None
    mask: Optional[bool] = None

    _change_enable: Optional[bool] = None
    _change_active: Optional[bool] = None
    _change_mask: Optional[bool] = None
    _was_active = False

    def _prepare(self, name: Optional[str] = None) -> None:
        super()._prepare()

        if is_true(self.mask) and (is_true(self.enable) or is_true(self.active)):
            raise RuntimeError(
                "{self.name}: masked units can't be enabled or activated"
            )

        if is_false(self.active) and (is_true(self.reload) or is_true(self.restart)):
            raise RuntimeError(
                "{self.name}: deactivated units can't be reloaded or restarted"
            )

    def _systemctl_is_enabled(self):
        command = ['systemctl', 'is-enabled', self.unit]
        result = run(command, capture_output=True, text=True, stdin=DEVNULL)

        enabled = result.stdout.strip()
        returncode = result.returncode
        if not enabled and returncode:
            if returncode == 1:
                # Assume the unit doesn't exist:
                return False
            raise RuntimeError(
                f"command '{' '.join(command)}' returned non-zero exit status {returncode}:\n{result.stderr}"
            )

        return enabled

    @property
    def _needs_update(self) -> bool:
        unit = self.unit
        enable = self.enable
        mask = self.mask
        if enable is not None or mask is not None:
            enabled = self._systemctl_is_enabled()

            bool_mask = bool(mask)
            if mask is not None and (enabled == 'masked') != bool_mask:
                self._change_mask = bool_mask

            bool_enable = bool(enable)
            if enable is not None and (
                enabled == 'masked' or (enabled == 'enabled') != bool_enable
            ):
                self._change_enable = bool_enable

        active = self.active
        if active is not None:
            command = ['systemctl', 'is-active', '--quiet', unit]
            result = run(command, capture_output=True, text=True, stdin=DEVNULL)
            returncode = result.returncode
            if returncode == 0:
                self._was_active = True
                if not active:
                    self._change_active = False
            elif returncode == 3:
                if active:
                    self._change_active = True
            else:
                raise RuntimeError(
                    f"command '{' '.join(command)}' returned non-zero exit status {returncode}:\n{result.stderr}"
                )

        return bool(self._change_enable or self._change_active or self._change_mask)

    def _update(self) -> None:
        unit = self.unit

        change_active = self._change_active
        change_mask = self._change_mask
        if change_mask is not None:
            command = ['systemctl']
            if change_mask:
                command.append('mask')
            else:
                command.append('unmask')
            command.append(unit)
            run(command, text=True, check=True, stdin=DEVNULL)

            # Avoid running _systemctl_is_enabled() unless we really need the
            # extra info:
            if (
                not change_mask
                and change_active
                and (self.reload or self.restart)
                and self._systemctl_is_enabled() == 'active'
            ):
                # It turned out to be active already, after unmasking
                change_active = None
                self._change_active = None
                self._was_active = True

        change_enable = self._change_enable
        if change_enable is not None or change_active is not None:
            command = ['systemctl']
            if change_enable is not None:
                if change_enable:
                    command.append('enable')
                    if is_true(change_active):
                        command.append('--now')
                else:
                    command.append('disable')
                    if is_false(change_active):
                        command.append('--now')
            else:
                if change_active:
                    command.append('start')
                else:
                    command.append('stop')

            command.append(unit)
            run(command, text=True, check=True, stdin=DEVNULL)

    def _activate(self) -> None:
        reload = self.reload
        restart = self.restart

        if not reload and not restart:
            return

        if self._change_active is not None:
            # We just started or stopped it, no use reloading/restarting it again
            return

        if not self._was_active:
            # It wasn't active and we didn't start it. Nothing to do, then.
            return

        command = ['systemctl']

        if reload:
            if restart:
                command.append('try-reload-or-restart')
            else:
                command.append('reload')
        else:
            command.append('try-restart')

        command.append(self.unit)
        run(command, text=True, check=True, stdin=DEVNULL)


__all__ = ('Service',)
