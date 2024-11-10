from subprocess import DEVNULL, run
from typing import Any, Optional

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
        super()._prepare(name)

        if is_true(self.mask) and (is_true(self.enable) or is_true(self.active)):
            raise RuntimeError(
                "{self.name}: masked units can't be enabled or activated"
            )

        if is_false(self.active) and (is_true(self.reload) or is_true(self.restart)):
            raise RuntimeError(
                "{self.name}: deactivated units can't be reloaded or restarted"
            )

    def _systemctl_is_enabled(self) -> str:
        command = ['systemctl', 'is-enabled', self.unit]
        result = run(command, capture_output=True, text=True, stdin=DEVNULL)

        enabled = result.stdout.strip()
        returncode = result.returncode
        if not enabled and returncode:
            if returncode == 1:
                # Assume the unit doesn't exist:
                return 'disabled'
            raise RuntimeError(
                f"command '{' '.join(command)}' returned non-zero exit status {returncode}:\n{result.stderr}"
            )

        return enabled

    def _systemctl_is_active(self) -> bool:
        command = ['systemctl', 'is-active', '--quiet', self.unit]
        result = run(command, capture_output=True, text=True, stdin=DEVNULL)
        returncode = result.returncode
        if returncode == 0:
            return True
        elif returncode == 3:
            return False
        else:
            raise RuntimeError(
                f"command '{' '.join(command)}' returned non-zero exit status {returncode}:\n{result.stderr}"
            )

    @property
    def _update_needed(self) -> bool:
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
            if self._systemctl_is_active():
                self._was_active = True
                if not active:
                    self._change_active = False
            else:
                if active:
                    self._change_active = True

    def _update(self) -> None:
        unit = self.unit

        change_mask = self._change_mask
        change_enable = self._change_enable
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
                and change_enable
                and self._systemctl_is_enabled() == 'enabled'
            ):
                # It turned out to be enabled already, after unmasking
                change_enable = None
                self._change_enable = None

        change_active = self._change_active
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
            run(command, check=True, stdin=DEVNULL)

    @property
    def _should_activate(self) -> bool:
        if not self.reload and not self.restart:
            return False

        if self._change_active is not None:
            # We just started or stopped it, no use reloading/restarting it again
            return False

        if not self._was_active:
            # It wasn't active and we didn't start it. Nothing to do, then.
            return False

        return super()._should_activate

    def _activate(self) -> None:
        command = ['systemctl']

        if self.reload:
            if self.restart:
                command.append('try-reload-or-restart')
            else:
                command.append('reload')
        else:
            command.append('try-restart')

        command.append(self.unit)
        run(command, check=True, stdin=DEVNULL)

    @property
    def _summary(self) -> dict[str, Any]:
        summary = super()._summary

        update_summary = {}
        if self._change_enable:
            update_summary['enable'] = bool(enable)
        if self._change_active:
            update_summary['active'] = bool(active)
        if self._change_mask:
            update_summary['mask'] = bool(mask)
        if update_summary:
            summary['updated'] = update_summary

        if self.activated:
            summary['activated'] = '-or-'.join(
                method for method in ('reload', 'restart') if getattr(self, method)
            )

        return summary


__all__ = ('Service',)
