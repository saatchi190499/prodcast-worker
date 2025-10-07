import time
from typing import Optional, Literal
import pythoncom
from win32com.client import Dispatch
from .exceptions import PetexException

AppName = Literal["PROSPER", "MBAL", "GAP", "PVT", "RESOLVE", "REVEAL"]

_DEFAULT_PROGID = "PX32.OpenServer.1"


def _app_name_from_tag(tag: str) -> AppName:
    """Extract and validate the application name from a tag like 'GAP.MOD[...]'."""
    try:
        point = tag.index(".")
    except ValueError as e:
        raise PetexException(f"Invalid tag (no '.'): {tag}") from e

    name = tag[:point].upper()
    if name not in {"PROSPER", "MBAL", "GAP", "PVT", "RESOLVE", "REVEAL"}:
        raise PetexException(f"Unrecognised application name in tag string ({name})")
    return name  # type: ignore[return-value]


class PetexServer:
    """
    Safe wrapper around PX32.OpenServer.1 with:
      - pythoncom CoInitialize/Uninitialize
      - error checking after commands
      - async wait with timeout
    """

    def __init__(self, progid: str = _DEFAULT_PROGID):
        self._progid = progid
        self._server = None

    # Context manager support
    def __enter__(self) -> "PetexServer":
        pythoncom.CoInitialize()
        self._server = Dispatch(self._progid)
        if self._server is None:
            raise PetexException("Unable to acquire COM server (license or connectivity issue)")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        self._server = None
        try:
            pythoncom.CoUninitialize()
        except Exception:
            # avoid masking real exceptions on exit
            pass

    # --- Core primitives -----------------------------------------------------

    def do_cmd(self, command: str) -> None:
        """Run a synchronous command; raise on error."""
        self._ensure()
        err = self._server.DoCommand(command)  # type: ignore[union-attr]
        if err > 0:
            raise PetexException(f"DoCmd: {command} - {self._server.GetErrorDescription(err)}")  # type: ignore[union-attr]

    def do_cmd_async_wait(self, command: str, poll_s: float = 0.5, timeout_s: float = 300) -> None:
        """Run async then poll IsBusy(app) with a timeout."""
        self._ensure()
        app = _app_name_from_tag(command)
        err = self._server.DoCommandAsync(command)  # type: ignore[union-attr]
        if err > 0:
            raise PetexException(f"DoSlowCmd: {command} - {self._server.GetErrorDescription(err)}")  # type: ignore[union-attr]

        deadline = time.time() + timeout_s
        while self._server.IsBusy(app) > 0:  # type: ignore[union-attr]
            if time.time() > deadline:
                raise PetexException(f"Timeout waiting for {app} to finish: {command}")
            time.sleep(poll_s)

        last_err = self._server.GetLastError(app)  # type: ignore[union-attr]
        if last_err > 0:
            raise PetexException(f"DoSlowCmd (post): {command} - {self._server.GetErrorDescription(last_err)}")  # type: ignore[union-attr]

    def set_value(self, tag: str, value) -> None:
        """Set a value and check last error for that app."""
        self._ensure()
        app = _app_name_from_tag(tag)
        _ = self._server.SetValue(tag, value)  # type: ignore[union-attr]
        err = self._server.GetLastError(app)  # type: ignore[union-attr]
        if err > 0:
            raise PetexException(f"DoSet: {tag} - {value} - {self._server.GetErrorDescription(err)}")  # type: ignore[union-attr]

    def get_value(self, tag: str) -> str:
        """Get a value and check last error for that app. Always returns string from COM; convert upstream."""
        self._ensure()
        app = _app_name_from_tag(tag)
        val = self._server.GetValue(tag)  # type: ignore[union-attr]
        err = self._server.GetLastError(app)  # type: ignore[union-attr]
        if err > 0:
            # Prefer GetLastErrorMessage if available; fall back to description
            try:
                msg = self._server.GetLastErrorMessage(app)  # type: ignore[union-attr]
            except Exception:
                msg = self._server.GetErrorDescription(err)  # type: ignore[union-attr]
            raise PetexException(f"DoGet: {tag} - {msg}")
        return val

    # --- Helpers to mirror your patterns ------------------------------------

    def gap_func(self, cmd: str, async_: bool = False, **wait_opts) -> str:
        """
        Execute a GAP function (sets LASTCMDRET) and return LASTCMDRET.
        """
        if async_:
            self.do_cmd_async_wait(cmd, **wait_opts)
        else:
            self.do_cmd(cmd)
        ret = self.get_value("GAP.LASTCMDRET")
        # Verify GAP last error
        self._check_last_error("GAP", context=f"GAP func: {cmd}")
        return ret

    def prosper_func(self, cmd: str, async_: bool = False, **wait_opts) -> str:
        if async_:
            self.do_cmd_async_wait(cmd, **wait_opts)
        else:
            self.do_cmd(cmd)
        ret = self.get_value("PROSPER.LASTCMDRET")
        self._check_last_error("PROSPER", context=f"PROSPER func: {cmd}")
        return ret

    def _check_last_error(self, app: AppName, context: str = ""):
        self._ensure()
        err = self._server.GetLastError(app)  # type: ignore[union-attr]
        if err > 0:
            raise PetexException(f"{context}: {self._server.GetErrorDescription(err)}")  # type: ignore[union-attr]

    def _ensure(self):
        if self._server is None:
            raise PetexException("COM server not initialized; use 'with PetexServer() as srv:' or call __enter__()")