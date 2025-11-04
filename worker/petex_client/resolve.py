from .server import PetexServer

def start(srv: PetexServer) -> None:
    srv.do_cmd("Resolve.Start()")

def extract_archive(srv: PetexServer, archive_file, archive_dir):
    srv.do_cmd('Resolve.EXTRACTARCHIVE(\''+ archive_file +'\', \''+ archive_dir + '\')')  
    return None

def open_file(srv: PetexServer, path: str) -> None:
    srv.do_cmd(f"RESOLVE.OPENFILE('{path}')")

def run_scenario(srv: PetexServer, name: str) -> None:
    srv.do_cmd(f"Resolve.RUNSCENARIO('{name}')")

def create_archive(srv: PetexServer, archivefile: str, force: int = 0) -> None:
    """
    Create a Resolve archive file.

    Parameters
    ----------
    srv : PetexServer
        Active Petex COM connection.
    archivefile : str
        Full path to archive file (e.g., r"C:\\Models\\archive.rsz").
    force : int, optional
        Overwrite flag (0 = don’t overwrite, 1 = overwrite). Default 0.
    """
    srv.do_cmd(f"Resolve.CREATEARCHIVE('{archivefile}', {force})")

def is_error(srv: PetexServer) -> bool:
    """Return True if Resolve reports an error after last action."""
    val = srv.get_value("Resolve.IsError")
    try:
        return int(str(val).strip() or "0") != 0
    except Exception:
        return bool(val)

def error_msg(srv: PetexServer) -> str:
    """Return Resolve error message string (may be empty)."""
    return srv.get_value("Resolve.ErrorMessage")

def shutdown(srv: PetexServer) -> None:
    """Shutdown the Resolve application."""
    srv.do_cmd("Resolve.SHUTDOWN()")
