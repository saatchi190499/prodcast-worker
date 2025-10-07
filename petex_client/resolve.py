from .server import PetexServer

def start(srv: PetexServer) -> None:
    srv.do_cmd("Resolve.Start()")

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

def shutdown(srv: PetexServer) -> None:
    """Shutdown the Resolve application."""
    srv.do_cmd("Resolve.SHUTDOWN()")

