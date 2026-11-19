"""Kill / start / add workers and damage a copy, through the docker command.

Only works when the system runs with docker compose (container names
object-store-worker-1, -2, ...). Without docker, these actions are refused with
a message and everything else in the dashboard still works.
"""
import asyncio
import locale
import os
import re
import shutil

# Flip the first byte of a chunk file inside a worker container (integrity demo).
_CORRUPT = ("import sys; p = sys.argv[1]; b = bytearray(open(p, 'rb').read()); "
            "b[0] ^= 0xFF; open(p, 'wb').write(bytes(b)); print('damaged', p)")


class DockerError(Exception):
    pass


class Docker:
    def __init__(self, project: str = "object-store", compose_file: str = None):
        self.project = project
        self.compose_file = compose_file or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                         "docker-compose.yml")
        self.available = shutil.which("docker") is not None

    def container(self, worker_id: str) -> str:
        """worker2 -> object-store-worker-2 (compose numbers the containers)."""
        m = re.search(r"(\d+)$", worker_id)
        if not m:
            raise DockerError(f"cannot tell the container of '{worker_id}'")
        return f"{self.project}-worker-{m.group(1)}"

    async def run(self, *args, timeout: float = 120) -> str:
        if not self.available:
            raise DockerError("docker is not available here (this needs the system running in Docker)")
        proc = await asyncio.create_subprocess_exec(
            "docker", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            raise DockerError(f"docker {' '.join(args)}: timed out") from None
        text = out.decode(errors="replace").strip()
        if proc.returncode != 0:
            raise DockerError(text.splitlines()[-1] if text else f"docker {args[0]} failed")
        return text

    async def kill(self, worker_id: str) -> str:
        """docker kill: the worker stops at once, like a crash (no goodbye to the controller)."""
        return await self.run("kill", self.container(worker_id))

    async def start(self, worker_id: str) -> str:
        return await self.run("start", self.container(worker_id))

    async def scale(self, count: int) -> str:
        return await self.run("compose", "-p", self.project, "-f", self.compose_file,
                              "up", "-d", "--no-recreate", "--scale", f"worker={count}", "worker", timeout=300)

    async def corrupt(self, worker_id: str, chunk_id: str) -> str:
        path = f"/data/{worker_id}/{chunk_id}"
        return await self.run("exec", self.container(worker_id), "python", "-c", _CORRUPT, path)


async def run_shell(command: str, on_line, on_process=None) -> int:
    """Run a shell command (cmd.exe on Windows, sh elsewhere) and pass every
    output line to on_line as it comes. Returns the exit code."""
    proc = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        stdin=asyncio.subprocess.DEVNULL)
    if on_process:
        on_process(proc)
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError:              # e.g. cmd.exe's own codepage
            text = line.decode(locale.getpreferredencoding(False), errors="replace")
        on_line(text.rstrip("\r\n"))
    return await proc.wait()
