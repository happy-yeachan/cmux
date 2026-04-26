#!/usr/bin/env python3
"""
cmux orchestrate — Agent Worker

Generic worker that runs in a cmux split pane. Connects to the IPC
broker, registers with a role, listens for tasks, executes them via
the configured agent CLI (interactive, in-pane), and sends results back.

All agents run interactively in the cmux pane via `script` capture.
The user can watch each agent work in real time. Agents create files
directly in the --cwd project directory.

Usage:
    python3 agent_worker.py --role frontend --agent claude-code --cwd /tmp/project
    python3 agent_worker.py --role backend  --agent codex --cwd /tmp/project
    python3 agent_worker.py --role review   --agent gemini-cli --cwd /tmp/project
    python3 agent_worker.py --role frontend --agent mock
"""

import argparse
import asyncio
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

# ── ANSI colors ──────────────────────────────────────────────────────
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"

ROLE_COLORS = {
    "frontend": CYAN,
    "backend": GREEN,
    "review": YELLOW,
    "orchestrator": MAGENTA,
}

AGENT_LABELS = {
    "claude-code": f"{BLUE}Claude Code{RESET}",
    "codex": f"{GREEN}Codex{RESET}",
    "gemini-cli": f"{CYAN}Gemini CLI{RESET}",
    "mock": f"{DIM}Mock Agent{RESET}",
}

SOCKET_PATH = os.environ.get(
    "CMUX_ORCHESTRATE_SOCK", "/tmp/cmux-orchestrate.sock"
)

AGENT_TIMEOUT = int(os.environ.get("CMUX_ORCHESTRATE_TIMEOUT", "600"))

_CURRENT_AGENT: str = ""


def log(role: str, msg: str, color: str = "") -> None:
    ts = time.strftime("%H:%M:%S")
    rc = ROLE_COLORS.get(role, WHITE)
    c = color or rc
    agent_tag = f" {DIM}({_CURRENT_AGENT}){RESET}" if _CURRENT_AGENT else ""
    print(f"{DIM}{ts}{RESET} {BOLD}{rc}[{role}]{RESET}{agent_tag} {c}{msg}{RESET}", flush=True)


def banner(role: str, agent: str, cwd: str) -> None:
    rc = ROLE_COLORS.get(role, WHITE)
    print(f"""
{BOLD}{rc}╔══════════════════════════════════════════╗
║  cmux orchestrate — Agent Worker         ║
║  Role:  {role:<33s}║
║  Agent: {agent:<33s}║
║  CWD:   {cwd[:33]:<33s}║
╚══════════════════════════════════════════╝{RESET}
""", flush=True)


def _strip_ansi(text: str) -> str:
    """Strip ANSI escape sequences from text."""
    text = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", text)
    text = re.sub(r"\x1b\].*?\x07", "", text)
    text = re.sub(r"\r", "", text)
    return text


# ── Agent execution ──────────────────────────────────────────────────

async def run_agent_mock(role: str, task_payload: str) -> str:
    """Mock agent — simulates processing with colored output."""
    log(role, f"processing task...", YELLOW)
    steps = [
        ("Analyzing requirements...", 1.0),
        ("Generating code...", 1.5),
        ("Running validation...", 0.8),
        ("Finalizing output...", 0.5),
    ]
    for step_msg, delay in steps:
        log(role, f"  ├─ {step_msg}", DIM)
        await asyncio.sleep(delay)

    result = (
        f"[{role}] Completed task: {task_payload[:80]}... "
        f"Generated {role} component with 42 lines of code."
    )
    log(role, f"  └─ Done ✓", GREEN)
    return result


def _run_interactive(cmd: list[str], cwd: str, capture_file: str,
                     timeout: float = 600) -> tuple[int, str]:
    """Run a command interactively in the real terminal, capturing output via `script`.

    The agent runs visibly in the cmux pane while `script` records all
    output to a file. This gives agents full TTY access so they can
    create files, use tools, and interact with the terminal.

    macOS script syntax: script -q <file> <command> [args...]
    """
    script_cmd = ["script", "-q", capture_file] + cmd
    proc = subprocess.Popen(script_cmd, cwd=cwd)

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise TimeoutError(f"Process timed out after {timeout}s")

    # Read captured output
    try:
        raw = Path(capture_file).read_text(errors="replace")
    except FileNotFoundError:
        raw = ""
    finally:
        Path(capture_file).unlink(missing_ok=True)

    return proc.returncode, _strip_ansi(raw).strip()


async def run_agent_cli(role: str, agent: str, task_payload: str, cwd: str) -> str:
    """Run a real agent CLI interactively in the pane and capture output.

    All agents run via `script` capture in the real terminal:
      - claude-code: claude --print --dangerously-skip-permissions "prompt"
      - codex:       codex "prompt"
      - gemini-cli:  gemini -p "prompt" -y
    """
    timeout = AGENT_TIMEOUT

    if agent == "claude-code":
        cmd = ["claude", "--print", "--dangerously-skip-permissions", task_payload]
    elif agent == "codex":
        cmd = ["codex", task_payload]
    elif agent == "gemini-cli":
        cmd = ["gemini", "-p", task_payload, "-y"]
    else:
        return await run_agent_mock(role, task_payload)

    log(role, f"executing: {cmd[0]} ... (timeout: {timeout}s, cwd: {cwd})", YELLOW)
    capture_file = f"/tmp/cmux-orchestrate-{role}-{os.getpid()}.log"

    try:
        returncode, output = await asyncio.wait_for(
            asyncio.to_thread(_run_interactive, cmd, cwd, capture_file, timeout),
            timeout=timeout + 10,
        )

        if returncode != 0:
            log(role, f"agent exited with code {returncode}", RED)
            if output:
                log(role, f"output: {output[:200]}", RED)
            return f"[ERROR] {agent} failed (exit {returncode}): {output[:300]}"

        log(role, f"agent completed ({len(output)} chars)", GREEN)
        return output

    except (TimeoutError, asyncio.TimeoutError):
        log(role, f"agent timed out ({timeout}s)", RED)
        return f"[ERROR] {agent} timed out after {timeout}s"
    except FileNotFoundError:
        log(role, f"'{cmd[0]}' not found — falling back to mock", RED)
        return await run_agent_mock(role, task_payload)


# ── Broker communication ─────────────────────────────────────────────

class WorkerClient:
    def __init__(self, role: str, agent: str, cwd: str) -> None:
        self.role = role
        self.agent = agent
        self.cwd = cwd
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._running = True

    async def connect(self, retries: int = 10, delay: float = 1.0) -> None:
        for attempt in range(retries):
            try:
                self.reader, self.writer = await asyncio.open_unix_connection(
                    SOCKET_PATH
                )
                log(self.role, "connected to broker", GREEN)
                return
            except (FileNotFoundError, ConnectionRefusedError):
                if attempt < retries - 1:
                    log(self.role, f"broker not ready, retrying ({attempt + 1}/{retries})...", DIM)
                    await asyncio.sleep(delay)
        raise ConnectionError("Could not connect to broker")

    async def send(self, msg: dict) -> None:
        assert self.writer is not None
        self.writer.write((json.dumps(msg) + "\n").encode())
        await self.writer.drain()

    async def recv(self) -> dict | None:
        assert self.reader is not None
        line = await self.reader.readline()
        if not line:
            return None
        return json.loads(line.decode().strip())

    async def register(self) -> None:
        await self.send({"type": "register", "role": self.role})
        resp = await self.recv()
        if resp and resp.get("type") == "registered":
            log(self.role, f"registered as '{self.role}'", GREEN)
        else:
            log(self.role, f"unexpected register response: {resp}", RED)

    async def run(self) -> None:
        await self.connect()
        await self.register()

        log(self.role, "waiting for tasks...", DIM)

        while self._running:
            msg = await self.recv()
            if msg is None:
                log(self.role, "broker disconnected", RED)
                break

            mtype = msg.get("type")

            if mtype == "task":
                payload = msg.get("payload", "")
                sender = msg.get("from", "?")
                log(self.role, f"received task from {sender}", CYAN)
                log(self.role, f"  task: {payload[:100]}", WHITE)

                # Execute via configured agent
                if self.agent == "mock":
                    result = await run_agent_mock(self.role, payload)
                else:
                    result = await run_agent_cli(self.role, self.agent, payload, self.cwd)

                # Send result back (include agent/model info)
                await self.send({
                    "type": "result",
                    "from": self.role,
                    "to": sender,
                    "agent": self.agent,
                    "payload": result,
                })
                log(self.role, f"result sent → {sender}", GREEN)

            elif mtype == "shutdown":
                log(self.role, "shutdown received", YELLOW)
                break

            elif mtype == "broadcast":
                payload = msg.get("payload", "")
                sender = msg.get("from", "?")
                log(self.role, f"broadcast from {sender}: {str(payload)[:100]}", BLUE)

    def stop(self) -> None:
        self._running = False
        if self.writer:
            self.writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="cmux orchestrate agent worker")
    parser.add_argument(
        "--role", required=True,
        help="Worker role (e.g., frontend, backend, review)"
    )
    parser.add_argument(
        "--agent", default="mock",
        choices=["claude-code", "codex", "gemini-cli", "mock"],
        help="Agent CLI to use (default: mock)"
    )
    parser.add_argument(
        "--socket", default=None,
        help="Broker socket path (default: /tmp/cmux-orchestrate.sock)"
    )
    parser.add_argument(
        "--cwd", default=None,
        help="Working directory for agent execution"
    )
    args = parser.parse_args()

    if args.socket:
        global SOCKET_PATH
        SOCKET_PATH = args.socket

    global _CURRENT_AGENT
    _CURRENT_AGENT = args.agent

    cwd = args.cwd or os.getcwd()

    banner(args.role, args.agent, cwd)

    client = WorkerClient(role=args.role, agent=args.agent, cwd=cwd)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, client.stop)

    try:
        await client.run()
    except ConnectionError as e:
        log(args.role, str(e), RED)
        sys.exit(1)
    except asyncio.CancelledError:
        pass
    finally:
        log(args.role, "worker stopped", RED)


if __name__ == "__main__":
    asyncio.run(main())
