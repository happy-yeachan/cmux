#!/usr/bin/env python3
"""
cmux orchestrate — Agent Worker

Generic worker that runs in a cmux split pane. Connects to the IPC
broker, registers with a role, listens for tasks, executes them via
the configured agent CLI (headless), and sends results back.

Usage:
    python3 agent_worker.py --role frontend --agent claude-code
    python3 agent_worker.py --role backend  --agent codex
    python3 agent_worker.py --role review   --agent gemini-cli
    python3 agent_worker.py --role frontend --agent mock   # (mock mode)
"""

import argparse
import asyncio
import json
import os
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


def log(role: str, msg: str, color: str = "") -> None:
    ts = time.strftime("%H:%M:%S")
    rc = ROLE_COLORS.get(role, WHITE)
    c = color or rc
    print(f"{DIM}{ts}{RESET} {BOLD}{rc}[{role}]{RESET} {c}{msg}{RESET}", flush=True)


def banner(role: str, agent: str) -> None:
    rc = ROLE_COLORS.get(role, WHITE)
    al = AGENT_LABELS.get(agent, agent)
    print(f"""
{BOLD}{rc}╔══════════════════════════════════════════╗
║  cmux orchestrate — Agent Worker         ║
║  Role:  {role:<33s}║
║  Agent: {agent:<33s}║
╚══════════════════════════════════════════╝{RESET}
""", flush=True)


# ── Agent execution backends ─────────────────────────────────────────

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


async def run_agent_cli(role: str, agent: str, task_payload: str) -> str:
    """Run a real agent CLI in headless mode and capture output."""
    # Build the CLI command based on agent type
    if agent == "claude-code":
        cmd = ["claude", "--print", "--output-format", "text", task_payload]
    elif agent == "codex":
        cmd = ["codex", "--quiet", task_payload]
    elif agent == "gemini-cli":
        cmd = ["gemini", "--non-interactive", task_payload]
    else:
        return await run_agent_mock(role, task_payload)

    log(role, f"executing: {' '.join(cmd[:3])}...", YELLOW)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=os.environ.get("CMUX_ORCHESTRATE_CWD", os.getcwd()),
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
        output = stdout.decode().strip()

        if proc.returncode != 0:
            err = stderr.decode().strip()
            log(role, f"agent exited with code {proc.returncode}", RED)
            if err:
                log(role, f"stderr: {err[:200]}", RED)
            return f"[ERROR] {agent} failed (exit {proc.returncode}): {err[:200]}"

        log(role, f"agent completed ({len(output)} chars)", GREEN)
        return output

    except asyncio.TimeoutError:
        log(role, "agent timed out (300s)", RED)
        return f"[ERROR] {agent} timed out after 300s"
    except FileNotFoundError:
        log(role, f"'{cmd[0]}' not found — falling back to mock", RED)
        return await run_agent_mock(role, task_payload)


# ── Broker communication ─────────────────────────────────────────────

class WorkerClient:
    def __init__(self, role: str, agent: str) -> None:
        self.role = role
        self.agent = agent
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
                    result = await run_agent_cli(self.role, self.agent, payload)

                # Send result back
                await self.send({
                    "type": "result",
                    "from": self.role,
                    "to": sender,
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
    args = parser.parse_args()

    if args.socket:
        global SOCKET_PATH
        SOCKET_PATH = args.socket

    banner(args.role, args.agent)

    client = WorkerClient(role=args.role, agent=args.agent)

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
