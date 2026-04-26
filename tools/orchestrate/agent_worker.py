#!/usr/bin/env python3
"""
cmux orchestrate — Agent Worker

Generic worker that runs in a cmux split pane. Connects to the IPC
broker, registers with a role, listens for tasks, executes them via
the configured agent CLI (interactive, visible in-pane), and sends
results back through the broker.

All agents run interactively in the cmux pane via `script` capture.
The user can watch each agent work in real time. Agents create files
directly in the --cwd project directory.

No hard timeout — workers use health-check (capture file activity)
to detect agent liveness and send periodic progress snapshots to
the broker for cross-phase context sharing.

Usage:
    python3 agent_worker.py --role frontend --agent claude-code --cwd ~/projects/app
    python3 agent_worker.py --role backend  --agent codex --cwd ~/projects/app
    python3 agent_worker.py --role review   --agent gemini-cli --cwd ~/projects/app
    python3 agent_worker.py --role frontend --agent mock
"""

import argparse
import asyncio
import json
import os
import re
import shlex
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

SOCKET_PATH = os.environ.get(
    "CMUX_ORCHESTRATE_SOCK", "/tmp/cmux-orchestrate.sock"
)
SCRIPT_DIR = Path(__file__).resolve().parent

# Health-check interval (seconds)
HEALTH_CHECK_INTERVAL = 10
# Snapshot send interval (seconds) — how often to push progress to broker
SNAPSHOT_INTERVAL = 30

_CURRENT_AGENT: str = ""


# ── Agent config from JSON ──────────────���────────────────────────────

def load_agent_config(agent_name: str) -> dict | None:
    """Load agent execution config from agents.json."""
    config_paths = [
        Path(os.environ.get("CMUX_ORCHESTRATE_AGENTS", "")),
        SCRIPT_DIR / "agents.json",
        Path.home() / ".config" / "cmux-orchestrate" / "agents.json",
    ]
    for p in config_paths:
        if p.is_file():
            try:
                data = json.loads(p.read_text())
                for agent in data.get("agents", []):
                    if agent.get("name") == agent_name:
                        return agent
            except (json.JSONDecodeError, OSError):
                continue
    return None


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


def _read_capture(capture_file: str) -> str:
    """Read and clean capture file contents."""
    try:
        raw = Path(capture_file).read_text(errors="replace")
        return _strip_ansi(raw).strip()
    except FileNotFoundError:
        return ""


# ── Agent execution ──────────────────────────────────────────────────

async def run_agent_mock(role: str, task_payload: str) -> str:
    """Mock agent — simulates processing with colored output."""
    log(role, "processing task...", YELLOW)
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
    log(role, "  └─ Done ✓", GREEN)
    return result


def _run_interactive(cmd: list[str], cwd: str, capture_file: str,
                     progress_callback=None,
                     stdin_text: str | None = None) -> tuple[int, str]:
    """Run a command interactively in the real terminal with health-check.

    No hard timeout. The agent runs until it exits naturally.
    Health-check monitors capture file activity. Progress snapshots
    are sent periodically via the callback.

    Args:
        stdin_text: If provided, pipe this text to the process stdin then
                    close stdin (EOF). Used for agents like claude-code that
                    read the prompt from stdin and show interactive TUI.

    macOS script syntax: script -q <file> <command> [args...]
    """
    if stdin_text:
        # Wrap command to pipe prompt via stdin: bash -c 'cmd < prompt_file'
        prompt_file = capture_file + ".prompt"
        Path(prompt_file).write_text(stdin_text)
        shell_cmd = " ".join(shlex.quote(c) for c in cmd) + f" < {shlex.quote(prompt_file)}"
        script_cmd = ["script", "-q", capture_file, "bash", "-c", shell_cmd]
    else:
        script_cmd = ["script", "-q", capture_file] + cmd

    proc = subprocess.Popen(script_cmd, cwd=cwd)

    last_size = 0
    stall_checks = 0
    last_snapshot_time = time.time()
    start_time = time.time()

    while proc.poll() is None:
        time.sleep(HEALTH_CHECK_INTERVAL)

        # Health-check: monitor capture file size
        try:
            current_size = Path(capture_file).stat().st_size
        except FileNotFoundError:
            current_size = 0

        elapsed = int(time.time() - start_time)

        if current_size > last_size:
            stall_checks = 0
            delta = current_size - last_size
            log("", f"  health: active (+{delta}B, {elapsed}s elapsed)", DIM)
            last_size = current_size
        else:
            stall_checks += 1
            if stall_checks % 6 == 0:  # every ~60s of no activity
                log("", f"  health: idle for {stall_checks * HEALTH_CHECK_INTERVAL}s ({elapsed}s elapsed)", YELLOW)

        # Send progress snapshot periodically
        now = time.time()
        if progress_callback and (now - last_snapshot_time) >= SNAPSHOT_INTERVAL:
            snapshot = _read_capture(capture_file)
            if snapshot:
                progress_callback(snapshot[-3000:])  # last 3000 chars
            last_snapshot_time = now

    # Read final output
    output = _read_capture(capture_file)
    Path(capture_file).unlink(missing_ok=True)
    Path(capture_file + ".prompt").unlink(missing_ok=True)

    elapsed = int(time.time() - start_time)
    log("", f"  agent exited (code={proc.returncode}, {elapsed}s, {len(output)} chars)", GREEN if proc.returncode == 0 else RED)

    return proc.returncode, output


def _build_agent_cmd(agent: str, task_payload: str) -> tuple[list[str], str | None]:
    """Build command and stdin_text from agents.json config.

    Returns (cmd, stdin_text). stdin_text is None for positional/flag modes.

    Execution modes (from agents.json):
      - "stdin":      prompt piped via stdin, agent shows interactive TUI
      - "positional": prompt as last CLI argument
      - "flag":       prompt via a flag (e.g. -p "prompt")
    """
    config = load_agent_config(agent)

    if config:
        exe = config["command"]
        exec_cfg = config.get("execution", {})
        mode = exec_cfg.get("mode", "positional")
        extra_args = exec_cfg.get("args", [])
        prompt_flag = exec_cfg.get("prompt_flag", "")

        if mode == "stdin":
            # Prompt piped via stdin → agent runs interactive TUI, exits on EOF
            cmd = [exe] + extra_args
            return cmd, task_payload
        elif mode == "flag":
            cmd = [exe] + extra_args + [prompt_flag, task_payload] if prompt_flag else [exe] + extra_args + [task_payload]
            return cmd, None
        else:  # positional
            cmd = [exe] + extra_args + [task_payload]
            return cmd, None

    # Fallback: hardcoded defaults if agent not in JSON
    if agent == "claude-code":
        return ["claude", "--dangerously-skip-permissions"], task_payload
    elif agent == "codex":
        return ["codex", task_payload], None
    elif agent == "gemini-cli":
        return ["gemini", "-p", task_payload, "-y"], None
    else:
        return [], None


async def run_agent_cli(role: str, agent: str, task_payload: str, cwd: str,
                        progress_callback=None) -> str:
    """Run a real agent CLI interactively in the pane.

    Agent command and execution mode are loaded from agents.json.
    All agents run via `script` capture — visible in cmux pane.
    No timeout — agents run until they exit naturally.
    """
    cmd, stdin_text = _build_agent_cmd(agent, task_payload)
    if not cmd:
        return await run_agent_mock(role, task_payload)

    mode_label = "stdin→interactive" if stdin_text else "interactive"
    log(role, f"executing ({mode_label}): {cmd[0]} (cwd: {cwd})", YELLOW)
    capture_file = f"/tmp/cmux-orchestrate-{role}-{os.getpid()}.log"

    try:
        returncode, output = await asyncio.to_thread(
            _run_interactive, cmd, cwd, capture_file, progress_callback, stdin_text
        )

        if returncode != 0:
            log(role, f"agent exited with code {returncode}", RED)
            if output:
                log(role, f"output: {output[:200]}", RED)
            return f"[ERROR] {agent} failed (exit {returncode}): {output[:300]}"

        log(role, f"agent completed ({len(output)} chars)", GREEN)
        return output

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

    def send_sync(self, msg: dict) -> None:
        """Synchronous send for use from non-async threads (progress callback)."""
        if self.writer and not self.writer.is_closing():
            data = (json.dumps(msg) + "\n").encode()
            self.writer.write(data)

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

                # Progress callback sends snapshots through broker
                def on_progress(snapshot: str):
                    self.send_sync({
                        "type": "progress",
                        "from": self.role,
                        "snapshot": snapshot,
                    })

                # Execute via configured agent
                if self.agent == "mock":
                    result = await run_agent_mock(self.role, payload)
                else:
                    result = await run_agent_cli(
                        self.role, self.agent, payload, self.cwd,
                        progress_callback=on_progress,
                    )

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
