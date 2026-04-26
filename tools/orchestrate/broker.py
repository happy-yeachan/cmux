#!/usr/bin/env python3
"""
cmux orchestrate — IPC Message Broker

Zero-dependency Unix domain socket broker that routes JSON messages
between agent workers by role. Supports register, task, result, and
broadcast message types.

Protocol: newline-delimited JSON over Unix domain socket.
"""

import asyncio
import json
import os
import signal
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

SOCKET_PATH = os.environ.get(
    "CMUX_ORCHESTRATE_SOCK", "/tmp/cmux-orchestrate.sock"
)

# ── Message types ────────────────────────────────────────────────────
# { "type": "register",   "role": "frontend" }
# { "type": "task",       "to": "frontend", "from": "orchestrator", "payload": "..." }
# { "type": "result",     "to": "orchestrator", "from": "frontend", "payload": "..." }
# { "type": "broadcast",  "from": "orchestrator", "payload": "..." }
# { "type": "shutdown" }
# { "type": "status" }                         → broker replies with connected roles
# { "type": "wait_ready", "roles": [...] }     → broker replies when all roles registered


def log(msg: str, color: str = DIM) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{DIM}{ts}{RESET} {BOLD}{MAGENTA}[broker]{RESET} {color}{msg}{RESET}", flush=True)


class Broker:
    def __init__(self) -> None:
        # role -> list of (reader, writer) — multiple clients per role allowed
        self.clients: dict[str, list[asyncio.StreamWriter]] = {}
        self.server: asyncio.AbstractServer | None = None
        self._ready_waiters: list[tuple[set[str], asyncio.Future]] = []

    # ── connection handler ───────────────────────────────────────────
    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        role: str | None = None
        peer = writer.get_extra_info("peername") or "unknown"
        log(f"connection from {peer}", CYAN)

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line.decode().strip())
                except json.JSONDecodeError:
                    await self._send(writer, {"error": "invalid JSON"})
                    continue

                mtype = msg.get("type")

                if mtype == "register":
                    role = msg.get("role", "unknown")
                    self.clients.setdefault(role, []).append(writer)
                    log(f"registered: {BOLD}{role}{RESET}", GREEN)
                    await self._send(writer, {"type": "registered", "role": role})
                    self._check_ready_waiters()

                elif mtype == "task":
                    target = msg.get("to")
                    log(f"task {msg.get('from', '?')} → {target}", YELLOW)
                    await self._route(msg, target)

                elif mtype == "result":
                    target = msg.get("to", "orchestrator")
                    log(f"result {msg.get('from', '?')} → {target}", GREEN)
                    await self._route(msg, target)

                elif mtype == "progress":
                    # Route progress snapshots to orchestrator
                    sender = msg.get("from", "?")
                    log(f"progress {sender} → orchestrator", DIM)
                    await self._route(msg, "orchestrator")

                elif mtype == "broadcast":
                    sender = msg.get("from", "?")
                    log(f"broadcast from {sender}", BLUE)
                    await self._broadcast(msg, exclude_role=sender)

                elif mtype == "status":
                    roles = list(self.clients.keys())
                    await self._send(writer, {"type": "status", "roles": roles})

                elif mtype == "wait_ready":
                    needed = set(msg.get("roles", []))
                    if needed.issubset(set(self.clients.keys())):
                        await self._send(writer, {"type": "ready", "roles": list(needed)})
                    else:
                        fut: asyncio.Future = asyncio.get_event_loop().create_future()
                        self._ready_waiters.append((needed, fut))
                        # store writer so we can reply later
                        fut.add_done_callback(
                            lambda f, w=writer: asyncio.ensure_future(
                                self._send(w, {"type": "ready", "roles": list(needed)})
                            )
                        )

                elif mtype == "shutdown":
                    log("shutdown requested", RED)
                    await self._broadcast({"type": "shutdown"})
                    asyncio.get_event_loop().call_soon(self._stop)
                    return

                else:
                    await self._send(writer, {"error": f"unknown type: {mtype}"})

        except asyncio.CancelledError:
            pass
        except ConnectionResetError:
            pass
        finally:
            if role and role in self.clients:
                self.clients[role] = [
                    w for w in self.clients[role] if w is not writer
                ]
                if not self.clients[role]:
                    del self.clients[role]
                log(f"disconnected: {role}", RED)
            writer.close()

    # ── routing helpers ──────────────────────────────────────────────
    async def _send(self, writer: asyncio.StreamWriter, msg: dict) -> None:
        try:
            writer.write((json.dumps(msg) + "\n").encode())
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass

    async def _route(self, msg: dict, target: str | None) -> None:
        if not target:
            return
        writers = self.clients.get(target, [])
        if not writers:
            log(f"no client for role '{target}' — dropping message", RED)
            return
        for w in writers:
            await self._send(w, msg)

    async def _broadcast(self, msg: dict, exclude_role: str | None = None) -> None:
        for role, writers in self.clients.items():
            if role == exclude_role:
                continue
            for w in writers:
                await self._send(w, msg)

    def _check_ready_waiters(self) -> None:
        remaining = []
        current_roles = set(self.clients.keys())
        for needed, fut in self._ready_waiters:
            if needed.issubset(current_roles):
                if not fut.done():
                    fut.set_result(True)
            else:
                remaining.append((needed, fut))
        self._ready_waiters = remaining

    # ── lifecycle ────────────────────────────────────────────────────
    async def start(self) -> None:
        # Clean up stale socket
        sock_path = Path(SOCKET_PATH)
        if sock_path.exists():
            sock_path.unlink()

        self.server = await asyncio.start_unix_server(
            self.handle_client, path=SOCKET_PATH
        )
        os.chmod(SOCKET_PATH, 0o600)
        log(f"listening on {SOCKET_PATH}", GREEN)

        # Write PID file for orchestrator cleanup
        pid_path = SOCKET_PATH + ".pid"
        Path(pid_path).write_text(str(os.getpid()))

    def _stop(self) -> None:
        if self.server:
            self.server.close()
        for writers in self.clients.values():
            for w in writers:
                w.close()
        asyncio.get_event_loop().stop()

    async def serve_forever(self) -> None:
        await self.start()
        assert self.server is not None
        async with self.server:
            await self.server.serve_forever()


async def main() -> None:
    broker = Broker()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, broker._stop)

    print(f"""
{BOLD}{MAGENTA}╔══════════════════════════════════════════╗
║       cmux orchestrate — IPC Broker      ║
╚══════════════════════════════════════════╝{RESET}
""", flush=True)

    try:
        await broker.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        sock = Path(SOCKET_PATH)
        if sock.exists():
            sock.unlink()
        pid_path = Path(SOCKET_PATH + ".pid")
        if pid_path.exists():
            pid_path.unlink()
        log("broker stopped", RED)


if __name__ == "__main__":
    asyncio.run(main())
