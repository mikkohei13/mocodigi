from __future__ import annotations

import signal
from datetime import datetime
from typing import Any


RUN_STATUS_RUNNING = "running"
RUN_STATUS_FINISHED = "finished"
RUN_STATUS_PARTIAL = "partial"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_TERMINATED = "terminated"


class RunTerminatedError(Exception):
    pass


def now_iso() -> str:
    return datetime.now().isoformat()


def log(message: str) -> None:
    print(f"[{now_iso()}] {message}")


def install_termination_handlers() -> None:
    def handle_termination_signal(signum: int, frame: Any) -> None:
        del frame
        signal_name = signal.Signals(signum).name
        raise RunTerminatedError(f"Received termination signal: {signal_name}")

    signal.signal(signal.SIGINT, handle_termination_signal)
    signal.signal(signal.SIGTERM, handle_termination_signal)
