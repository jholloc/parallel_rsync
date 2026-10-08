#!/usr/bin/env -S python3.11 -u
"""
parallel_rsync.py - Copy subdirectories in parallel using multiple rsync processes.

Usage:
    python parallel_rsync.py <source> <destination> <num_processes>

Each worker receives a round-robin share of the subdirectories found in <source>
and runs a single rsync invocation to copy them all to <destination>.
Output from all workers is interleaved to stdout, prefixed with a colour-coded
worker label so you can tell streams apart at a glance.
"""

import argparse
import os
import pty
import select
import subprocess
import sys
import threading
import time
from glob import glob
from pathlib import Path

# rsync lines that only appear after SSH authentication has succeeded.
# Seeing any of these means it's safe to start the next worker.
_AUTH_MARKERS = (
    "sending incremental file list",
    "receiving incremental file list",
    "building file list",
    "Number of files:",
    "Transfer starting:",
)

# Maximum rate at which each worker may print lines to the console.
_OUTPUT_INTERVAL = 10.0  # seconds

# ANSI colour codes for worker labels (cycles if there are more workers than colours)
_COLOURS = [
    "\033[36m",  # cyan
    "\033[33m",  # yellow
    "\033[32m",  # green
    "\033[35m",  # magenta
    "\033[34m",  # blue
    "\033[31m",  # red
]
_RESET = "\033[0m"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy subdirectories to a destination in parallel using multiple rsync processes."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "source", help="Directory whose immediate subdirectories will be copied"
    )
    parser.add_argument("destination", help="Destination directory")
    parser.add_argument(
        "num_processes", type=int, help="Number of parallel rsync workers"
    )
    parser.add_argument(
        "--glob", type=str, default="*", help="Glob pattern to match subdirectories"
    )
    return parser.parse_args()


def get_subdirectories(directory: str, glob_pattern: str) -> list[str]:
    """Return sorted list of immediate subdirectory paths inside *directory*."""
    try:
        entries = [Path(e) for e in glob(os.path.join(directory, glob_pattern))]
    except FileNotFoundError:
        sys.exit(f"Error: source directory '{directory}' does not exist.")
    except PermissionError:
        sys.exit(f"Error: permission denied reading '{directory}'.")
    return sorted(str(e) for e in entries if e.is_dir())


def divide_into_groups(items: list, n: int) -> list[list]:
    """Round-robin assignment of *items* into *n* non-empty groups."""
    groups: list[list] = [[] for _ in range(n)]
    for i, item in enumerate(items):
        groups[i % n].append(item)
    return [g for g in groups if g]


def _flush_held(held: list[str], colour: str, label: str, lock: threading.Lock) -> None:
    """Write all buffered lines to stdout and clear the buffer."""
    if not held:
        return
    with lock:
        for line in held:
            sys.stdout.write(f"{colour}{label}{_RESET} {line}\n")
        sys.stdout.flush()
    held.clear()


def run_worker(
    group: list[str],
    destination: str,
    label: str,
    colour: str,
    lock: threading.Lock,
    exit_codes: list[int | None],
    idx: int,
    auth_event: threading.Event,
    output_allowed: threading.Event,
) -> None:
    """Run a single rsync process for *group* and stream its output.

    Lines are held in a buffer until *output_allowed* is set, so that rsync
    output from already-running workers never interleaves with SSH password
    prompts for workers that are still starting up.
    """
    dest = destination.rstrip("/") + "/"
    cmd = ["rsync", "-a", "--info=progress2", "--no-inc-recursive"] + group + [dest]

    with lock:
        sys.stdout.write(f"{colour}{label}{_RESET} {' '.join(cmd)}\n")
        sys.stdout.flush()

    # Attach rsync's stdout/stderr to the slave end of a PTY so that rsync's
    # C runtime sees isatty() == True and uses line buffering rather than the
    # block buffering it applies when writing to a plain pipe. The master end
    # is read by this thread. SSH password prompts are unaffected because ssh
    # opens /dev/tty (the controlling terminal) directly.
    master_fd, slave_fd = pty.openpty()

    proc = subprocess.Popen(
        cmd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
    )
    os.close(slave_fd)  # slave is only needed by the child

    auth_signaled = False
    buf = ""
    held: list[str] = []  # lines buffered until output_allowed is set
    last_printed_at: float = 0.0

    def _raw_emit(line: str) -> None:
        """Send *line* to stdout or the held buffer, bypassing the throttle."""
        if not line:
            return
        if output_allowed.is_set():
            _flush_held(held, colour, label, lock)
            with lock:
                sys.stdout.write(f"{colour}{label}{_RESET} {line}\n")
                sys.stdout.flush()
        else:
            held.append(line)

    def emit(line: str) -> None:
        """Rate-limit output to at most one line per _OUTPUT_INTERVAL seconds."""
        nonlocal last_printed_at
        now = time.monotonic()
        if now - last_printed_at >= _OUTPUT_INTERVAL:
            _raw_emit(line)
            last_printed_at = now

    try:
        while True:
            ready, _, _ = select.select([master_fd], [], [], 0.1)
            if not ready:
                # No data available; exit if rsync itself has already finished.
                # SSH may still hold the slave fd open during connection teardown,
                # which would cause os.read to block indefinitely without this check.
                if proc.poll() is not None:
                    break
                continue
            try:
                chunk = os.read(master_fd, 4096).decode("utf-8", errors="replace")
            except OSError:
                # EIO: all holders of the slave fd have closed it
                break
            if not chunk:
                break
            # Normalise line endings so that bare \r (used by rsync's
            # --info=progress2 to overwrite the current line) is treated as a
            # line boundary, the same as \n. Do \r\n first to avoid doubling.
            chunk = chunk.replace("\r\n", "\n").replace("\r", "\n")
            buf += chunk
            while "\n" in buf:
                line, buf = buf.split("\n", 1)

                if not auth_signaled and line:
                    auth_signaled = True
                    auth_event.set()

                emit(line)
    finally:
        os.close(master_fd)

    # Flush any partial line that had no trailing newline
    if buf.strip():
        emit(buf)

    # Always signal so the startup loop is never left waiting
    auth_event.set()

    # Block here until all workers have started, then flush any held lines.
    # The read loop above never blocks, so the PTY buffer never backs up.
    output_allowed.wait()
    _flush_held(held, colour, label, lock)

    proc.wait()
    exit_codes[idx] = proc.returncode

    with lock:
        if proc.returncode == 0:
            status = "finished successfully"
        else:
            status = f"FAILED (exit code {proc.returncode})"
        sys.stdout.write(f"{colour}{label}{_RESET} {status}\n")
        sys.stdout.flush()


def main() -> None:
    args = parse_args()

    if args.num_processes < 1:
        sys.exit("Error: num_processes must be at least 1.")

    subdirs = get_subdirectories(args.source, args.glob)
    if not subdirs:
        sys.exit(f"No subdirectories found in '{args.source}'. Nothing to do.")

    n = min(args.num_processes, len(subdirs))
    if n < args.num_processes:
        noun = "subdirectory" if len(subdirs) == 1 else "subdirectories"
        print(
            f"Note: only {len(subdirs)} {noun} found; "
            f"reducing workers from {args.num_processes} to {n}."
        )

    groups = divide_into_groups(subdirs, n)

    total_noun = "subdirectory" if len(subdirs) == 1 else "subdirectories"
    worker_noun = "process" if n == 1 else "processes"
    print(
        f"Copying {len(subdirs)} {total_noun} "
        f"from '{args.source}' → '{args.destination}' "
        f"across {n} parallel {worker_noun}.\n"
    )

    lock = threading.Lock()
    output_allowed = threading.Event()
    exit_codes: list[int | None] = [None] * n
    threads: list[threading.Thread] = []

    for i, group in enumerate(groups):
        colour = _COLOURS[i % len(_COLOURS)]
        label = f"[worker {i + 1}/{n}]"
        auth_event = threading.Event()

        print(f"{colour}{label}{_RESET} Starting — enter SSH password if prompted.")

        t = threading.Thread(
            target=run_worker,
            args=(
                group,
                args.destination,
                label,
                colour,
                lock,
                exit_codes,
                i,
                auth_event,
                output_allowed,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

        # Wait for this worker to authenticate before moving on to the next,
        # so password prompts are always presented one at a time.
        auth_event.wait()

        if i < n - 1:
            print()  # visual separator before the next "Starting" banner

    # Every worker has authenticated; release buffered output for all of them.
    print("\nAll workers running — resuming output.\n")
    output_allowed.set()

    for t in threads:
        t.join()

    print("\n" + "=" * 60)
    failed = [i + 1 for i, rc in enumerate(exit_codes) if rc != 0]
    if failed:
        print(f"Finished with errors. Failed worker(s): {failed}")
        sys.exit(1)
    else:
        print("All transfers completed successfully.")


if __name__ == "__main__":
    main()
