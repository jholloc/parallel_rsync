# parallel_rsync

Copies the subdirectories of a source directory to a destination using multiple parallel `rsync` processes.

## Usage

```
./parallel_rsync.py <source> <destination> <num_processes>
```

| Argument | Description |
|---|---|
| `source` | Directory whose immediate subdirectories will be copied |
| `destination` | Destination path — local or remote (`user@host:/path`) |
| `num_processes` | Number of parallel rsync workers |

### Example

```
./parallel_rsync.py /data/media user@nas:/backup/media 4
```

This scans `/data/media` for subdirectories, splits them across 4 workers using round-robin assignment, and runs 4 rsync processes simultaneously.

## How it works

1. **Sequential startup** — workers are launched one at a time. Each worker waits for the previous one to finish authenticating before starting, so SSH password prompts are never presented simultaneously.
2. **Buffered output** — while workers are starting up, any output they produce is held in memory. Once all workers have authenticated, the buffered output is flushed and all workers stream their output to the console concurrently.
3. **Colour-coded labels** — each worker's output is prefixed with a `[worker N/N]` label in a distinct colour so streams are easy to tell apart.

## Requirements

- Python 3.10+
- `rsync` available on `$PATH`
- Unix/macOS (uses `pty` from the standard library)
