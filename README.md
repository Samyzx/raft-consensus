# Raft key-value store

Five independent Python processes replicate a string-to-string key-value store over gRPC. The `standard` and `lease` modes use the same election, log replication, persistence, and client code. They differ in how the leader proves it can serve a linearizable read.

| Mode | `GET` | After losing a quorum |
| --- | --- | --- |
| `standard` | Starts a fresh `AppendEntries` round and waits for a majority. | Reads and writes cannot complete successfully. |
| `lease` | Reads locally from the applied state while a quorum-confirmed lease is valid. | Reads can succeed until expiry. Writes still require a majority. The leader steps down at expiry. |

Both modes wait for the leader’s current-term `NO-OP` to be committed and applied before serving client operations. A successful `SET` means the log entry was stored by a majority and applied by the leader. A missing key returns an empty string. The client reuses a request ID across retries of each `SET`, so replaying the same operation cannot overwrite a later value.

## Prerequisites

- Python 3.11 or newer with `venv` and `pip`. Verified with Python 3.14 on Windows.
- Git for version control. GitHub CLI and an authenticated GitHub account are needed only for publishing via `gh`.
- Five available loopback TCP ports. `cluster.local.json` uses `50051` through `50055`.
- `grpcio` and `protobuf` are installed from `requirements.txt`. Development dependencies add the protobuf compiler and pytest. The SQLite storage engine comes with Python.

From the repository root in PowerShell, create the environment and install the dependencies.

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
```

On Linux, use `python3 -m venv .venv` and `.venv/bin/python` for the equivalent commands. The generated bindings are checked in. After editing `raftkv/raft.proto`, regenerate them with the environment’s Python.

```powershell
.\.venv\Scripts\python.exe -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raftkv/raft.proto
```

## Run five nodes locally

Open five terminals in the repository root and run the following command in each, using a different `--id` from `0` through `4`.

```powershell
.\.venv\Scripts\python.exe -m raftkv node --id 0 --mode lease --data-root runs/lease
```

From a sixth terminal, run the client.

```powershell
.\.venv\Scripts\python.exe -m raftkv set x "hello Raft"
.\.venv\Scripts\python.exe -m raftkv get x
```

For standard Raft, stop the first cluster and launch all five nodes with `--mode standard --data-root runs/standard`. Keep the same mode, membership, and timing profile on every node. Restart a node with its original ID and data root to recover its durable log. A timed-out `SET` can already have been committed.

| Profile | Heartbeat | Election timeout | Lease duration |
| --- | --- | --- | --- |
| `default` | 500 ms | Randomized from 3 to 5 s | 2 s |
| `assignment` | 1 s | Randomized from 5 to 10 s | 4 s |

The default uses YugabyteDB’s documented heartbeat and lease defaults. The lower end of our election window comes from six missed 500 ms heartbeats, and we randomize over a wider window for this project. Use `--profile assignment` on all nodes for the slower classroom demo. [YugabyteDB tablet-server flags](https://docs.yugabyte.com/stable/reference/configuration/all-flags-yb-tserver/)

## Verify failover

Look for `became the leader` in each node’s `dump.txt`. The authoritative state is in each node’s SQLite database. `logs.txt` and `metadata.txt` are inspectable exports of the log and metadata, respectively. The dump records elections, replication, commits, and lease changes.

1. Stop two followers, write a value, then restart them. Their logs and applied values should catch up.
2. Stop the leader, then issue a `GET` and a `SET`. The client should discover the successor. Restart the former leader and check that it catches up as a follower.
3. In lease mode, stop three followers together. A prompt `GET` can succeed until the last lease expires. Watch for `lease renewal failed` in the leader’s dump. A `GET` or `SET` with `--timeout 2` then exits with an error. In standard mode the same `GET` requires a quorum immediately.

The automated tests exercise these behaviors with shorter timings. They also verify conflict repair, vote persistence across restart, and the lease handoff using actual loopback gRPC calls.

## How the lease works

Each positive `AppendEntries` lease offer carries a duration. The leader records the local send time plus that duration for each peer. Its lease expiry is the latest deadline acknowledged by at least a majority, counting itself. Late acknowledgments cannot revive an expired leader. Followers record a conservative wait based on the local receipt time and return their longest known remaining wait in granted votes. A newly elected leader waits out those reports and its own record before it can offer a new lease. On restart, a node conservatively reports a full lease window to cover any prior grant it might have forgotten. [YugabyteDB’s Raft lease design](https://docs.yugabyte.com/stable/architecture/transactions/single-row-transactions/#leader-leases)

This design sends durations and uses monotonic clocks. Its safety assumes monotonic clocks advance continuously, including through VM pauses, and each node’s rate stays within 500 ppm of real time. Standard mode does not rely on a clock-rate bound for read safety. [Raft paper, §8](https://raft.github.io/raft.pdf)

## Run on five VMs

Copy `cluster.local.json` and replace each address with the corresponding VM’s reachable private address. Install Python and the runtime dependencies on each VM. Give each node the same cluster file, mode, and profile. Restrict the gRPC port to cluster peers and the client on the private network. The demo uses insecure gRPC and assumes trusted peers.

```bash
.venv/bin/python -m raftkv --cluster cluster.vm.json node --id 0 --mode lease --listen 0.0.0.0:50051 --data-root runs/lease
.venv/bin/python -m raftkv --cluster cluster.vm.json get x
```
