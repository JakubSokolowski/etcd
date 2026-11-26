# Savescum Reproduction of etcd Issue #15271

This branch is reset to commit `8cb8f4bf1cc6a737b642695687e41fa1e232962a`, the parent commit immediately before PR #15492 (`6d01ab641f0def3c2f8932eec150f83a388d6f10`) fixed [etcd-io/etcd#15271](https://github.com/etcd-io/etcd/issues/15271).

## Upstream bug

In `server/storage/mvcc/watchable_store.go`, `watchableStore.notify` only updated `w.minRev = rev + 1` on the slow-watcher victim branch, never when `w.send(eb)` succeeded for a healthy synced watcher.

Under normal operation `w.minRev` is never consulted again for a synced watcher. Under a network partition, however, if a follower falls behind the leader by more than `snapshot-count` + `experimental-snapshot-catchup-entries`, reconnecting triggers a leader Raft snapshot transfer and `watchableStore.Restore(b)`. `Restore` moves all synced watchers back into `unsynced`, which resyncs them from their stale `w.minRev` — causing live watch streams to travel back in time and re-deliver already-observed revisions.

## Instrumentation

- Pure read-only Savescum telemetry hooks in `server/etcdserver/server.go` (`triggerSnapshot`, `applySnapshot`) and `server/storage/mvcc/watchable_store.go` (`Restore`) guide snapshot branching without altering etcd behavior.
- `server/cmd/savescum-driver/main.go` runs two concurrent `client/v3` writers, three per-member watch verifiers (`etcd_watch_revision_monotonic`, `etcd_watch_start_rev_bound`), and a linearizable read verifier (`etcd_linearizable_read_monotonic`).
