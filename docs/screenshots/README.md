# Screenshots and the tour animation

Everything in this directory is generated, and every word in it came off a real
WebSocket. Nothing is mocked up or hand-edited: the pipeline starts the gateway
in-process, drives it over a genuine socket with the same script the browser's
tour runs, and records the frames the server actually sent.

```bash
go run ./tools/transcript -out docs/screenshots/transcript.jsonl
python tools/render.py
```

The first command records the session. The second decides typography and
timing and draws it — the only things it invents are the font and the pauses.

| File | Shows |
|------|-------|
| `tour.gif` | The failure arc: kill the leader, watch it re-elect, kill another, watch the survivor refuse. ~20s |
| **Normal operation** | |
| `startup-election.png` | A fresh cluster: three shards each winning a leader through a real prepare round |
| `first-write-paxos.png` | One write as one round — accept, chosen, commit |
| `intra-shard-single-round.png` | A transfer whose keys share a shard: one log entry, no 2PC |
| `cross-shard-2pc.png` | A transfer spanning two shards: parallel prepares, both votes, commit |
| `datastore-converged.png` | Every replica's committed log, side by side |
| **Under failure** | |
| `leader-reelection.png` | A killed leader replaced at a higher ballot |
| `no-quorum-refuses.png` | One replica of three left, refusing to commit alone |
| `partitioned-leaderless.png` | All three replicas alive, no majority group, no leader |
| **Recovery** | |
| `replica-rejoins.png` | A revived replica restoring quorum, and the new leader finishing slots the dead one left prepared |
| `heal-recovers.png` | The split repaired and the shard electing again |
| `cluster-status.png` | The end state: every shard led, balances intact after the whole run |

`transcript.jsonl` is the recording itself, kept so the images can be
regenerated without re-running the cluster, and so the exact frames behind any
image can be checked.

## Changing what is captured

The script lives in [`tools/transcript/main.go`](../../tools/transcript/main.go)
and mirrors the `TOUR` array in
[`frontend/index.html`](../../frontend/index.html). Change both, or the
animation stops matching what a visitor sees.

Which moments become stills, and which slice of the session becomes the GIF,
are the `STILLS`, `GIF_FROM` and `GIF_TO` settings at the top of
[`tools/render.py`](../../tools/render.py).

## The v1 lab has no captures

The original lab in `distributed-banking/` is driven by an interactive stdin
menu, which this pipeline cannot record. It was never captured and, now that the
control plane is the thing worth looking at, probably will not be.
