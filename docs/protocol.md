# WebSocket control-plane protocol

The contract between the Go backend (`cmd/cluster`) and the browser frontend
(`frontend/`). The frontend is written against this document; the backend is
tested against it. Changing a field name here is a breaking change on both
sides.

- **Endpoint:** `GET /ws`
- **Health:** `GET /healthz` → `200 OK`, body `ok`
- **Subprotocol:** none
- **Origin policy:** any origin is accepted. The frontend is served from a
  different host than the backend, and this demo holds no user data.

Local development uses `ws://localhost:8080/ws`. Anything served over HTTPS
must use `wss://` — a page on `https://` cannot open a `ws://` socket, browsers
block it as mixed content.

---

## Session model

**One cluster per connection.** A fresh cluster is built when the socket opens
and destroyed when it closes. Nothing is shared between visitors and nothing
survives a reload: kill a node, partition the network, leave it broken, and the
next visitor still gets a clean 3x3 cluster.

State is in memory only. There is no database and no persistence.

### The connect banner

On connect the server runs the startup elections and narrates them, then sends
**one result frame** — exactly as a command would. Connecting is, in effect, an
implicit command.

A client should therefore keep its prompt disabled until that first result
arrives. A cluster that has not elected yet cannot serve anything, and inviting
commands before then just produces avoidable aborts.

```json
{"type":"log","level":"info","tag":"[gw]","msg":"fresh cluster: 3 shards x 3 nodes, quorum 2"}
{"type":"log","level":"paxos","tag":"[s0]","msg":"PREPARE <1,s0n0> from s0n0"}
{"type":"log","level":"leader","tag":"[s0]","msg":"s0n0 leads s0 at <1,s0n0>"}
{"type":"result","msg":"OK cluster up, 3/3 shards led"}
```

---

## Client to server

One JSON object per command. The raw line as typed:

```json
{"cmd": "transfer tushar ram 25"}
```

| Field | Type | Notes |
|-------|------|-------|
| `cmd` | string | The raw command line. Leading/trailing whitespace is trimmed by the server. |

Unknown fields are ignored. A frame that is not valid JSON, or that carries an
empty `cmd`, produces an `error` result.

Commands are processed **one at a time per connection**, in the order received.

## Server to client

Every command produces **zero or more `log` frames followed by exactly one
`result` frame.** The result frame is the terminator: a frontend can rely on it
to re-enable the prompt.

### Log frame

```json
{"type":"log","level":"paxos","tag":"[s0]","msg":"PREPARE ballot=<2,s0n1>"}
```

| Field | Type | Notes |
|-------|------|-------|
| `type` | string | Always `"log"`. |
| `level` | string | One of the seven values below. The frontend color-codes on this. |
| `tag` | string | Short origin marker, e.g. `[s0]`, `[2PC]`, `[net]`. May be empty. |
| `msg` | string | Plain text. No ANSI codes — the frontend styles it. |

**`level` is a closed set.** These seven and no others:

| Level | Meaning |
|-------|---------|
| `paxos` | Consensus phases: PREPARE, PROMISE, ACCEPT, COMMIT |
| `twopc` | Two-phase commit: BEGIN, votes, COMMIT/ABORT |
| `leader` | Elections, step-downs, heartbeat loss |
| `net` | Transport events: message dropped, node killed/revived, partition |
| `error` | Something failed |
| `client` | Echo of what the operator asked for |
| `info` | Everything else |

### Result frame

```json
{"type":"result","msg":"OK tushar=75 ram=125"}
```

| Field | Type | Notes |
|-------|------|-------|
| `type` | string | Always `"result"`. |
| `msg` | string | Plain text, one line. Conventionally prefixed `OK `, `ABORT ` or `ERROR `, but the frontend must not parse it — treat it as display text and tint on the prefix at most. |

The three prefixes carry a meaning worth distinguishing in the UI:

| Prefix | Means |
|--------|-------|
| `OK` | The command did what it said |
| `ABORT` | The cluster refused, correctly: no quorum, no leader, insufficient funds, locked key, unknown key. Not a malfunction — the demo exists to produce these. |
| `ERROR` | Bad input, or something genuinely went wrong |

An empty `msg` is valid and means "nothing to report" — sent for a blank line and for `clear`. A client should re-enable its prompt and print nothing.

---

## Commands

Node ids are `s{shard}n{index}`, zero-based: `s0n0`, `s0n1`, `s0n2`, `s1n0`, ...

| Command | Form | Result |
|---------|------|--------|
| `status` | `status` | Per-shard leader, term, alive count; accounts and their shards |
| `datastore` | `datastore` | Every replica's committed log, one line each, so divergence is visible |
| `put` | `put <key> <val>` | Paxos write on the key's shard. `<val>` is a signed integer. |
| `get` | `get <key>` | Linearizable read from that shard's leader |
| `transfer` | `transfer <from> <to> <amt>` | 2PC across shards, or a single Paxos round if same-shard. `<amt>` must be positive. |
| `kill` | `kill <node>` | Mark a node dead. Re-election follows if it was a leader. |
| `revive` | `revive <node>` | Bring it back and let it rejoin. |
| `partition` | `partition <ids...> \| <ids...>` | Split the network into pipe-separated groups. Two or more groups. |
| `heal` | `heal` | Remove all partitions. |
| `help` | `help` | Command summary |
| `clear` | `clear` | Client-side only. The server accepts and ignores it. |

Anything else is an `error` result: `unknown command: foo`.

### Failure results are first-class

A shard with no quorum cannot accept writes. That is the point of the demo, not
an edge case, and it returns a normal `result` frame explaining why:

```
ABORT shard s0 has no leader (1/3 alive, needs 2)
```

---

## Limits

Per connection, to keep a free-tier instance from becoming someone's compute toy:

| Limit | Value |
|-------|-------|
| Commands per second | 10 (burst 20) |
| Distinct keys | 256 |
| Max concurrent sessions | 32 |
| Max frame size (inbound) | 4 KiB |
| Idle timeout | 10 minutes |

Exceeding a limit produces an `error` result; exceeding the session cap refuses
the handshake with `503`.

---

## Display pacing

The server can insert a small delay between log frames so the stream reads like
a live cluster instead of arriving as one instant dump. This is **presentation
only** — it lives in the gateway, is controlled by `-pace` (default `120ms`,
`0` disables), and no delay exists anywhere inside the consensus code.
