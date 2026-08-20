# Screenshot capture checklist

Ten captures, referenced by the table in the root README. Save each with the
exact filename below (PNG) so the README picks it up. A missing file shows as a
broken image, which is deliberate — it makes gaps obvious.

Start from a clean slate each time:

```bash
cd distributed-banking && rm -f db_S*.db && go run .
```

Answer `3` clusters and `3` servers per cluster unless a step says otherwise.

| # | Filename | What to capture |
|---|----------|-----------------|
| 1 | `01-cluster-startup.png` | The startup output: cluster map, server-to-cluster mapping, "Starting Distributed Banking System..." |
| 2 | `02-intra-shard-commit.png` | Set 1 processing `(100, 501, 8)` — both accounts in C1. Then menu option **2** showing account 100 at 2 and 501 at 18 on all three C1 replicas. |
| 3 | `03-cross-shard-commit.png` | Set 6, transfer `(1001, 2999, 6)` — C2 to C3. Then option **2** on both accounts showing the credit and debit landed. |
| 4 | `04-cross-shard-abort.png` | Set 6, transfer `(299, 1999, 15)` — the amount exceeds the balance of 10, so the source shard votes no and the whole thing aborts. Show both balances unchanged. |
| 5 | `05-minority-down-commits.png` | Option **5**, crash `S3`. Option **7** showing `C1: S1 UP, S2 UP, S3 DOWN [2/3 alive, needs 2: quorum]`. Run the next set — it still commits. |
| 6 | `06-majority-down-refuses.png` | Crash `S2` as well. Option **7** showing `[1/3 alive, needs 2: NO QUORUM]`. Run a transfer touching C1 — the leader refuses and balances do not move. |
| 7 | `07-recovery-catch-up.png` | Option **6** to restore `S2` and `S3`. Run a transfer, then option **3** showing the recovered replicas now hold the same entries. |
| 8 | `08-datastore-converged.png` | Option **3** after several sets: every replica in a cluster printing an identical chain of `<ballot,server>,(src,dst,amt)` entries. This is the money shot. |
| 9 | `09-performance.png` | Option **4** after a full run — transaction count, throughput, latency. |
| 10 | `10-tests-green.png` | A terminal running `go test -race ./... -count=1` with everything passing. Use `-v` if the individual test names read better. |

Tips:

- Keep the terminal wide enough that datastore lines do not wrap — the chain of
  entries is much easier to read unwrapped.
- For #6, the point is that balances are **unchanged**. Capture the balance
  print both before and after if it fits in one frame.
- Crop to the terminal. No desktop, no taskbar.
