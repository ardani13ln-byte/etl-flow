# etlflow

Declarative data pipelines that fail loudly and resume cheaply. **Zero
dependencies**, Python 3.10+.

```bash
git clone https://github.com/ardani13ln-byte/etl-flow
cd etl-flow
python3 -m etlflow demo              # watch a full pipeline run, no setup
python3 -m unittest discover -s tests
```

Most ETL tooling is either a 300-line script nobody dares touch or a cluster
you need a team to operate. This sits in between: you describe the steps, and
the runner gives you the three things that actually matter in production —
**resumability**, **observability**, and a **dry-run** you can trust.

## The four bugs this prevents

| What goes wrong in production | What etlflow does |
|---|---|
| A pipeline fails at step 4 of 6 and the retry re-runs everything, doubling rows or API calls | Checkpoints mark completed steps; a re-run **skips** them |
| Resume replays stale data because the supplier corrected their export in between | `load_csv` fingerprints the input file (size + mtime); a changed file **re-runs** instead of replaying yesterday's rows |
| Resume skips the loader and the writer happily writes an **empty file** | The skipped step's context delta is **replayed**, so downstream steps see the same rows |
| A cached `write` step reports success **without producing the file** | The write step is never checkpointed — the output file is the deliverable |
| `transform` silently runs before `validate` because steps were sorted alphabetically | Declaration order is preserved; dependencies come from `after`, not from names |

## Quick start

```bash
python3 -m etlflow demo --out orders.csv
```

```
run finished in 0.00s
  [OK  ] load                     0.00s  0 in / 5 out
  [OK  ] validate                 0.00s  5 in / 3 out
  [OK  ] transform                0.00s  3 in / 3 out
  [OK  ] write                    0.00s  3 in / 3 out

rejected 2 row(s):
  line 4: missing name
  line 6: missing qty

wrote orders.csv:
id,name,total
1,Widget,21.00
2,Gadget,12.75
4,Doohickey,99.99
```

Check a new pipeline before it touches anything real:

```bash
python3 -m etlflow demo --out orders.csv --dry-run
```

```
run finished in 0.00s  (dry run, nothing was written)
  [DRY ] load                     0.00s  planned, not executed
  ...
```

Dry-run steps are reported as `DRY`, never as `OK` — claiming work happened
that never did is how a "validated" pipeline surprises you.

## Plan files

```toml
name = "supplier ingest"

[steps.load]
kind = "load_csv"
source = "data/in.csv"

[steps.validate]
kind = "validate"
required = ["id", "name", "qty", "price"]
strict = false
report = "rejected.json"
after = "load"

[steps.transform]
kind = "map"
after = "validate"
keep_fields = ["id", "name"]
drop_if_empty = ["name"]
computed = { total = "round(qty * price, 2)" }

[steps.write]
kind = "write_csv"
target = "out/report.csv"
columns = ["id", "name", "total"]
after = "transform"
retries = 2
```

```bash
python3 -m etlflow run -f plan.toml --checkpoint cp.json
python3 -m etlflow run -f plan.toml --checkpoint cp.json --json   # machine-readable run record
```

Step kinds: `load_csv`, `filter` (`not_empty` / `equals` / `greater_than` /
`contains`), `map` (keep/drop/computed expressions), `validate`, `write_csv`.
`computed` expressions allow literals, field names, `+ - * / % **` and
`round/abs/min/max/float/int` — anything else (`open(...)`, attribute access,
subscripts, lambdas) is refused with a `PlanError`. A plan file is not a place
to run arbitrary code.

## Library

```python
from etlflow import Runner, story_load_csv, story_validate, story_map, story_write_csv

record = Runner({
    "load":     story_load_csv("load", "in.csv"),
    "validate": story_validate("validate", ["id", "name"], strict=False),
    "totals":   story_map("totals", lambda r: {**r, "total": float(r["qty"]) * float(r["price"])}),
    "write":    story_write_csv("write", "out.csv"),
}, checkpoint="cp.json").run()

print(record.summary())
print(record.to_json())   # the artifact you debug with at 2am
```

Steps are plain callables receiving a shared context dict. Custom steps slot in
beside the built-ins — no subclassing, no framework to learn.

Rejected rows carry their **1-based line number in the source CSV**, because the
person fixing them is reading the file, not your log.

## How checkpoints work (and their limits, stated honestly)

- Each step's checkpoint key combines the **step name**, the **context shape**,
  and the step's **input fingerprint** (for `load_csv`: file size + mtime).
- On a hit, the step is skipped and its stored **context delta** is replayed,
  so downstream steps see identical rows.
- The checkpoint file is written **atomically** (temp file + rename) and a
  corrupt checkpoint degrades to "run everything" instead of crashing.
- `write_csv` is never checkpointed: re-running a write is cheap and
  idempotent, and skipping it would report success without the output file.
- The fingerprint is stat-based, not a content hash: O(1), and good enough to
  notice a replaced export. If you need byte-level guarantees, hash the file in
  your own `input_fingerprint`.

## Failure semantics

- A failed step records `ExceptionType: message` and every step that never ran
  is listed as `blocked`, naming the failure that stopped it. A run log with
  holes in it is not debuggable.
- `allow_failure=True` lets dependents run anyway — the step's contract is
  "this may fail, carry on".
- Retries use capped exponential backoff (`1s, 2s, 4s…`, max 10s) with an
  injectable `sleep`, so tests never wait.

## Tests

```
116 tests, ~1s, no network, no dependencies.
```

The suite covers the failure modes rather than the happy path: dependency
cycles with the path named, unknown step references, blocked-step reporting,
checkpoint skip + replay, stale-input invalidation, write-never-cached, corrupt
checkpoints, dry-run executing zero step code, plan-parser edge cases
(inline tables, quoted commas, unterminated arrays with line numbers), and
expression sandboxing (`open()`, dunders, subscripts all refused).

## Design decisions worth stating

- **No dependencies, including no TOML parser.** The plan grammar is a
  hand-written subset small enough to read in one sitting, and it runs on 3.10
  where `tomllib` does not exist.
- **Blocked steps are recorded, not omitted.** `stop_on_failure` stops
  execution, not reporting.
- **Dry is a status, not a flag on success.** `DRY` in the summary is honest
  about what happened; `OK` would be a lie.
- **Declaration order is execution order** for independent steps. Alphabetical
  sorting would silently reorder `transform` before `validate` one day.

## License

MIT
