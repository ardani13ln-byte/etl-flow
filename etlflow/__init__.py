"""etlflow - declarative data pipelines that fail loudly and resume cheaply.

Most ETL tooling is either a 300-line script nobody dares touch or a cluster
you need a PhD to operate. This sits in between: you describe the steps in
YAML-subset TOML, and the runner gives you the three things that actually
matter in production:

* **resumable** - a failed step's checkpoint is kept, so a re-run skips what
  already succeeded
* **observable** - every step records duration, rows in/out, and the reason for
  failure, in a run log you can read afterwards
* **safe by default** - dry-run mode validates the whole plan without writing
  anything, so you can check a new pipeline before it touches real data

Zero dependencies. Python 3.10+.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__version__ = "1.0.0"

__all__ = [
    "FlowError",
    "PlanError",
    "Runner",
    "Step",
    "StepResult",
    "RunRecord",
    "load_plan",
    "parse_plan",
    "STORY",
]


class FlowError(Exception):
    """A step failed while running."""


class PlanError(Exception):
    """The plan itself is invalid: unknown step, cycle, bad reference."""


@dataclass
class Step:
    """One unit of work.

    ``run`` receives the shared context dict and returns either None or a dict
    merged into it, so steps compose without globals.
    """

    name: str
    run: Callable[[dict[str, Any]], Any]
    retries: int = 0
    depends_on: tuple[str, ...] = ()
    allow_failure: bool = False
    # Optional extra material for the checkpoint key, evaluated before the step
    # runs. Without it a changed input file would replay yesterday's rows.
    input_fingerprint: Callable[[], str] | None = None
    # Whether a completed step may be skipped on a later run. False for the
    # step whose effect *is* the deliverable: caching it would produce a run
    # that reports success without writing the file the user asked for.
    checkpointable: bool = True


@dataclass
class StepResult:
    name: str
    status: str  # ok | dry | failed | skipped | blocked
    seconds: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    error: str | None = None
    checkpoint: str | None = None

    @property
    def ok(self) -> bool:
        # "dry" counts as ok so a dry run walks the whole plan instead of
        # blocking every step after the first.
        return self.status in ("ok", "skipped", "dry")


@dataclass
class RunRecord:
    """Everything that happened in one run. This is the artifact you debug with."""

    started: float
    finished: float = 0.0
    steps: list[StepResult] = field(default_factory=list)
    dry_run: bool = False

    @property
    def seconds(self) -> float:
        return max(0.0, self.finished - self.started)

    @property
    def failed(self) -> list[StepResult]:
        return [s for s in self.steps if s.status == "failed"]

    def summary(self) -> str:
        lines = [f"run finished in {self.seconds:.2f}s"
                 + ("  (dry run, nothing was written)" if self.dry_run else "")]
        for step in self.steps:
            marker = {"ok": "OK  ", "dry": "DRY ", "skipped": "SKIP", "failed": "FAIL", "blocked": "BLOCK"}[step.status]
            detail = f"{step.seconds:.2f}s"
            if step.rows_in or step.rows_out:
                detail += f"  {step.rows_in} in / {step.rows_out} out"
            if step.error:
                detail += f"  {step.error}"
            lines.append(f"  [{marker}] {step.name:<24} {detail}")
        return "\n".join(lines)

    def to_json(self) -> str:
        return json.dumps(
            {
                "started": self.started,
                "finished": self.finished,
                "dry_run": self.dry_run,
                "steps": [s.__dict__ for s in self.steps],
            },
            indent=2,
        )


# --------------------------------------------------------------------------
# checkpointing
# --------------------------------------------------------------------------


def _fingerprint(*parts: Any) -> str:
    material = "|".join(json.dumps(p, sort_keys=True, default=str) for p in parts)
    return hashlib.sha256(material.encode()).hexdigest()[:16]


class CheckpointStore:
    """Records which step-input combinations already succeeded, and their output.

    The point is not cleverness: if step ``load`` produced byte-identical
    output for the same input fingerprint, re-running it is pure cost.

    Storing the step's **context delta** is what makes skipping safe. A skipped
    step still has to leave its output behind, otherwise downstream steps see
    an empty context and happily write an empty file. Resuming a pipeline must
    not destroy the data the previous attempt loaded.
    """

    def __init__(self, path: str | os.PathLike[str] | None) -> None:
        self.path = Path(path) if path else None
        self._data: dict[str, dict[str, Any]] = {}
        if self.path and self.path.exists():
            try:
                loaded = json.loads(self.path.read_text())
                if isinstance(loaded, dict):
                    self._data = {
                        k: v for k, v in loaded.items() if isinstance(v, dict)
                    }
            except (json.JSONDecodeError, OSError):
                self._data = {}

    def done(self, key: str) -> bool:
        return key in self._data

    def delta(self, key: str) -> dict[str, Any]:
        return dict(self._data.get(key, {}).get("context", {}))

    def mark(self, key: str, delta: Mapping[str, Any] | None = None) -> None:
        self._data[key] = {"context": _jsonable(dict(delta or {}))}
        self.flush()

    def flush(self) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, default=str))
        os.replace(tmp, self.path)  # atomic: never leave a half-written checkpoint


def _jsonable(value: Any) -> Any:
    """Make a value survive a JSON round trip, degrading rather than raising."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


# --------------------------------------------------------------------------
# planning
# --------------------------------------------------------------------------


def _topo_order(steps: Mapping[str, Step]) -> list[str]:
    """Order steps by dependency, raising PlanError on a cycle or a bad ref."""
    for step in steps.values():
        for dep in step.depends_on:
            if dep not in steps:
                raise PlanError(
                    f"step '{step.name}' depends on unknown step '{dep}'"
                )
    ordered: list[str] = []
    temp: set[str] = set()
    done: set[str] = set()

    def visit(name: str, trail: tuple[str, ...]) -> None:
        if name in done:
            return
        if name in temp:
            cycle = " -> ".join((*trail, name))
            raise PlanError(f"dependency cycle: {cycle}")
        temp.add(name)
        for dep in steps[name].depends_on:
            visit(dep, (*trail, name))
        temp.discard(name)
        done.add(name)
        ordered.append(name)

    # Declaration order is preserved for steps with no dependencies. Sorting
    # alphabetically here would silently reorder a pipeline whose steps happen
    # to be named out of sequence, which is how "transform ran before validate"
    # happens.
    for name in steps:
        visit(name, ())
    return ordered


def _checkpoint_key(step: Step, ctx: Mapping[str, Any]) -> str:
    """Identity of "this step, with this context and inputs, already succeeded".

    Derived from the step name plus the context's shape, plus whatever the
    step's ``input_fingerprint`` reports about the files it reads. Without that
    last part a corrected source file would replay yesterday's rows, which is
    worse than re-running the step.
    """
    extra = ""
    if step.input_fingerprint is not None:
        try:
            extra = step.input_fingerprint()
        except OSError:
            extra = "unreadable"
    return _fingerprint(step.name, sorted(str(k) for k in ctx), extra)


def file_fingerprint(path: str | os.PathLike[str]) -> Callable[[], str]:
    """Checkpoint fingerprint from a file's size and mtime.

    Deliberately not a content hash: stat is O(1) and good enough to notice
    that a supplier replaced or corrected their export.
    """
    def fingerprint() -> str:
        stat = os.stat(path)
        return f"{stat.st_size}:{int(stat.st_mtime)}"
    return fingerprint


class Runner:
    """Executes a plan, with retries, checkpoints and an auditable record."""

    def __init__(
        self,
        steps: Mapping[str, Step],
        *,
        checkpoint: str | os.PathLike[str] | None = None,
        dry_run: bool = False,
        stop_on_failure: bool = True,
        sleep: Callable[[float], None] = time.sleep,
        on_step: Callable[[StepResult], None] | None = None,
    ) -> None:
        self.steps = dict(steps)
        self.store = CheckpointStore(checkpoint)
        self.dry_run = dry_run
        self.stop_on_failure = stop_on_failure
        self._sleep = sleep
        self.on_step = on_step

    def run(self, context: dict[str, Any] | None = None) -> RunRecord:
        # Ordered here rather than in __init__ so a bad plan is reported when
        # the run starts, not when the object is built.
        order = _topo_order(self.steps)
        ctx = dict(context or {})
        record = RunRecord(started=time.time(), dry_run=self.dry_run)
        completed: set[str] = set()

        for name in order:
            step = self.steps[name]
            unmet = [d for d in step.depends_on if d not in completed]

            if unmet:
                record.steps.append(
                    StepResult(name, "blocked", error=f"dependency not completed: {', '.join(unmet)}")
                )
                self._emit(record.steps[-1])
                if self.stop_on_failure:
                    break
                continue

            key = _checkpoint_key(step, ctx)
            cacheable = step.checkpointable and not self.dry_run
            if cacheable and self.store.done(key):
                # Replay what the step produced last time. Skipping the work
                # without restoring the output would hand downstream steps an
                # empty context and silently write empty results.
                ctx.update(self.store.delta(key))
                result = StepResult(name, "skipped", seconds=0.0, error="checkpoint hit")
                record.steps.append(result)
                self._emit(result)
                completed.add(name)
                continue

            result = self._run_step(step, ctx, key)
            record.steps.append(result)
            self._emit(result)
            if result.ok:
                completed.add(name)
            elif self.stop_on_failure and not step.allow_failure:
                # Do not abandon the record half-finished: the steps that did
                # not run are listed as blocked, naming the failure that
                # stopped them. A run log with holes in it is not debuggable.
                self._block_remaining(record, order, set(completed),
                                      reason=f"pipeline stopped: '{name}' failed")
                break
            else:
                # A tolerated failure still lets dependents run: the step's
                # contract is "this may fail, carry on".
                completed.add(name)

        record.finished = time.time()
        return record

    def _block_remaining(
        self,
        record: RunRecord,
        order: list[str],
        completed: set[str],
        reason: str,
    ) -> None:
        """Record every not-yet-run step as blocked, instead of dropping it."""
        seen = {s.name for s in record.steps}
        for name in order:
            if name in seen or name in completed:
                continue
            result = StepResult(name, "blocked", error=reason)
            record.steps.append(result)
            self._emit(result)

    def _run_step(self, step: Step, ctx: dict[str, Any], key: str) -> StepResult:
        started = time.time()
        if self.dry_run:
            # A dry run validates the plan, it does not execute it. Reporting
            # these steps as "ok" would claim work happened that never did, so
            # they get their own status. Nothing is read, written, marked, or
            # retried here.
            return StepResult(step.name, "dry", time.time() - started,
                              error="planned, not executed")
        attempt = 0
        last_error: str | None = None
        snapshot = {k: v for k, v in ctx.items() if k != "_rowcount"}

        while attempt <= step.retries:
            attempt += 1
            before = len(ctx.get("_rowcount", []))
            try:
                produced = step.run(ctx)
            except Exception as exc:  # noqa: BLE001 - failures are data here
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt <= step.retries:
                    self._sleep(min(2.0 ** (attempt - 1), 10.0))
                    continue
                return StepResult(
                    step.name, "failed", time.time() - started, error=last_error
                )
            rows = ctx.get("_rowcount", [])[before:]
            result = StepResult(
                step.name, "ok", time.time() - started,
                rows_in=sum(r.get("in", 0) for r in rows),
                rows_out=sum(r.get("out", 0) for r in rows),
            )
            if isinstance(produced, dict):
                ctx.update(produced)
            if not self.dry_run and step.checkpointable:
                # Marked under the key computed BEFORE the step ran, which is
                # the same key the next run checks, and storing the keys the
                # step changed so a resumed run can replay them.
                changed = {
                    k: v for k, v in ctx.items()
                    if k != "_rowcount" and (k not in snapshot or snapshot[k] != v)
                }
                self.store.mark(key, changed)
            return result

        return StepResult(step.name, "failed", time.time() - started, error=last_error)

    def _emit(self, result: StepResult) -> None:
        if self.on_step:
            self.on_step(result)


# --------------------------------------------------------------------------
# the built-in step library
# --------------------------------------------------------------------------


def _read_csv(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def story_load_csv(name: str, source: str, *, encoding: str = "utf-8-sig") -> Step:
    """Load a CSV into ``ctx['rows']`` and record the row count.

    The step's checkpoint fingerprint tracks the file's size and mtime, so a
    corrected or replaced export is re-read instead of replayed from the
    previous run.
    """
    def run(ctx: dict[str, Any]) -> dict[str, Any]:
        rows = _read_csv(source)
        ctx["rows"] = rows
        ctx["_rowcount"] = ctx.get("_rowcount", []) + [{"in": 0, "out": len(rows)}]
        return {}
    return Step(name=name, run=run, input_fingerprint=file_fingerprint(source))


def story_filter(name: str, field_name: str, predicate: Callable[[Any], bool]) -> Step:
    """Keep rows where ``predicate(value)`` is true."""
    def run(ctx: dict[str, Any]) -> dict[str, Any]:
        rows = ctx.get("rows", [])
        kept = [r for r in rows if predicate(r.get(field_name))]
        ctx["rows"] = kept
        ctx["_rowcount"] = ctx.get("_rowcount", []) + [{"in": len(rows), "out": len(kept)}]
        return {}
    return Step(name=name, run=run)


def story_map(name: str, fn: Callable[[dict[str, Any]], dict[str, Any]]) -> Step:
    """Transform every row, dropping any that return None."""
    def run(ctx: dict[str, Any]) -> dict[str, Any]:
        rows = ctx.get("rows", [])
        mapped = [m for m in (fn(r) for r in rows) if m is not None]
        ctx["rows"] = mapped
        ctx["_rowcount"] = ctx.get("_rowcount", []) + [{"in": len(rows), "out": len(mapped)}]
        return {}
    return Step(name=name, run=run)


def story_validate(
    name: str,
    required: Iterable[str],
    *,
    strict: bool = True,
    report_path: str | None = None,
) -> Step:
    """Fail (or collect) rows missing required fields.

    Rejected rows carry their 1-based line number in the source CSV, because
    the person who has to fix them is reading the file, not the log.
    """
    required = tuple(required)

    def run(ctx: dict[str, Any]) -> dict[str, Any]:
        rows = ctx.get("rows", [])
        good: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=2):  # header occupies line 1
            missing = [f for f in required if row.get(f) in (None, "")]
            if missing:
                rejected.append({
                    "line": index,
                    "id": row.get("id", ""),
                    "missing": ",".join(missing),
                })
            else:
                good.append(row)
        ctx["rows"] = good
        ctx["rejected"] = rejected
        ctx["_rowcount"] = ctx.get("_rowcount", []) + [{"in": len(rows), "out": len(good)}]
        if rejected and strict:
            preview = "; ".join(f"line {r['line']} missing {r['missing']}" for r in rejected[:3])
            more = f" (+{len(rejected) - 3} more)" if len(rejected) > 3 else ""
            raise ValueError(
                f"{len(rejected)} of {len(rows)} rows missing required fields: {preview}{more}"
            )
        if report_path and rejected:
            Path(report_path).write_text(json.dumps(rejected, indent=2))
        return {}
    return Step(name=name, run=run)


def story_write_csv(name: str, target: str, columns: Iterable[str] | None = None) -> Step:
    """Write ``ctx['rows']`` as CSV, creating parent directories."""
    columns = tuple(columns) if columns else None

    def run(ctx: dict[str, Any]) -> dict[str, Any]:
        rows = ctx.get("rows", [])
        fields = columns or (list(rows[0].keys()) if rows else [])
        path = Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(fields), extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        ctx["_rowcount"] = ctx.get("_rowcount", []) + [{"in": len(rows), "out": len(rows)}]
        return {}
    # Never cached: the written file is the deliverable, so a resumed run must
    # produce it again rather than report success without the output.
    return Step(name=name, run=run, checkpointable=False)


STORY = {
    "load_csv": story_load_csv,
    "filter": story_filter,
    "map": story_map,
    "validate": story_validate,
    "write_csv": story_write_csv,
}


# --------------------------------------------------------------------------
# plan files (TOML subset: no external parser needed)
# --------------------------------------------------------------------------


def _strip_comment(line: str) -> str:
    """Remove a trailing ``#`` comment, ignoring ``#`` inside quoted strings."""
    quote: str | None = None
    for position, char in enumerate(line):
        if quote:
            if char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char == "#":
            return line[:position]
    return line


def parse_plan(text: str) -> dict[str, Any]:
    """Parse a small TOML subset: sections, strings, numbers, booleans, arrays.

    Deliberately not a full TOML implementation. Using ``tomllib`` would need
    Python 3.11, and this runs on 3.10; hand-parsing keeps the dependency count
    at zero and the accepted grammar small enough to read in one sitting.
    """
    data: dict[str, Any] = {}
    section: dict[str, Any] | None = None
    pending_key: str | None = None
    pending_items: list[Any] = []

    def resolve(path: str) -> dict[str, Any]:
        """Follow a dotted section path, creating levels as needed.

        ``[steps.load]`` becomes ``data['steps']['load']``, which is what makes
        readable plan files possible without a full TOML implementation.
        """
        node = data
        for part in path.split("."):
            part = part.strip()
            if not part:
                raise PlanError(f"empty section name in [{path}]")
            nxt = node.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                node[part] = nxt
            node = nxt
        return node

    for number, raw in enumerate(text.splitlines(), start=1):
        line = _strip_comment(raw).strip()
        if not line:
            continue

        if line.startswith("[") and line.endswith("]") and "=" not in line:
            section = resolve(line[1:-1].strip())
            pending_key = None
            continue

        if "=" not in line:
            if pending_key is None:
                raise PlanError(f"line {number}: expected 'key = value'")
            # continuation of a multi-line array
            body = line.strip()
            if pending_items:
                if body.endswith(","):
                    pending_items.append(_scalar(body[:-1].strip(), number))
                    continue
                pending_items.append(_scalar(body, number))
                (section if section is not None else data)[pending_key] = pending_items
                pending_key, pending_items = None, []
                continue
            raise PlanError(f"line {number}: unexpected continuation")

        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Keys before any [section] header land at the root of the document.
        target = section if section is not None else data

        if value.startswith("{") and value.endswith("}"):
            target[key] = _inline_table(value[1:-1], number)
            continue
        if value.startswith("[") and not value.endswith("]"):
            pending_key = key
            pending_items = [_scalar(p.strip(), number)
                             for p in value[1:].split(",") if p.strip()]
            continue
        if value.endswith(",") and value.startswith("["):
            target[key] = pending_items
            pending_key, pending_items = None, []
            continue

        target[key] = _scalar(value, number)

    if pending_key is not None:
        raise PlanError(f"unterminated array for '{pending_key}'")
    return data


def _inline_table(body: str, line: int) -> dict[str, Any]:
    """Parse a single-line inline table: ``{ a = 1, b = "two" }``.

    Values may themselves be quoted strings or numbers; nested tables and
    arrays inside an inline table are not supported, on purpose: a plan file
    should stay readable on one line per key.
    """
    table: dict[str, Any] = {}
    for chunk in _split_top_level(body):
        if not chunk.strip():
            continue
        if "=" not in chunk:
            raise PlanError(f"line {line}: inline table entry needs 'key = value': {chunk!r}")
        key, _, value = chunk.partition("=")
        table[key.strip()] = _scalar(value.strip(), line)
    return table


def _split_top_level(body: str) -> list[str]:
    """Split on commas that are not inside quotes or nested brackets."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    for char in body:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in "\"'":
            quote = char
            current.append(char)
        elif char in "[{":
            depth += 1
            current.append(char)
        elif char in "]}":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def _scalar(text: str, line: int) -> Any:
    text = text.strip().rstrip(",").strip()
    if not text:
        raise PlanError(f"line {line}: empty value")
    if text[0] in "\"'" and text[-1] == text[0] and len(text) >= 2:
        return text[1:-1]
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        return [_scalar(p.strip(), line) for p in inner.split(",") if p.strip()]
    lowered = text.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered in ("null", "none"):
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def load_plan(path: str | os.PathLike[str]) -> dict[str, Any]:
    return parse_plan(Path(path).read_text(encoding="utf-8"))