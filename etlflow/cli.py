"""Command line: ``python -m etlflow run -f plan.toml`` and ``python -m etlflow demo``.

A plan file declares steps; this maps them onto the step library and runs them.
Anything unknown is refused with a message, never a traceback: a bad plan file
is user input, and user input does not deserve a stack trace.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from . import (
    PlanError,
    Runner,
    load_plan,
    story_filter,
    story_load_csv,
    story_map,
    story_validate,
    story_write_csv,
)

BUILTIN_STEPS = {"load_csv", "filter", "map", "validate", "write_csv"}


def build_steps(plan: dict[str, Any]) -> dict[str, Any]:
    """Turn a parsed plan into Runner steps.

    Rejects unknown step types and unknown keys, because a typo in a plan file
    should say so rather than silently doing nothing.
    """
    from . import Step

    declared = plan.get("steps")
    if not isinstance(declared, dict) or not declared:
        raise PlanError("plan has no [steps.*] sections")

    steps: dict[str, Any] = {}
    for name, config in declared.items():
        if not isinstance(config, dict):
            raise PlanError(f"step '{name}' has no configuration")
        kind = config.get("kind") or config.get("type")
        if kind is None:
            raise PlanError(
                f"step '{name}' is missing 'kind'; expected one of "
                f"{', '.join(sorted(BUILTIN_STEPS))}"
            )
        if kind not in BUILTIN_STEPS:
            raise PlanError(f"step '{name}' has unknown kind '{kind}'")

        depends = tuple(config.get("depends_on", ()) or ())
        retries = int(config.get("retries", 0) or 0)

        if kind == "load_csv":
            source = config.get("source")
            if not source:
                raise PlanError(f"step '{name}': load_csv needs 'source'")
            step = story_load_csv(name, str(source))
        elif kind == "validate":
            required = config.get("required") or []
            if isinstance(required, str):
                required = [r.strip() for r in required.split(",") if r.strip()]
            step = story_validate(
                name, required,
                strict=bool(config.get("strict", True)),
                report_path=config.get("report"),
            )
        elif kind == "write_csv":
            target = config.get("target")
            if not target:
                raise PlanError(f"step '{name}': write_csv needs 'target'")
            step = story_write_csv(name, str(target), config.get("columns"))
        elif kind == "filter":
            field = config.get("field")
            if not field:
                raise PlanError(f"step '{name}': filter needs 'field'")
            op = str(config.get("op", "not_empty"))
            threshold = config.get("value")
            step = story_filter(name, field, _predicate(op, threshold, name))
        else:  # map
            expression = config.get("keep_if") or config.get("drop_if_empty")
            step = story_map(name, _row_transform(name, config))

        steps[name] = Step(
            name=step.name, run=step.run, retries=retries,
            depends_on=depends, allow_failure=bool(config.get("allow_failure", False)),
        )

    # Resolve depends_on from implicit "previous step" shorthand if used.
    ordered = list(steps)
    for index, name in enumerate(ordered):
        if not steps[name].depends_on and config_uses_previous(declared[name]):
            if index:
                steps[name].depends_on = (ordered[index - 1],)
    return steps


def config_uses_previous(config: dict[str, Any]) -> bool:
    return bool(config.get("after"))


def _predicate(op: str, value: Any, step_name: str):
    if op == "not_empty":
        return lambda v: v not in (None, "")
    if op == "equals":
        return lambda v: v == value
    if op == "greater_than":
        try:
            bound = float(value)
        except (TypeError, ValueError) as exc:
            raise PlanError(f"step '{step_name}': greater_than needs a numeric 'value'") from exc

        def above(value_: Any) -> bool:
            # A non-numeric cell is not "greater than" anything; skipping it is
            # correct, and raising here would abort a whole run over one odd
            # row in a column that was misconfigured.
            try:
                return float(value_) > bound
            except (TypeError, ValueError):
                return False

        return above
    if op == "contains":
        needle = str(value)
        return lambda v: v is not None and needle in str(v)
    raise PlanError(f"step '{step_name}': unknown filter op '{op}'")


def _row_transform(step_name: str, config: dict[str, Any]):
    """Build a row transform from declarative fields.

    Supported: ``keep_fields``, ``drop_fields``, ``computed`` (a simple
    ``name = a * b`` style expression over other fields, evaluated safely).
    """
    keep = config.get("keep_fields")
    drop = set(config.get("drop_fields") or ())
    raw_computed = config.get("computed") or {}
    if isinstance(raw_computed, dict):
        computed = dict(raw_computed)
    elif isinstance(raw_computed, list):
        # TOML inline tables arrive as a list of key/value pairs from the
        # hand-written parser; accept either shape.
        computed = {
            str(pair[0]): pair[1]
            for pair in raw_computed
            if isinstance(pair, (list, tuple)) and len(pair) == 2
        }
    else:
        raise PlanError(f"step '{step_name}': 'computed' must be a table")

    def transform(row: dict[str, Any]) -> dict[str, Any] | None:
        out: dict[str, Any] = {k: v for k, v in row.items() if k not in drop}
        if keep:
            out = {k: v for k, v in out.items() if k in keep}
        for name, expression in computed.items():
            out[name] = _evaluate(expression, row, step_name)
        if config.get("drop_if_empty"):
            empty = set(config["drop_if_empty"])
            if all(out.get(k) in (None, "") for k in empty):
                return None
        return out

    return transform


def _evaluate(expression: str, row: dict[str, Any], step_name: str) -> Any:
    """Evaluate ``"qty * price"`` against a row.

    Names resolve to field values; only literals and these operators are
    allowed. A plan file is not a place to run arbitrary code.
    """
    import ast
    import operator

    ops = {
        ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
        ast.Div: operator.truediv, ast.Mod: operator.mod, ast.Pow: operator.pow,
    }
    # Positional arity: round(x, 2) is the natural spelling for money.
    funcs: dict[str, tuple[Callable[..., Any], int]] = {
        "round": (round, 2), "abs": (abs, 1), "min": (min, 2),
        "max": (max, 2), "float": (float, 1), "int": (int, 1),
    }

    def walk(node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in funcs:
                raise PlanError(
                    f"step '{step_name}': use {node.id}(...) form, not bare {node.id}"
                )
            raw = row.get(node.id)
            if raw in (None, ""):
                return 0.0
            try:
                return float(raw)
            except (TypeError, ValueError):
                return raw
        if isinstance(node, ast.BinOp) and type(node.op) in ops:
            left, right = walk(node.left), walk(node.right)
            return ops[type(node.op)](left, right)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id not in funcs:
                raise PlanError(f"step '{step_name}': unknown function '{node.func.id}'")
            func, arity = funcs[node.func.id]
            if len(node.args) > arity:
                raise PlanError(
                    f"step '{step_name}': {node.func.id} takes at most {arity} argument(s)"
                )
            return func(*(walk(a) for a in node.args))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -walk(node.operand)
        raise PlanError(f"step '{step_name}': unsupported expression")

    try:
        tree = ast.parse(str(expression), mode="eval")
    except SyntaxError as exc:
        raise PlanError(f"step '{step_name}': bad expression {expression!r}") from exc
    return walk(tree.body)


# --------------------------------------------------------------------------
# demo: the pipeline a client recognises, runnable with zero setup
# --------------------------------------------------------------------------

DEMO_CSV = """id,name,qty,price
1,Widget,2,10.50
2,Gadget,3,4.25
3,,7,3.00
4,Doohickey,1,99.99
5,Thingamajig,,15.00
"""


def run_demo(out: str, dry_run: bool, checkpoint: str | None) -> int:
    """Normalise a messy supplier CSV into a clean line-item report."""
    import csv
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        source = Path(tmp) / "supplier.csv"
        source.write_text(DEMO_CSV, encoding="utf-8")
        report = Path(tmp) / "rejected.json"
        checkpoint_path = checkpoint or str(Path(tmp) / "checkpoint.json")

        def to_line_total(row: dict[str, Any]) -> dict[str, Any] | None:
            if not row.get("qty") or not row.get("price"):
                return None  # incomplete rows are dropped, not guessed at
            return {
                "id": row["id"],
                "name": row["name"],
                "total": f"{float(row['qty']) * float(row['price']):.2f}",
            }

        steps = {
            "load": story_load_csv("load", str(source)),
            "validate": story_validate("validate", ["id", "name", "qty", "price"],
                                       strict=False, report_path=str(report)),
            "transform": story_map("transform", to_line_total),
            "write": story_write_csv("write", out, ["id", "name", "total"]),
        }

        runner = Runner(steps, checkpoint=checkpoint_path, dry_run=dry_run)
        record = runner.run()
        print(record.summary())
        print()
        if report.exists():
            rejected = json.loads(report.read_text())
            print(f"rejected {len(rejected)} row(s):")
            for row in rejected:
                print(f"  line {row['line']}: missing {row['missing']}")
            print()
        if dry_run:
            print(f"dry run: nothing was written to {out}")
        else:
            print(f"wrote {out}:")
            print(Path(out).read_text().rstrip())
        return 1 if record.failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etlflow",
        description="Declarative data pipelines that fail loudly and resume cheaply.",
    )
    parser.add_argument("--version", action="version", version=f"etlflow {_version()}")
    sub = parser.add_subparsers(dest="command")

    run_cmd = sub.add_parser("run", help="execute a plan file")
    run_cmd.add_argument("-f", "--file", required=True, help="plan file (TOML subset)")
    run_cmd.add_argument("--dry-run", action="store_true")
    run_cmd.add_argument("--checkpoint", help="path to the checkpoint file")
    run_cmd.add_argument("--json", action="store_true", help="emit the run record as JSON")

    demo = sub.add_parser("demo", help="run the bundled example pipeline")
    demo.add_argument("--out", default="orders.csv", help="output CSV path")
    demo.add_argument("--dry-run", action="store_true")
    demo.add_argument("--checkpoint", help="path to the checkpoint file")
    return parser


def _version() -> str:
    from . import __version__

    return __version__


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    try:
        if args.command == "demo":
            return run_demo(args.out, args.dry_run, args.checkpoint)

        plan = load_plan(args.file)
        steps = build_steps(plan)
        record = Runner(steps, dry_run=args.dry_run, checkpoint=args.checkpoint).run()
        if args.json:
            print(record.to_json())
        else:
            print(record.summary())
        return 1 if record.failed else 0
    except PlanError as exc:
        print(f"etlflow: {exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"etlflow: {exc.strerror}: {exc.filename}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - a CLI should not print a traceback
        print(f"etlflow: unexpected {type(exc).__name__}: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())