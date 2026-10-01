"""Tests for plan-file execution: steps built from config, expressions, CLI wiring."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etlflow import PlanError  # noqa: E402
from etlflow.cli import build_steps, run_demo  # noqa: E402

CSV = "id,name,qty,price\n1,Widget,2,10.50\n2,,7,3.00\n3,Gizmo,4,1.25\n"

BASE_PLAN = """
[steps.load]
kind = "load_csv"
source = "{source}"

[steps.write]
kind = "write_csv"
target = "{target}"
columns = ["id", "name"]
after = "load"
"""


def plan_with_source(tmp: str, extra: str = "") -> str:
    source = Path(tmp) / "in.csv"
    source.write_text(CSV, encoding="utf-8")
    target = Path(tmp) / "out.csv"
    return BASE_PLAN.format(source=source, target=target) + extra


def load_plan_text(text: str):
    from etlflow import parse_plan

    return parse_plan(text)


def run_plan(tmp: str, extra: str = ""):
    from etlflow import Runner

    steps = build_steps(load_plan_text(plan_with_source(tmp, extra)))
    record = Runner(steps, sleep=lambda _: None).run()
    return record, Path(tmp) / "out.csv"


class TestBuildSteps(unittest.TestCase):
    def test_minimal_plan_builds_and_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            record, out = run_plan(tmp)
            self.assertEqual([s.status for s in record.steps], ["ok", "ok"])
            self.assertIn("Widget", out.read_text())

    def test_plan_without_steps_is_refused(self):
        with self.assertRaises(PlanError) as ctx:
            build_steps({"name": "empty"})
        self.assertIn("steps", str(ctx.exception))

    def test_step_without_kind_names_the_alternatives(self):
        with self.assertRaises(PlanError) as ctx:
            build_steps({"steps": {"a": {"source": "x.csv"}}})
        self.assertIn("kind", str(ctx.exception))
        self.assertIn("load_csv", str(ctx.exception))

    def test_unknown_kind_is_refused(self):
        with self.assertRaises(PlanError) as ctx:
            build_steps({"steps": {"a": {"kind": "teleport"}}})
        self.assertIn("teleport", str(ctx.exception))

    def test_load_without_source_is_refused(self):
        with self.assertRaises(PlanError) as ctx:
            build_steps({"steps": {"a": {"kind": "load_csv"}}})
        self.assertIn("source", str(ctx.exception))

    def test_write_without_target_is_refused(self):
        with self.assertRaises(PlanError) as ctx:
            build_steps({"steps": {"a": {"kind": "write_csv"}}})
        self.assertIn("target", str(ctx.exception))

    def test_filter_without_field_is_refused(self):
        with self.assertRaises(PlanError):
            build_steps({"steps": {"a": {"kind": "filter"}}})

    def test_dependencies_come_from_after(self):
        plan = load_plan_text(plan_with_source.__self__ if False else """
[steps.a]
kind = "load_csv"
source = "x.csv"
[steps.b]
kind = "validate"
required = ["id"]
after = "a"
""")
        steps = build_steps(plan)
        self.assertEqual(steps["b"].depends_on, ("a",))

    def test_retries_are_read_from_the_plan(self):
        plan = load_plan_text("""
[steps.a]
kind = "load_csv"
source = "x.csv"
retries = 4
""")
        self.assertEqual(build_steps(plan)["a"].retries, 4)


class TestPredicates(unittest.TestCase):
    def _filter(self, tmp, op, value=""):
        extra = f'\n[steps.f]\nkind = "filter"\nfield = "name"\nop = "{op}"\nvalue = "{value}"\nafter = "load"\n'
        plan = load_plan_text(plan_with_source(tmp, extra))
        steps = build_steps(plan)
        # Story steps mutate the context in place rather than returning it.
        ctx = {"rows": [{"name": "Widget"}, {"name": ""}, {"name": None}]}
        steps["f"].run(ctx)
        return ctx

    def test_not_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = self._filter(tmp, "not_empty")["rows"]
        self.assertEqual(rows, [{"name": "Widget"}])

    def test_equals(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = self._filter(tmp, "equals", "Widget")["rows"]
        self.assertEqual(rows, [{"name": "Widget"}])

    def test_contains(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = self._filter(tmp, "contains", "idg")["rows"]
        self.assertEqual(rows, [{"name": "Widget"}])

    def test_greater_than_on_numeric(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = self._filter(tmp, "greater_than", "5")["rows"]
        self.assertEqual(rows, [])

    def test_greater_than_needs_a_number(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = '\n[steps.f]\nkind = "filter"\nfield = "name"\nop = "greater_than"\nvalue = "abc"\n'
            with self.assertRaises(PlanError) as ctx:
                build_steps(load_plan_text(plan_with_source(tmp, extra)))
        self.assertIn("numeric", str(ctx.exception))

    def test_unknown_op_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = '\n[steps.f]\nkind = "filter"\nfield = "name"\nop = "sounds_like"\n'
            with self.assertRaises(PlanError) as ctx:
                build_steps(load_plan_text(plan_with_source(tmp, extra)))
        self.assertIn("sounds_like", str(ctx.exception))


class TestComputedExpressions(unittest.TestCase):
    def test_arithmetic_and_round(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = ('\n[steps.t]\nkind = "map"\nafter = "load"\n'
                     'computed = { total = "round(qty * price, 2)" }\n')
            record, _ = run_plan(tmp, extra)
            self.assertEqual([s.status for s in record.steps], ["ok", "ok", "ok"])

    def test_total_is_computed_correctly(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = ('\n[steps.t]\nkind = "map"\nafter = "load"\n'
                     'keep_fields = ["id", "name"]\n'
                     'computed = { total = "round(qty * price, 2)" }\n')
            plan = load_plan_text(plan_with_source(tmp, extra))
            steps = build_steps(plan)
            ctx = {}
            steps["load"].run(ctx)
            steps["t"].run(ctx)
            totals = {r["id"]: r["total"] for r in ctx["rows"]}
            self.assertEqual(totals["1"], 21.0)
            self.assertEqual(totals["3"], 5.0)

    def test_missing_field_treated_as_zero_not_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = ('\n[steps.t]\nkind = "map"\nafter = "load"\n'
                     'computed = { total = "qty * price" }\n')
            plan = load_plan_text(plan_with_source(tmp, extra))
            steps = build_steps(plan)
            ctx = {}
            steps["load"].run(ctx)
            steps["t"].run(ctx)  # row 2 has qty but no name; must not raise
            self.assertEqual(len(ctx["rows"]), 3)

    def test_unsupported_syntax_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = '\n[steps.t]\nkind = "map"\nafter = "load"\ncomputed = { x = "open(\'/etc/passwd\')" }\n'
            plan = load_plan_text(plan_with_source(tmp, extra))
            steps = build_steps(plan)
            ctx = {"rows": [{"id": "1"}]}
            with self.assertRaises(PlanError):
                steps["t"].run(ctx)

    def test_arbitrary_attribute_access_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = '\n[steps.t]\nkind = "map"\nafter = "load"\ncomputed = { x = "qty.__class__" }\n'
            plan = load_plan_text(plan_with_source(tmp, extra))
            steps = build_steps(plan)
            with self.assertRaises(PlanError) as ctx:
                steps["t"].run({"rows": [{"qty": "1"}]})
        self.assertIn("unsupported", str(ctx.exception))

    def test_subscript_and_comprehension_are_refused(self):
        for expression in ("qty[0]", "[r for r in rows]", "lambda: 1"):
            with tempfile.TemporaryDirectory() as tmp:
                extra = f'\n[steps.t]\nkind = "map"\nafter = "load"\ncomputed = {{ x = "{expression}" }}\n'
                plan = load_plan_text(plan_with_source(tmp, extra))
                steps = build_steps(plan)
                with self.assertRaises(PlanError, msg=expression):
                    steps["t"].run({"rows": [{"qty": "1"}]})

    def test_unknown_function_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = '\n[steps.t]\nkind = "map"\nafter = "load"\ncomputed = { x = "shell(qty)" }\n'
            plan = load_plan_text(plan_with_source(tmp, extra))
            steps = build_steps(plan)
            with self.assertRaises(PlanError) as ctx:
                steps["t"].run({"rows": [{"qty": "1"}]})
        self.assertIn("shell", str(ctx.exception))

    def test_drop_if_empty_removes_the_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = ('\n[steps.t]\nkind = "map"\nafter = "load"\n'
                     'keep_fields = ["id", "name"]\ndrop_if_empty = ["name"]\n')
            plan = load_plan_text(plan_with_source(tmp, extra))
            steps = build_steps(plan)
            ctx = {}
            steps["load"].run(ctx)
            steps["t"].run(ctx)
            self.assertEqual([r["id"] for r in ctx["rows"]], ["1", "3"])

    def test_drop_fields_removes_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            extra = '\n[steps.t]\nkind = "map"\nafter = "load"\ndrop_fields = ["price"]\n'
            plan = load_plan_text(plan_with_source(tmp, extra))
            steps = build_steps(plan)
            ctx = {}
            steps["load"].run(ctx)
            steps["t"].run(ctx)
            self.assertNotIn("price", ctx["rows"][0])


class TestInlineTableParsing(unittest.TestCase):
    def test_comma_inside_a_quoted_value_is_kept(self):
        from etlflow import parse_plan

        self.assertEqual(parse_plan('a = { x = "1,2,3" }')["a"], {"x": "1,2,3"})

    def test_multiple_entries(self):
        from etlflow import parse_plan

        table = parse_plan('a = { x = 1, y = "two", z = true }')["a"]
        self.assertEqual(table, {"x": 1, "y": "two", "z": True})

    def test_entry_without_equals_is_an_error(self):
        from etlflow import parse_plan

        with self.assertRaises(PlanError):
            parse_plan("a = { x }")


class TestDemo(unittest.TestCase):
    def test_demo_returns_zero_and_writes_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "orders.csv"
            code = run_demo(str(out), dry_run=False, checkpoint=None)
            self.assertEqual(code, 0)
            rows = out.read_text().strip().splitlines()
            self.assertEqual(len(rows), 4)  # header + 3 valid rows
            self.assertIn("21.00", out.read_text())

    def test_demo_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "orders.csv"
            self.assertEqual(run_demo(str(out), dry_run=True, checkpoint=None), 0)
            self.assertFalse(out.exists())

    def test_demo_reports_rejected_rows_with_line_numbers(self):
        # A dry run executes nothing, so the rejection report only exists on a
        # real run. Asserting it here guards the report path end to end.
        with tempfile.TemporaryDirectory() as tmp:
            import contextlib
            import io

            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                run_demo(str(Path(tmp) / "o.csv"), dry_run=False, checkpoint=None)
            output = buffer.getvalue()
            self.assertIn("rejected 2 row(s)", output)
            self.assertIn("line 4", output)


if __name__ == "__main__":
    unittest.main(verbosity=2)