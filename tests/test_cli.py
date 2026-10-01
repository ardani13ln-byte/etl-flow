"""Tests for the plan-file parser and the CLI.

The parser is hand-written on purpose (no tomllib on 3.10, no dependency), so
it needs the same adversarial treatment a parser would get.
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etlflow import PlanError, load_plan, parse_plan  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

PLAN = """
# a comment
name = "orders"
version = 2
ratio = 0.25
enabled = true
disabled = false
missing = null
tags = ["a", "b", "c"]

[steps.load]
source = "in.csv"
retries = 3

[steps.validate]
required = ["id", "name"]
strict = false
"""


class TestParsePlan(unittest.TestCase):
    def setUp(self):
        self.plan = parse_plan(PLAN)

    def test_top_level_strings(self):
        self.assertEqual(self.plan["name"], "orders")

    def test_integers_and_floats(self):
        self.assertEqual(self.plan["version"], 2)
        self.assertEqual(self.plan["ratio"], 0.25)

    def test_booleans(self):
        self.assertIs(self.plan["enabled"], True)
        self.assertIs(self.plan["disabled"], False)

    def test_null(self):
        self.assertIsNone(self.plan["missing"])

    def test_single_line_array(self):
        self.assertEqual(self.plan["tags"], ["a", "b", "c"])

    def test_sections_become_nested_dicts(self):
        self.assertEqual(self.plan["steps"]["load"]["source"], "in.csv")

    def test_types_inside_sections(self):
        self.assertEqual(self.plan["steps"]["load"]["retries"], 3)
        self.assertIs(self.plan["steps"]["validate"]["strict"], False)

    def test_arrays_inside_sections(self):
        self.assertEqual(self.plan["steps"]["validate"]["required"], ["id", "name"])

    def test_comments_are_ignored(self):
        self.assertNotIn("# a comment", self.plan)

    def test_single_quoted_strings(self):
        self.assertEqual(parse_plan("k = 'v'")["k"], "v")

    def test_unterminated_array_is_an_error(self):
        with self.assertRaises(PlanError):
            parse_plan('k = ["a", "b"')

    def test_empty_value_is_an_error(self):
        with self.assertRaises(PlanError):
            parse_plan("k = ")

    def test_garbage_line_is_an_error(self):
        with self.assertRaises(PlanError):
            parse_plan("this is not a plan line at all")

    def test_error_reports_the_line_number(self):
        with self.assertRaises(PlanError) as ctx:
            parse_plan('a = "1"\nb = "2"\nthis is broken')
        self.assertIn("3", str(ctx.exception))

    def test_empty_plan_is_an_empty_dict(self):
        self.assertEqual(parse_plan(""), {})
        self.assertEqual(parse_plan("# only a comment\n"), {})

    def test_values_containing_equals_signs_survive(self):
        self.assertEqual(parse_plan('url = "https://x.test/?a=1&b=2"')["url"],
                         "https://x.test/?a=1&b=2")

    def test_empty_array(self):
        self.assertEqual(parse_plan("k = []")["k"], [])

    def test_load_plan_reads_from_disk(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.toml"
            path.write_text(PLAN, encoding="utf-8")
            self.assertEqual(load_plan(path)["name"], "orders")


class TestCli(unittest.TestCase):
    def run_cli(self, args, stdin=None):
        return subprocess.run(
            [sys.executable, "-m", "etlflow", *args],
            input=stdin, capture_output=True, text=True, cwd=str(ROOT),
        )

    def test_help_works(self):
        proc = self.run_cli(["--help"])
        self.assertEqual(proc.returncode, 0)
        self.assertIn("usage", proc.stdout.lower())

    def test_demo_runs_and_exits_zero(self):
        # --out points at tmp so the test never pollutes the repo directory.
        with tempfile.TemporaryDirectory() as tmp:
            proc = self.run_cli(["demo", "--out", str(Path(tmp) / "o.csv")])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("run finished", proc.stdout)

    def test_demo_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "orders.csv"
            proc = self.run_cli(["demo", "--out", str(out), "--dry-run"])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertFalse(out.exists())
            self.assertIn("dry run", proc.stdout)

    def test_demo_writes_the_output_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "orders.csv"
            proc = self.run_cli(["demo", "--out", str(out)])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertTrue(out.exists())
            lines = out.read_text().strip().splitlines()
            self.assertEqual(lines[0].split(","), ["id", "name", "total"])
            self.assertIn("21.00", out.read_text())

    def test_demo_checkpoint_is_created_and_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            cp = Path(tmp) / "cp.json"
            first = self.run_cli(["demo", "--out", str(Path(tmp) / "a.csv"),
                                  "--checkpoint", str(cp)])
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertTrue(cp.exists())
            # Checkpoint entries carry the replayed context, not just a marker.
            data = json.loads(cp.read_text())
            self.assertTrue(any("rows" in entry.get("context", {}) for entry in data.values()))

            second = self.run_cli(["demo", "--out", str(Path(tmp) / "b.csv"),
                                   "--checkpoint", str(cp)])
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertIn("[SKIP]", second.stdout)
            # The resumed run must reproduce the same rows, not an empty file.
            self.assertEqual(
                (Path(tmp) / "a.csv").read_text(), (Path(tmp) / "b.csv").read_text()
            )

    def test_bad_plan_exits_2_with_a_message_not_a_traceback(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "bad.toml"
            plan.write_text("this is broken", encoding="utf-8")
            proc = self.run_cli(["run", "-f", str(plan)])
            self.assertEqual(proc.returncode, 2)
            self.assertNotIn("Traceback", proc.stderr)

    def test_missing_plan_file_exits_2(self):
        proc = self.run_cli(["run", "-f", "/nonexistent/plan.toml"])
        self.assertEqual(proc.returncode, 2)
        self.assertNotIn("Traceback", proc.stderr)

    def test_unknown_step_in_a_plan_exits_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "p.toml"
            plan.write_text('[steps.nonsense]\nsource = "x.csv"\n', encoding="utf-8")
            proc = self.run_cli(["run", "-f", str(plan)])
            self.assertEqual(proc.returncode, 2)
            self.assertNotIn("Traceback", proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)