"""Tests for the story steps: the CSV → validate → transform → write pipeline."""

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etlflow import (  # noqa: E402
    Runner,
    story_filter,
    story_load_csv,
    story_map,
    story_validate,
    story_write_csv,
)

CSV = (
    "id,name,qty,price\n"
    "1,Widget,2,10.50\n"
    "2,Gadget,,4.25\n"
    "3,,7,3.00\n"
    "4,Doohickey,1,99.99\n"
)


def csv_file(tmp: str, content: str = CSV) -> str:
    path = Path(tmp) / "in.csv"
    path.write_text(content, encoding="utf-8")
    return str(path)


class TestLoad(unittest.TestCase):
    def test_loads_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = Runner({"load": story_load_csv("load", csv_file(tmp))}).run()
            self.assertTrue(record.steps[0].ok)
            self.assertEqual(record.steps[0].rows_out, 4)

    def test_bom_is_handled(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bom.csv"
            path.write_bytes(CSV.encode("utf-8-sig"))
            ctx = {}
            story_load_csv("load", str(path)).run(ctx)
            self.assertEqual(len(ctx["rows"]), 4)

    def test_rows_land_in_context_under_a_documented_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = {}
            story_load_csv("load", csv_file(tmp)).run(ctx)
            self.assertEqual(ctx["rows"][0]["name"], "Widget")

    def test_empty_file_loads_zero_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.csv"
            path.write_text("", encoding="utf-8")
            ctx = {}
            story_load_csv("load", str(path)).run(ctx)
            self.assertEqual(ctx["rows"], [])


class TestFilter(unittest.TestCase):
    def test_filter_keeps_matching_rows(self):
        ctx = {"rows": [{"n": 1}, {"n": 5}, {"n": 3}]}
        story_filter("f", "n", lambda v: v > 3).run(ctx)
        self.assertEqual(ctx["rows"], [{"n": 5}])

    def test_filter_records_in_and_out_counts(self):
        ctx = {"rows": [{"n": 1}, {"n": 5}]}
        before = len(ctx.get("_rowcount", []))
        story_filter("f", "n", lambda v: v > 3).run(ctx)
        self.assertEqual(ctx["_rowcount"][before], {"in": 2, "out": 1})

    def test_missing_field_is_passed_as_none(self):
        ctx = {"rows": [{"a": 1}, {}]}
        story_filter("f", "a", lambda v: v is not None).run(ctx)
        self.assertEqual(len(ctx["rows"]), 1)

    def test_filter_can_empty_the_dataset(self):
        ctx = {"rows": [{"n": 1}]}
        story_filter("f", "n", lambda v: False).run(ctx)
        self.assertEqual(ctx["rows"], [])


class TestMap(unittest.TestCase):
    def test_map_transforms_rows(self):
        ctx = {"rows": [{"n": 2}]}
        story_map("m", lambda r: {"double": r["n"] * 2}).run(ctx)
        self.assertEqual(ctx["rows"], [{"double": 4}])

    def test_none_result_drops_the_row(self):
        ctx = {"rows": [{"n": 1}, {"n": 2}]}
        story_map("m", lambda r: None if r["n"] == 1 else r).run(ctx)
        self.assertEqual(ctx["rows"], [{"n": 2}])

    def test_dropped_rows_are_counted_in_the_delta(self):
        ctx = {"rows": [{"n": 1}, {"n": 2}]}
        before = len(ctx.get("_rowcount", []))
        story_map("m", lambda r: None if r["n"] == 1 else r).run(ctx)
        self.assertEqual(ctx["_rowcount"][before], {"in": 2, "out": 1})


class TestValidate(unittest.TestCase):
    def test_missing_field_raises_with_counts_and_a_preview(self):
        ctx = {"rows": [{"id": "1", "name": "a", "qty": "1", "price": "1"},
                        {"id": "2", "name": "", "qty": "1", "price": "1"}]}
        with self.assertRaises(ValueError) as exc:
            story_validate("v", ["id", "name"]).run(ctx)
        message = str(exc.exception)
        self.assertIn("1 of 2 rows", message)
        self.assertIn("line 3", message)  # header is line 1

    def test_line_numbers_account_for_the_header(self):
        ctx = {"rows": [{"id": ""}, {"id": "1"}, {"id": ""}]}
        with self.assertRaises(ValueError):
            story_validate("v", ["id"]).run(ctx)
        self.assertEqual([r["line"] for r in ctx["rejected"]], [2, 4])

    def test_rejected_rows_carry_the_missing_fields(self):
        ctx = {"rows": [{"id": "1", "name": "", "qty": ""}]}
        with self.assertRaises(ValueError):
            story_validate("v", ["id", "name", "qty"]).run(ctx)
        self.assertEqual(ctx["rejected"][0]["missing"], "name,qty")

    def test_strict_false_keeps_going_and_collects(self):
        ctx = {"rows": [{"id": "1", "name": "a"}, {"id": "", "name": "b"}]}
        story_validate("v", ["id", "name"], strict=False).run(ctx)
        self.assertEqual(len(ctx["rows"]), 1)
        self.assertEqual(len(ctx["rejected"]), 1)

    def test_none_and_empty_string_are_both_missing(self):
        ctx = {"rows": [{"id": None}, {"id": ""}]}
        story_validate("v", ["id"], strict=False).run(ctx)
        self.assertEqual(len(ctx["rejected"]), 2)

    def test_rejection_report_is_written_when_requested(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "rejected.json"
            ctx = {"rows": [{"id": "", "line": ""}]}
            story_validate("v", ["id"], strict=False, report_path=str(report)).run(ctx)
            self.assertEqual(len(json.loads(report.read_text())), 1)

    def test_duplicate_field_names_in_a_row(self):
        ctx = {"rows": [{"id": "1", "name": "a"}, {"id": "1", "name": "a"}]}
        story_validate("v", ["id", "name"]).run(ctx)
        self.assertEqual(len(ctx["rows"]), 2)


class TestWrite(unittest.TestCase):
    def test_writes_a_csv_with_a_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.csv"
            ctx = {"rows": [{"a": 1, "b": 2}]}
            story_write_csv("w", str(target)).run(ctx)
            self.assertIn("a,b", target.read_text())
            self.assertIn("1,2", target.read_text())

    def test_creates_missing_parent_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "deep" / "nested" / "out.csv"
            story_write_csv("w", str(target)).run(ctx={"rows": [{"a": 1}]})
            self.assertTrue(target.exists())

    def test_explicit_columns_control_the_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.csv"
            ctx = {"rows": [{"a": 1, "b": 2}]}
            story_write_csv("w", str(target), columns=["b", "a"]).run(ctx)
            self.assertEqual(target.read_text().splitlines()[0], "b,a")

    def test_extra_keys_are_dropped_not_crashed_on(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.csv"
            ctx = {"rows": [{"a": 1, "b": 2, "surprise": 3}]}
            story_write_csv("w", str(target), columns=["a"]).run(ctx)
            self.assertEqual(target.read_text().splitlines(), ["a", "1"])

    def test_empty_rowset_writes_a_header_only_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.csv"
            story_write_csv("w", str(target), columns=["a", "b"]).run(ctx={"rows": []})
            self.assertEqual(target.read_text().strip(), "a,b")

    def test_output_is_readable_again_by_the_loader(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.csv"
            story_write_csv("w", str(target)).run(ctx={"rows": [{"a": 1}, {"a": 2}]})
            ctx = {}
            story_load_csv("load", str(target)).run(ctx)
            self.assertEqual(len(ctx["rows"]), 2)


class TestFullPipeline(unittest.TestCase):
    def test_end_to_end_reports_the_whole_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = csv_file(tmp)
            target = Path(tmp) / "out.csv"

            def to_line_total(row):
                if not row.get("qty") or not row.get("price"):
                    return None  # drop incomplete rows
                return {"id": row["id"], "name": row["name"],
                        "total": f"{float(row['qty']) * float(row['price']):.2f}"}

            record = Runner({
                "load": story_load_csv("load", source),
                "validate": story_validate("validate", ["id", "name"], strict=False),
                "transform": story_map("transform", to_line_total),
                "write": story_write_csv("write", str(target), columns=["id", "name", "total"]),
            }).run()

            self.assertEqual([s.status for s in record.steps],
                             ["ok", "ok", "ok", "ok"])
            self.assertFalse(record.failed)

            with target.open() as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(len(rows), 2)          # two rows dropped upstream
            self.assertEqual(rows[0]["total"], "21.00")
            self.assertEqual(rows[1]["total"], "99.99")
            self.assertIn("[OK  ] validate", record.summary())

    def test_a_broken_step_stops_the_pipeline_and_says_why(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "never.csv"
            record = Runner({
                "load": story_load_csv("load", csv_file(tmp)),
                "validate": story_validate("validate", ["id", "name", "qty", "price"]),
                "write": story_write_csv("write", str(target)),
            }).run()

            self.assertEqual(record.steps[0].status, "ok")
            self.assertEqual(record.steps[1].status, "failed")
            self.assertEqual(record.steps[2].status, "blocked")
            self.assertIn("pipeline stopped", record.steps[2].error)
            self.assertFalse(target.exists())

    def test_resume_skips_finished_steps_after_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = csv_file(tmp)
            target = Path(tmp) / "out.csv"
            checkpoint = Path(tmp) / "cp.json"
            plan = {
                "load": story_load_csv("load", source),
                "validate": story_validate("validate", ["id", "name", "qty", "price"]),
                "write": story_write_csv("write", str(target)),
            }

            first = Runner(plan, checkpoint=checkpoint).run()
            self.assertEqual([s.status for s in first.steps], ["ok", "failed", "blocked"])

            # The CSV is corrected, so the source fingerprint changes and the
            # load step is deliberately re-run rather than replaying the old
            # rows. Everything downstream then succeeds.
            csv_file(tmp, "id,name,qty,price\n1,Widget,2,10.50\n")
            second = Runner(plan, checkpoint=checkpoint).run()
            statuses = {s.name: s.status for s in second.steps}
            self.assertEqual(statuses["load"], "ok")       # input changed
            self.assertEqual(statuses["validate"], "ok")
            self.assertEqual(statuses["write"], "ok")
            self.assertEqual(len(second.failed), 0)
            self.assertTrue(target.exists())

    def test_resume_skips_load_when_the_input_is_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = csv_file(tmp, "id,name,qty,price\n1,Widget,2,10.50\n")
            target = Path(tmp) / "out.csv"
            checkpoint = Path(tmp) / "cp.json"
            plan = {
                "load": story_load_csv("load", source),
                "validate": story_validate("validate", ["id", "name", "qty", "price"]),
                "write": story_write_csv("write", str(target), ["id", "name"]),
            }
            first = Runner(plan, checkpoint=checkpoint).run()
            self.assertTrue(all(s.ok for s in first.steps))

            second = Runner(plan, checkpoint=checkpoint).run()
            statuses = {s.name: s.status for s in second.steps}
            self.assertEqual(statuses["load"], "skipped")
            # The write step is never cached: its file is the deliverable.
            self.assertEqual(statuses["write"], "ok")
            # Replayed context, not an empty file: resume must not lose the data.
            self.assertIn("Widget", target.read_text())

    def test_write_is_never_cached(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.csv"
            plan = {"write": story_write_csv("write", str(target), ["id"])}
            Runner(plan, checkpoint=Path(tmp) / "cp.json").run()
            target.unlink()  # pretend the output was lost
            Runner(plan, checkpoint=Path(tmp) / "cp.json").run()
            self.assertTrue(target.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)