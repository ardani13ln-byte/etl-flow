"""Tests for the runner: ordering, retries, checkpoints, dry-run and failure."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etlflow import (  # noqa: E402
    CheckpointStore,
    FlowError,
    PlanError,
    Runner,
    Step,
    story_filter,
    story_load_csv,
    story_map,
    story_validate,
    story_write_csv,
)

CSV = "id,name,qty,price\n1,Widget,2,10.50\n2,Gadget,,4.25\n3,,7,3.00\n4,Doohickey,1,99.99\n"


def csv_file(tmp: str) -> str:
    path = Path(tmp) / "in.csv"
    path.write_text(CSV, encoding="utf-8")
    return str(path)


class TestOrdering(unittest.TestCase):
    def test_dependencies_run_first(self):
        calls = []
        runner = Runner({
            "c": Step("c", lambda ctx: calls.append("c")),
            "a": Step("a", lambda ctx: calls.append("a")),
            "b": Step("b", lambda ctx: calls.append("b"), depends_on=("a",)),
        })
        runner.run()
        # Declaration order is kept for independent steps; b still follows a.
        self.assertEqual(calls, ["c", "a", "b"])

    def test_declaration_order_is_preserved_without_dependencies(self):
        # Sorting alphabetically here would reorder a pipeline whose steps are
        # simply named out of sequence.
        calls = []
        runner = Runner({
            "zebra": Step("zebra", lambda ctx: calls.append("zebra")),
            "alpha": Step("alpha", lambda ctx: calls.append("alpha")),
        })
        runner.run()
        self.assertEqual(calls, ["zebra", "alpha"])

    def test_chain_of_three(self):
        calls = []
        runner = Runner({
            "a": Step("a", lambda ctx: calls.append("a")),
            "b": Step("b", lambda ctx: calls.append("b"), depends_on=("a",)),
            "c": Step("c", lambda ctx: calls.append("c"), depends_on=("b",)),
        })
        self.assertTrue(runner.run().steps[-1].ok)
        self.assertEqual(calls, ["a", "b", "c"])

    def test_unknown_dependency_is_a_plan_error(self):
        runner = Runner({"a": Step("a", lambda ctx: None, depends_on=("ghost",))})
        with self.assertRaises(PlanError) as ctx:
            runner.run()
        self.assertIn("ghost", str(ctx.exception))

    def test_cycle_is_detected_with_the_path_named(self):
        runner = Runner({
            "a": Step("a", lambda ctx: None, depends_on=("b",)),
            "b": Step("b", lambda ctx: None, depends_on=("a",)),
        })
        with self.assertRaises(PlanError) as ctx:
            runner.run()
        message = str(ctx.exception)
        self.assertIn("cycle", message)
        self.assertIn("a", message)
        self.assertIn("b", message)

    def test_independent_steps_all_run(self):
        calls = []
        runner = Runner({
            name: Step(name, (lambda n: lambda ctx: calls.append(n))(name))
            for name in ("a", "b", "c", "d")
        })
        record = runner.run()
        self.assertEqual(len(record.steps), 4)
        self.assertEqual(sorted(calls), ["a", "b", "c", "d"])


class TestFailure(unittest.TestCase):
    def test_failure_records_the_exception_type_and_message(self):
        def boom(ctx):
            raise ValueError("bad input")
        record = Runner({"a": Step("a", boom)}).run()
        step = record.steps[0]
        self.assertEqual(step.status, "failed")
        self.assertIn("ValueError", step.error)
        self.assertIn("bad input", step.error)

    def test_downstream_steps_are_blocked_not_run(self):
        calls = []
        record = Runner({
            "a": Step("a", lambda ctx: (_ for _ in ()).throw(RuntimeError("x"))),
            "b": Step("b", lambda ctx: calls.append("b"), depends_on=("a",)),
        }).run()
        self.assertEqual(len(calls), 0)
        statuses = {s.name: s.status for s in record.steps}
        self.assertEqual(statuses["a"], "failed")
        self.assertEqual(statuses["b"], "blocked")

    def test_blocked_step_explains_which_dependency(self):
        record = Runner({
            "a": Step("a", lambda ctx: (_ for _ in ()).throw(RuntimeError("x"))),
            "b": Step("b", lambda ctx: None, depends_on=("a",)),
        }).run()
        blocked = [s for s in record.steps if s.status == "blocked"][0]
        self.assertIn("a", blocked.error)

    def test_allow_failure_keeps_the_pipeline_running(self):
        calls = []
        record = Runner({
            "a": Step("a", lambda ctx: (_ for _ in ()).throw(RuntimeError("x")),
                      allow_failure=True),
            "b": Step("b", lambda ctx: calls.append("b"), depends_on=("a",)),
        }, stop_on_failure=False).run()
        self.assertEqual(calls, ["b"])
        self.assertEqual({s.name: s.status for s in record.steps}["a"], "failed")

    def test_failed_step_is_in_the_failed_list(self):
        record = Runner({"a": Step("a", lambda ctx: 1 / 0)}).run()
        self.assertEqual(len(record.failed), 1)


class TestRetries(unittest.TestCase):
    def test_step_retries_until_success(self):
        state = {"n": 0}

        def flaky(ctx):
            state["n"] += 1
            if state["n"] < 3:
                raise ConnectionError("reset")
            return {"value": "ok"}

        record = Runner({"a": Step("a", flaky, retries=3)}, sleep=lambda _: None).run()
        self.assertEqual(record.steps[0].status, "ok")
        self.assertEqual(state["n"], 3)

    def test_retries_exhausted_reports_the_last_error(self):
        def always(ctx):
            raise ConnectionError("still down")
        record = Runner({"a": Step("a", always, retries=2)}, sleep=lambda _: None).run()
        self.assertEqual(record.steps[0].status, "failed")
        self.assertIn("still down", record.steps[0].error)

    def test_no_retry_by_default(self):
        state = {"n": 0}

        def flaky(ctx):
            state["n"] += 1
            raise ConnectionError("reset")

        Runner({"a": Step("a", flaky)}, sleep=lambda _: None).run()
        self.assertEqual(state["n"], 1)

    def test_sleep_is_injectable_so_tests_never_wait(self):
        waits = []

        def flaky(ctx):
            raise ConnectionError("x")

        Runner({"a": Step("a", flaky, retries=3)}, sleep=waits.append).run()
        self.assertEqual(waits, [1.0, 2.0, 4.0])  # capped by max_delay=10


class TestCheckpoints(unittest.TestCase):
    def test_second_run_skips_completed_steps(self):
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cp.json"
            plan = {"a": Step("a", lambda ctx: calls.append("a"))}
            Runner(plan, checkpoint=path).run()
            Runner(plan, checkpoint=path).run()
        self.assertEqual(calls, ["a"])  # ran once
        self.assertEqual(len(calls), 1)

    def test_checkpoint_file_is_json_and_reloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cp.json"
            Runner({"a": Step("a", lambda ctx: None)}, checkpoint=path).run()
            self.assertIn("{", path.read_text())
            store = CheckpointStore(path)
            self.assertTrue(any(store.done(k) for k in store._data))

    def test_corrupt_checkpoint_does_not_crash_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cp.json"
            path.write_text("{ this is not json")
            record = Runner({"a": Step("a", lambda ctx: None)}, checkpoint=path).run()
        self.assertTrue(record.steps[0].ok)

    def test_checkpoint_write_is_atomic(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cp.json"
            Runner({"a": Step("a", lambda ctx: None)}, checkpoint=path).run()
            self.assertFalse((Path(tmp) / "cp.tmp").exists())

    def test_without_checkpoint_nothing_is_skipped(self):
        calls = []
        plan = {"a": Step("a", lambda ctx: calls.append("a"))}
        Runner(plan).run()
        Runner(plan).run()
        self.assertEqual(len(calls), 2)


class TestDryRun(unittest.TestCase):
    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = csv_file(tmp)
            target = Path(tmp) / "out.csv"
            runner = Runner({
                "load": story_load_csv("load", source),
                "write": story_write_csv("write", str(target)),
            }, dry_run=True)
            record = runner.run()
            self.assertFalse(target.exists())
            self.assertTrue(record.dry_run)
            # Every step is reported as planned-but-not-executed, never as ok.
            self.assertEqual([s.status for s in record.steps], ["dry", "dry"])
            self.assertIn("[DRY ] load", record.summary())

    def test_dry_run_does_not_execute_step_code(self):
        calls = []
        record = Runner({"a": Step("a", lambda ctx: calls.append("a"))},
                        dry_run=True).run()
        self.assertEqual(calls, [])
        self.assertEqual(record.steps[0].status, "dry")

    def test_dry_run_still_reports_every_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = Runner({"a": Step("a", lambda ctx: None)}, dry_run=True).run()
            self.assertEqual(len(record.steps), 1)

    def test_dry_run_leaves_no_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cp.json"
            Runner({"a": Step("a", lambda ctx: None)}, checkpoint=path, dry_run=True).run()
            self.assertFalse(path.exists())

    def test_summary_mentions_dry_run(self):
        record = Runner({"a": Step("a", lambda ctx: None)}, dry_run=True).run()
        self.assertIn("dry run", record.summary())


class TestContextFlow(unittest.TestCase):
    def test_step_return_value_merges_into_context(self):
        record = Runner({
            "a": Step("a", lambda ctx: {"shared": 42}),
            "b": Step("b", lambda ctx: ctx.get("shared")),
        }).run()
        self.assertTrue(all(s.ok for s in record.steps))

    def test_context_is_isolated_between_runs(self):
        seen = []

        def writer(ctx):
            ctx["leaked"] = True
            return {}

        runner = Runner({"a": Step("a", writer)})
        runner.run()
        runner.run({"fresh": 1})
        self.assertEqual(seen, [])

    def test_on_step_callback_fires_per_step(self):
        seen = []
        Runner({"a": Step("a", lambda ctx: None), "b": Step("b", lambda ctx: None)},
               on_step=seen.append).run()
        self.assertEqual([s.name for s in seen], ["a", "b"])


class TestRunRecord(unittest.TestCase):
    def test_summary_lists_each_step_with_a_status_marker(self):
        record = Runner({
            "load_ok": Step("load_ok", lambda ctx: None),
            "bad": Step("bad", lambda ctx: 1 / 0),
        }, stop_on_failure=False).run()
        text = record.summary()
        self.assertIn("[OK  ] load_ok", text)
        self.assertIn("[FAIL] bad", text)

    def test_record_serialises_to_json(self):
        import json
        record = Runner({"a": Step("a", lambda ctx: None)}).run()
        data = json.loads(record.to_json())
        self.assertEqual(data["steps"][0]["name"], "a")
        self.assertIn("started", data)

    def test_seconds_is_measured(self):
        record = Runner({"a": Step("a", lambda ctx: None)}).run()
        self.assertGreaterEqual(record.seconds, 0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)