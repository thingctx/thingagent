# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""The examples run as the README says: the quickstart with no broker and no key,
and the configurations pass ``thingagent check`` or ``generate``."""

from __future__ import annotations

import asyncio
import builtins
import importlib.util
import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from thingctx.authz import (
    AccessRequest,
    Decision,
    LocalPolicyGrantSource,
    PolicyDecisionPoint,
    build_vocabulary,
)
from thingctx.thing import fill_declared, parse_thing

from thingagent.cli import main
from thingagent.grants import loaded

EXAMPLES = Path(__file__).parent.parent / "examples"


def _quickstart(tmp_path: Path) -> Any:
    # A trace left by a run of the example in the checkout would count as this run's.
    shutil.copytree(
        EXAMPLES / "quickstart",
        tmp_path / "quickstart",
        ignore=shutil.ignore_patterns("trace.jsonl"),
    )
    spec = importlib.util.spec_from_file_location("quickstart", tmp_path / "quickstart" / "run.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("yes", [True, False])
async def test_the_quickstart_asks_before_it_unlocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], yes: bool
) -> None:
    run = _quickstart(tmp_path)
    asked: list[str] = []

    def reply(question: str) -> str:
        asked.append(question)
        return "yes" if yes else "no"

    monkeypatch.setattr(builtins, "input", reply)
    await run.main(None)

    out = capsys.readouterr().out
    assert len(asked) == 1
    assert asked[0].startswith("Unlock the front door?")
    assert out.endswith(f"The door is {'unlocked' if yes else 'locked'}.\n")
    trace = (tmp_path / "quickstart" / "trace.jsonl").read_text().splitlines()
    statuses = [
        json.loads(line)["record"].get("schema:actionStatus")
        for line in trace
        if "record" in json.loads(line)
    ]
    assert "schema:CompletedActionStatus" in statuses
    assert ("schema:FailedActionStatus" in statuses) is not yes


def test_the_quickstart_passes_check(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["check", str(EXAMPLES / "quickstart" / "thingagent.json")]) == 0
    assert capsys.readouterr().out == ""


def test_the_ur5e_owner_grants_the_visitor_no_motion(tmp_path: Path) -> None:
    ur5e = EXAMPLES / "ur5e"
    main(
        [
            "generate",
            str(ur5e / "operator.json"),
            "--gateway",
            str(ur5e / "gateway.json"),
            "--out",
            str(tmp_path),
        ]
    )
    given = json.loads((tmp_path / "grants.json").read_text())
    actions = {
        user: {grant[1] for grant in grants if grant[2] == "invokeaction"}
        for user, grants in given.items()
    }
    assert {"move_to", "grip", "release"} <= actions["picker"]
    assert actions["visitor"] == {"compute_ik", "list_controllers"}
    assert "picker" in (tmp_path / "acl").read_text()


def _move_to(td: dict[str, Any], x: float, y: float, z: float) -> AccessRequest:
    """A move of the cup to ``(x, y, z)``, as thingctx's decision point is asked it."""
    pose = {"position": {"x": x, "y": y, "z": z}}
    region = {"constraint_region": {"primitive_poses": [pose]}}
    goal = {"request": {"goal_constraints": [{"position_constraints": [region]}]}}
    filled = fill_declared(td["actions"]["move_to"]["input"], goal)
    return AccessRequest(td["id"], "move_to", "invokeaction", arguments=filled)


def test_the_ur5e_picker_moves_the_cup_only_over_the_bench_and_tray(tmp_path: Path) -> None:
    ur5e = EXAMPLES / "ur5e"
    gateway = ["--gateway", str(ur5e / "gateway.json")]
    assert main(["generate", str(ur5e / "operator.json"), *gateway, "--out", str(tmp_path)]) == 0
    td = json.loads((ur5e / "things" / "ur5e.td.json").read_text())
    pdp = PolicyDecisionPoint(
        vocabulary=build_vocabulary([parse_thing(td)]),
        grant_source=LocalPolicyGrantSource(
            loaded(json.loads((tmp_path / "grants.json").read_text())), claim="sub"
        ),
    )

    def decide(x: float, y: float, z: float) -> Decision:
        return asyncio.run(pdp.decide({"sub": "picker"}, _move_to(td, x, y, z)))

    bench = [(0.39, 0.26, 0.42), (0.50, 0.37, 0.58), (0.39, 0.37, 0.57)]
    tray = [(0.45, -0.07, 0.42), (0.45, -0.07, 0.58)]

    for x, y, z in [*bench, *tray]:
        assert decide(x, y, z).permit, (x, y, z)
    far, low = decide(0.9, 0.1, 0.5), decide(0.45, -0.07, 0.30)
    assert not far.permit
    assert far.reason.endswith("position.x: 0.9 is greater than the maximum of 0.65")
    assert not low.permit
