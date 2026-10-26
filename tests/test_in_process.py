# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""The three ways an agent starts, with no broker: thingagent reaches the devices
directly and runs the gate itself, granting each agent and rule only what its
definition names."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from conftest import (
    AUDIENCE,
    BROKER,
    CTX,
    FLEET,
    ISSUER,
    LOCKED,
    NOSEC,
    SNAPSHOT,
    Rig,
    _clear_retained,
    _reachable,
    eventually,
    login,
)
from test_hero import Script, Stranger, _calls, _says, _to
from thingctx import ThingClient, TransportError
from thingctx.bindings import MqttTls

from thingagent import Agent, Rule, Step, checked
from thingagent.cli import Config, _generate, _serve_fleet, _verify, retained
from thingagent.harness import Harness
from thingagent.rules import binds_logins, client_as, probe_topics

PERSON_RULE = Rule.from_dict(
    {
        "name": "person_at_door",
        "event": "camera__person",
        "when": {"label": "person"},
        "calls": "doorwatch__decide",
    }
)


async def test_a_person_asks_in_process(in_process: Any) -> None:
    devices, make = in_process
    model = Script(
        _calls(
            ("door__locked__get", {}),
            ("thermostat__temperature__get", {}),
        ),
        _says("A courier is at the door."),
    )
    harness = await make(chat_fn=model)

    await harness.channel.receive("alice", "@doorwatch who is there?")
    told = await eventually(lambda: _to("alice", harness.channel.outbox), what="the answer")

    messages, _tools = model.shown[1]
    results = {m["name"]: m["content"] for m in messages if m["role"] == "tool"}
    assert results["door__locked__get"] == "false"
    # doorwatch reads only the door, so its view has no thermostat to read.
    assert "unknown" in results["thermostat__temperature__get"]
    assert {"type": "image_url", "image_url": {"url": SNAPSHOT}} in messages[1]["content"]
    assert told == ["A courier is at the door."]
    assert not devices.locked


async def test_an_event_starts_a_decision_in_process(in_process: Any) -> None:
    devices, make = in_process
    decider = Stranger()
    await make(decider=decider, rules=[PERSON_RULE])

    devices.camera.emit("person", {"label": "cat", "at": "t1"})
    devices.camera.emit("person", {"label": "person", "at": "t2"})
    await eventually(lambda: devices.sounded, what="the alarm")

    assert [context["trigger"] for context, _ in decider.asked] == [{"label": "person", "at": "t2"}]
    assert decider.asked[0][1] == SNAPSHOT
    assert devices.locked


PLANT = {
    "@context": CTX,
    "id": "urn:demo:plant",
    "title": "Plant",
    **NOSEC,
    "actions": {
        "open_valve": {
            "input": {"type": "object", "properties": {"valve": {"type": "integer"}}},
            "output": {"type": "object"},
            "forms": [{"href": "local://open_valve"}],
        }
    },
}
EXECUTOR = {
    "name": "executor",
    "questions": [{"type": "choice", "name": "verdict", "values": ["open", "hold"]}],
    "act": {"verdict": {"open": [{"invoke": "plant__open_valve", "args": {"valve": 3}}]}},
}


class Opens:
    """A decider that always answers open."""

    async def answer(self, agent: Agent, _context: Any, _image: Any) -> dict[str, Any]:
        return {
            "answers": [checked(q, {"choice": "open", "confidence": 0.9}) for q in agent.questions]
        }


async def test_an_executor_whose_table_names_valve_3_cannot_open_valve_9(in_process: Any) -> None:
    devices, make = in_process
    opened: list[Any] = []
    local = devices.binding()
    local.register_thing("plant", {"open_valve": lambda *a, **k: opened.append((a, k)) or {}})
    executor = Agent.from_dict(EXECUTOR)
    harness = await make(
        agents=[executor], fleet=[*FLEET, PLANT], bindings=[local], allow={}, decider=Opens()
    )

    decision = await harness.decide(executor, {})
    tried = await harness._run(executor, (Step("plant__open_valve", {"valve": 9}),))

    assert "result" in decision["acted"][0]
    assert len(opened) == 1
    assert "9" not in repr(opened)
    assert tried == [
        {
            "invoke": "plant__open_valve",
            "refused": {
                "error": "authorization denied",
                "thing": "urn:demo:plant",
                "affordance": "open_valve",
                "op": "invokeaction",
                "reason": "arguments outside the grant's limits for "
                "('urn:demo:plant', 'open_valve', 'invokeaction'): "
                "{'valve': 9} is not one of [{'valve': 3}]",
            },
        }
    ]


async def test_a_limit_refuses_a_valve_out_of_range_and_opens_one_in_range(
    in_process: Any,
) -> None:
    devices, make = in_process
    opened: list[Any] = []
    local = devices.binding()
    local.register_thing("plant", {"open_valve": lambda **k: opened.append(k) or {}})
    agent = Agent.from_dict(
        {
            "name": "valves",
            "may": ["plant__open_valve"],
            "limits": {
                "plant__open_valve": {"properties": {"valve": {"type": "integer", "maximum": 4}}}
            },
        }
    )
    harness = await make(agents=[agent], fleet=[*FLEET, PLANT], bindings=[local], allow={})

    refused = await harness._doers["valves"].invoke("plant__open_valve", {"valve": 9})
    done = await harness._doers["valves"].invoke("plant__open_valve", {"valve": 3})

    assert refused["reason"] == (
        "arguments outside the grant's limits for ('urn:demo:plant', 'open_valve', "
        "'invokeaction'): valve: 9 is greater than the maximum of 4"
    )
    assert done == {}
    assert opened == [{"valve": 3}]


async def test_a_grants_file_replaces_the_grants_the_definitions_give(
    in_process: Any, tmp_path: Path
) -> None:
    devices, make = in_process
    opened: list[Any] = []
    local = devices.binding()
    local.register_thing("plant", {"open_valve": lambda **k: opened.append(k) or {}})
    only_9 = ["urn:demo:plant", "open_valve", "invokeaction", {"enum": [{"valve": 9}]}]
    (tmp_path / "grants.json").write_text(json.dumps({"executor": [only_9]}))
    config = {"fleet": [], "agents": [], "grants": "grants.json"}
    (tmp_path / "thingagent.json").write_text(json.dumps(config))
    executor = Agent.from_dict(EXECUTOR)
    grants = Config(tmp_path / "thingagent.json").grants()
    harness = await make(
        agents=[executor], fleet=[*FLEET, PLANT], bindings=[local], allow={}, grants=grants
    )

    done = await harness._run(
        executor,
        (Step("plant__open_valve", {"valve": 9}), Step("plant__open_valve", {"valve": 3})),
    )

    assert "result" in done[0]
    assert "refused" in done[1]
    assert opened == [{"valve": 9}]


@pytest.mark.network
async def test_the_fleet_gateway_refuses_valve_9_and_traces_each_call(
    rig: Rig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = {"plant.json": PLANT, "executor.json": EXECUTOR, "jwks.json": rig.keys.jwks}
    files["thingagent.json"] = {"fleet": ["plant.json"], "agents": ["executor.json"]}
    files["gateway.json"] = {
        "broker": BROKER,
        "prefix": rig.prefix,
        "things": ["plant.json"],
        "login": {"user": "fleet_gw", "password_env": "FLEET_GW_PASSWORD"},
        "guard": {"issuers": [ISSUER], "audience": AUDIENCE, "jwks": "jwks.json"},
        "grants": "policy/grants.json",
        "handlers": {"plant": "valves.py:handlers"},
        "trace": "trace.jsonl",
    }
    for name, content in files.items():
        (tmp_path / name).write_text(json.dumps(content))
    (tmp_path / "valves.py").write_text(
        "from pathlib import Path\n\n"
        "def handlers(_tds):\n"
        "    def open_valve(valve):\n"
        "        with Path(__file__).with_name('opened').open('a') as log:\n"
        "            log.write(f'{valve}\\n')\n"
        "        return {}\n"
        "    return {'open_valve': open_valve}\n"
    )
    monkeypatch.setenv("FLEET_GW_PASSWORD", login("fleet_gw")[1])
    await _generate(Config(tmp_path / "thingagent.json"), tmp_path / "policy")
    gateway = asyncio.ensure_future(_serve_fleet(tmp_path / "gateway.json"))
    executor: ThingClient | None = None
    try:
        topic = f"{rig.prefix}/plant/td"
        while not (found := await retained(BROKER, topic, login=login("executor"), settle=0.3)):
            assert not gateway.done(), gateway.result()
        token = rig.keys.token("executor")
        executor = client_as([found[topic]], token, login("executor"), qos=1, timeout=10.0)

        refused = await executor.invoke("plant__open_valve", {"valve": 9})
        opened = await executor.invoke("plant__open_valve", {"valve": 3})
    finally:
        if executor is not None:
            await executor.aclose()
        gateway.cancel()
        await asyncio.gather(gateway, return_exceptions=True)
        _clear_retained(rig.prefix, ["plant"], "fleet_gw")

    assert refused == {
        "error": "authorization denied: arguments outside the grant's limits for "
        "('urn:demo:plant', 'open_valve', 'invokeaction'): "
        "{'valve': 9} is not one of [{'valve': 3}]",
        "denied": True,
    }
    assert opened == {}
    assert (tmp_path / "opened").read_text() == "3\n"
    records = [json.loads(line)["record"] for line in (tmp_path / "trace.jsonl").open()]
    assert [(r["schema:actionStatus"], r["prov:wasAssociatedWith"]) for r in records] == [
        ("schema:FailedActionStatus", "executor"),
        ("schema:CompletedActionStatus", "executor"),
    ]


class NoAnswerError(TransportError):
    def __init__(self) -> None:
        Exception.__init__(self, "no answer in 10s")

    def __str__(self) -> str:
        return "no answer in 10s"


async def test_a_lost_reply_is_logged_and_an_ended_stream_reopens(
    in_process: Any, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    _devices, make = in_process
    harness = await make()
    doorwatch = harness.agents["doorwatch"]
    monkeypatch.setattr("thingagent.harness.REOPEN", 0.01)
    seen: list[Any] = []
    monkeypatch.setattr(harness, "_on_event", lambda _rule, data: seen.append(data))

    async def no_answer(*_args: Any, **_kwargs: Any) -> Any:
        raise NoAnswerError

    async def ends() -> AsyncIterator[Any]:
        return
        yield

    async def fails() -> AsyncIterator[Any]:
        raise RuntimeError("broker gone")
        yield

    async def person() -> AsyncIterator[Any]:
        yield {"label": "person"}

    opens: list[Any] = [fails(), ConnectionError("no broker"), person()]

    async def events(_rule: Rule) -> AsyncIterator[Any]:
        opened = opens.pop(0)
        if isinstance(opened, Exception):
            raise opened
        return opened

    harness._actors["doorwatch"].invoke = no_answer  # type: ignore[method-assign]
    monkeypatch.setattr(harness, "_events", events)
    with caplog.at_level(logging.WARNING, logger="thingagent"):
        done = await harness._run(doorwatch, doorwatch.act["who"]["delivery"])
        follow = asyncio.ensure_future(harness._follow(PERSON_RULE, ends()))
        await eventually(lambda: seen, what="the event after the reopen")
        follow.cancel()

    assert done == [{"invoke": "door__lock", "failed": "no answer in 10s"}]
    assert seen == [{"label": "person"}]
    assert [r.getMessage() for r in caplog.records if r.name == "thingagent"][:6] == [
        "thingagent: doorwatch got no answer to door__lock: no answer in 10s",
        "thingagent: the stream of rule person_at_door ended; reopening it in 0.01 s",
        "thingagent: rule person_at_door reopened camera__person",
        "thingagent: the stream of rule person_at_door failed: broker gone; reopening it in 0.02 s",
        (
            "thingagent: rule person_at_door could not reopen camera__person: no broker;"
            " trying again in 0.04 s"
        ),
        "thingagent: rule person_at_door reopened camera__person",
    ]


async def test_a_model_reads_no_correlation_id(in_process: Any) -> None:
    _devices, make = in_process
    decider = Stranger()
    harness = await make(decider=decider)
    trigger = {"label": "person", "correlation": "c1", "seen": [{"correlation": "c0"}]}

    decision = await harness.decide(harness.agents["doorwatch"], trigger)

    assert decision["correlation"] == "c1"
    assert decision["trigger"] == trigger
    assert "correlation" not in json.dumps(decider.asked[0][0])


async def test_a_reopened_stream_uses_the_rule_token_as_it_is_then(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    made: list[Any] = []
    opened: list[str] = []

    class Source:
        def __init__(self, token: str, tls: Any) -> None:
            self.token, self.tls, self.closed = token, tls, False

        async def subscribe(self, _event: str) -> Source:
            return self

        async def aclose(self) -> None:
            self.closed = True

    def made_as(_tds: Any, token: str, _login: Any, **options: Any) -> Source:
        made.append(Source(token, options["tls"]))
        return made[-1]

    async def open_stream(_td: Any, _href: str, secrets: dict[str, Any], **_: Any) -> str:
        opened.append(secrets["bearer_sc"])
        return "stream"

    td = {"id": "urn:demo:camera", "securityDefinitions": {"bearer_sc": {"scheme": "bearer"}}}
    monkeypatch.setattr("thingagent.harness.client_as", made_as)
    monkeypatch.setattr("thingagent.harness.open_stream", open_stream)
    monkeypatch.setattr("thingagent.harness.stream_href", lambda _tds, _tool: (td, "href"))
    tokens = {"person_at_door": "old"}
    tls = MqttTls("ca.pem")
    harness = Harness(
        [], fleet=[], broker=BROKER, rules=[PERSON_RULE], tokens=tokens, tls=tls, timeout=0.01
    )
    try:
        await harness._events(PERSON_RULE)
        await harness._events(PERSON_RULE)
        tokens["person_at_door"] = "new"
        await harness._events(PERSON_RULE)
        # The old event client closes at once, its call client after a call's timeout.
        await eventually(lambda: made[1].closed, 1, what="the old call client to close")
    finally:
        await harness.aclose()

    assert opened == ["old", "old", "new"]
    assert [(m.token, m.closed) for m in made] == [
        ("old", True),
        ("old", True),
        ("new", True),
        ("new", True),
    ]
    assert {m.tls for m in made} == {tls}
    assert Harness([], fleet=[], broker="mqtts://broker.example")._tls == MqttTls()


async def test_a_harness_refuses_a_plain_broker_off_this_machine() -> None:
    harness = Harness([], fleet=[], broker="10.0.0.5:1883")
    with pytest.raises(ValueError, match=r"10.0.0.5:1883 is not on this machine and the link"):
        await harness.start()
    Harness([], fleet=[], broker="10.0.0.5:1883", insecure_bus=True)._reach_over_bus(
        "10.0.0.5:1883"
    )


async def test_retained_follows_an_mqtts_broker_onto_tls(monkeypatch: pytest.MonkeyPatch) -> None:
    applied: list[MqttTls] = []

    class Paho:
        def __init__(self, *_args: Any, **_options: Any) -> None:
            pass

        def __getattr__(self, _name: str) -> Any:
            return lambda *_args, **_options: None

    monkeypatch.setattr("paho.mqtt.client.Client", Paho)
    monkeypatch.setattr(MqttTls, "apply", lambda self, _client: applied.append(self))

    await retained("mqtts://broker.example", "tc/+/td", settle=0)

    assert applied == [MqttTls()]


async def test_a_rule_with_max_age_drops_a_stale_event_and_logs_it(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    rule = Rule.from_dict(
        {
            "name": "fresh_only",
            "event": "camera__person",
            "calls": "doorwatch__decide",
            "max_age": 60,
        }
    )
    harness = Harness([], fleet=[])
    started: list[Any] = []

    async def guarded(_handle: Any, data: Any) -> None:
        started.append(data)

    monkeypatch.setattr(harness, "_guarded", guarded)
    fresh = {"label": "person", "at": datetime.now(timezone.utc).isoformat()}
    with caplog.at_level(logging.WARNING, logger="thingagent"):
        harness._on_event(rule, {"label": "person", "at": "2026-10-13T08:00:00Z"})
        harness._on_event(rule, {"label": "person"})
        harness._on_event(rule, fresh)
        await eventually(lambda: started, 1, what="the fresh event")

    assert started == [fresh]
    dropped = [r.getMessage() for r in caplog.records if r.name == "thingagent"]
    assert len(dropped) == 2
    assert dropped[0].startswith("thingagent: rule fresh_only dropped an event of camera__person")
    assert dropped[1].endswith("it has no time with a zone at at")


async def test_verify_broker_names_each_login_that_publishes_under_another_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hrefs: list[str] = []

    class Probe:
        def __init__(self, tds: list[dict[str, Any]], bindings: list[dict[str, Any]]) -> None:
            self.user = bindings[0]["credentials"]["bus_basic"][0]
            self.tls = bindings[0]["tls"]
            hrefs.extend(a["forms"][0]["href"] for a in tds[0]["actions"].values())

        async def invoke(self, _tool: str, _args: dict[str, Any]) -> Any:
            assert self.tls == MqttTls("ca.pem")
            if self.user == "doorwatch":
                raise TransportError("PUBLISH", "t", status=0x87, detail="refused")
            if self.user == "advisor":
                raise TransportError("PUBLISH", "t", status=0x10, detail="no subscriber on t")
            if self.user == "rule_person":
                # Only the status proves a refusal, whatever the detail says.
                raise TransportError("PUBLISH", "t", detail="the broker refused the publish on t")
            if self.user == "stranger":
                raise TransportError("CONNECT", "t", detail="not authorized")
            return {}

        async def aclose(self) -> None:
            pass

    monkeypatch.setattr("thingagent.rules.MqttBinding", lambda **options: options)
    monkeypatch.setattr("thingagent.rules.ThingClient", Probe)
    logins = [("doorwatch", "p"), ("advisor", "p"), ("rule_person", "p")]
    tls = MqttTls("ca.pem")
    topic = ["tc/door/actions/lock"]

    published = await binds_logins("mqtts://broker.example", topic, logins, tls=tls)

    assert published == [
        f"the broker let {user} publish a request under another name on tc/door/actions/lock, "
        "so it does not bind each login to its name"
        for user in ("advisor", "rule_person")
    ]
    assert hrefs == [
        f"mqtts://broker.example:8883/tc/door/actions/lock/{user}"
        for user in ("advisor", "rule_person", "doorwatch")
    ]
    assert await binds_logins(BROKER, topic, logins[:1], tls=tls) == []
    unused = hrefs[-1].rsplit("/", 1)[-1]
    assert hrefs[-1] == f"mqtt://{BROKER}/tc/door/actions/lock/{unused}"
    assert unused.startswith("thingagent_probe_")
    with pytest.raises(SystemExit, match="let advisor publish a request under another name"):
        await _verify(BROKER, topic, logins, tls)
    with pytest.raises(SystemExit, match="verify_broker could not log in"):
        await _verify(BROKER, topic, [("stranger", "p")], tls)
    with pytest.raises(SystemExit, match="verify_broker has no login to check"):
        await _verify(BROKER, topic, [], tls)


def test_verify_broker_probes_every_request_topic_the_fleet_has() -> None:
    topics = probe_topics(FLEET, "tc")
    for td in FLEET:
        slug = td["id"].rsplit(":", 1)[-1]
        for name in td.get("actions", {}):
            assert f"tc/{slug}/actions/{name}" in topics
        for name in td.get("properties", {}):
            assert {f"tc/{slug}/props/{name}/{k}" for k in ("read", "write", "observe")} <= set(
                topics
            )
        for name in td.get("events", {}):
            assert f"tc/{slug}/events/{name}/subscribe" in topics
    (only,) = probe_topics([], "tc")
    assert only.startswith("tc/thingagent/actions/thingagent_probe_")


@pytest.mark.network
async def test_verify_broker_against_the_live_broker() -> None:
    host, _, port = BROKER.partition(":")
    if not _reachable(host, int(port or 1883)):
        pytest.skip(f"no MQTT broker at {BROKER}")

    published = await binds_logins(BROKER, ["tc/door/actions/lock"], [login("doorwatch")])

    # The compose broker takes any publish; a locked one refuses a name not the login's.
    if LOCKED:
        assert published == []
    else:
        [sentence] = published
        assert "let doorwatch publish a request under another name" in sentence
