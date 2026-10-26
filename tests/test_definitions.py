# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from conftest import ADVISOR, CLIENT, DOORWATCH, FLEET, eventually
from thingctx.authz import LocalPolicyGrantSource, limited
from thingctx.bindings import MqttTls
from thingctx.validate import validate_td

from thingagent import Agent, ChatChannel, Question, Rule, check, checked, grants
from thingagent.check import plain_bus_risk
from thingagent.cli import Config, main, running
from thingagent.grants import loaded, saved
from thingagent.rules import broker_address


def test_an_agent_td_lists_every_answer_and_hides_the_table() -> None:
    td = Agent.from_dict(DOORWATCH).td()

    shapes = td["actions"]["decide"]["output"]["properties"]["answers"]["items"]["oneOf"]
    who = next(s for s in shapes if s["properties"]["type"]["const"] == "choice")
    assert who["properties"]["name"] == {"const": "who"}
    assert who["properties"]["choice"] == {"enum": ["resident", "delivery", "stranger", "unclear"]}
    assert set(td["actions"]) == {"decide", "ask", "approve"}
    assert td["events"]["decided"]["data"] == td["actions"]["decide"]["output"]
    assert "door__lock" not in json.dumps(td)
    assert validate_td(td) == []


def test_a_decision_matches_the_shape_its_td_declares() -> None:
    agent = Agent.from_dict(DOORWATCH)
    schema = agent.td()["actions"]["decide"]["output"]
    decision = {
        "agent": "doorwatch",
        "correlation": "c1",
        "trigger": {"label": "person"},
        "answers": [
            checked(agent.questions[0], {"choice": "stranger", "confidence": 0.9}),
            checked(agent.questions[0], {"choice": "burglar"}),
        ],
        "acted": [{"invoke": "door__lock", "result": {"locked": True}}],
        "at": "2026-10-13T08:00:00+00:00",
        "usage": {"input_tokens": 614, "output_tokens": 0},
        "cost": 0.0000614,
    }

    jsonschema.validate(decision, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(decision | {"answers": [{"type": "choice", "name": "who"}]}, schema)
    with pytest.raises(jsonschema.ValidationError):
        bad = {"type": "refusal", "name": "who", "reason": "bored"}
        jsonschema.validate(decision | {"answers": [bad]}, schema)
    advisor = Agent.from_dict(ADVISOR).td()
    jsonschema.validate(
        decision | {"agent": "advisor", "answers": []}, advisor["actions"]["decide"]["output"]
    )
    assert validate_td(advisor) == []


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"questions": [{"type": "guess", "name": "who"}]}, "unknown type"),
        ({"act": {"who": {"burglar": []}}}, "no question can answer"),
    ],
)
def test_a_definition_that_cannot_work_is_refused(change: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Agent.from_dict(DOORWATCH | change)


@pytest.mark.parametrize(
    ("raw", "answer"),
    [
        ({"choice": "stranger", "confidence": 0.8}, {"choice": "stranger", "confidence": 0.8}),
        ({"choice": "burglar"}, "not_a_choice"),
        ({}, "model_refused"),
        (None, "model_refused"),
    ],
)
def test_a_choice_outside_the_listed_values_is_a_refusal(
    raw: dict | None, answer: dict | str
) -> None:
    question = Question("choice", "who", "", ("resident", "stranger"))

    expected = (
        {"type": "refusal", "name": "who", "reason": answer}
        if isinstance(answer, str)
        else {"type": "choice", "name": "who", **answer}
    )
    assert checked(question, raw) == expected
    assert checked(question, None, "request_failed")["reason"] == "request_failed"


@pytest.mark.parametrize(("probability", "refused"), [(0.25, False), (1.5, True), ("0.5", True)])
def test_a_predicate_needs_a_probability(probability: object, refused: bool) -> None:
    answer = checked(Question("predicate", "open", ""), {"probability": probability})

    assert (answer.get("reason") == "not_a_choice") is refused


def test_check_passes_agents_that_match_the_fleet() -> None:
    rule = Rule.from_dict(
        {"name": "stranger", "event": "camera__person", "calls": "doorwatch__decide"}
    )
    agents = [Agent.from_dict(DOORWATCH), Agent.from_dict(ADVISOR)]

    assert check(agents, [rule], FLEET) == []


def test_check_names_each_problem() -> None:
    agent = Agent.from_dict(
        DOORWATCH
        | {"reads": ["door__colour"], "act": {"who": {"stranger": [{"invoke": "door__melt"}]}}}
    )
    rules = [
        Rule.from_dict({"name": "night", "event": "camera__wave", "calls": "advisor__decide"}),
        Rule.from_dict(
            {"name": "dawn", "event": "camera__person", "when": {"when": 1}}
            | {"calls": "doorwatch__decide"}
        ),
    ]

    assert check([agent], rules, FLEET) == [
        "doorwatch reads door__colour, which no fleet TD has",
        "doorwatch invokes door__melt, which no fleet TD has",
        "rule night calls advisor__decide, which is not an agent's decide, do or approve",
        (
            "rule night reads camera__wave, which no fleet TD has as an event or an "
            "observable property"
        ),
        "rule dawn reads when from camera__person, which its data lacks",
    ]


def test_check_refuses_a_name_two_definitions_share() -> None:
    agents = [Agent.from_dict(DOORWATCH), Agent.from_dict(ADVISOR)]
    twin = Rule.from_dict(
        {"name": "doorwatch", "event": "camera__person", "calls": "doorwatch__decide"}
    )

    assert check(agents, [twin], FLEET) == [
        "doorwatch names more than one agent, client or rule; each needs a name of its own"
    ]


def test_check_refuses_a_client_that_thinks_or_names_what_no_td_has() -> None:
    clients = [
        Agent.from_dict(CLIENT),
        Agent.from_dict({"name": "doorwatch"}),
        Agent.from_dict(DOORWATCH | {"name": "smart", "model": "openai/gpt-5"}),
        Agent.from_dict({"name": "odd", "reads": ["door__colour"], "may": ["door__melt"]}),
    ]

    assert check([Agent.from_dict(DOORWATCH)], [], FLEET, clients=clients) == [
        "doorwatch names more than one agent, client or rule; each needs a name of its own",
        "client smart has model, act, questions, but a client only reads and takes actions",
        "odd reads door__colour, which no fleet TD has",
        "odd invokes door__melt, which no fleet TD has",
    ]


def test_check_flags_an_mqtt_action_nothing_confirms() -> None:
    fleet = json.loads(json.dumps(FLEET))
    lock = fleet[0]["actions"]["lock"]
    del lock["output"]
    lock["forms"] = [{"href": "mqtt://broker/tc/door/actions/lock"}]

    unconfirmed = [
        (
            "doorwatch invokes door__lock, which declares no output, "
            "so over MQTT nothing confirms it ran or was refused"
        )
    ]
    agents = [Agent.from_dict(DOORWATCH)]
    assert check(agents, [], fleet) == unconfirmed
    assert check(agents, [], fleet, tokens={"doorwatch"}) == unconfirmed


def test_a_call_that_carries_a_token_needs_no_declared_output() -> None:
    fleet = json.loads(json.dumps(FLEET))
    door = fleet[0]
    door["securityDefinitions"] = {
        "bus_bearer": {"scheme": "bearer", "in": "header", "name": "authorization"}
    }
    door["security"] = ["bus_bearer"]
    del door["actions"]["lock"]["output"]
    door["actions"]["lock"]["forms"] = [{"href": "mqtt://broker/tc/door/actions/lock"}]
    agents = [Agent.from_dict(DOORWATCH)]

    assert check(agents, [], fleet, tokens={"doorwatch"}) == []
    assert len(check(agents, [], fleet)) == 1


def test_a_rule_that_approves_names_the_action() -> None:
    knob = {"name": "knob", "event": "lamp__knob", "calls": "keeper__approve"}
    with pytest.raises(ValueError, match="rule knob calls keeper__approve without the action"):
        Rule.from_dict(knob | {"args": {"person": "alice", "yes": True}})
    named = Rule.from_dict(
        knob | {"args": {"person": "alice", "yes": True, "action": "door__unlock"}}
    )
    assert named.args["action"] == "door__unlock"


def test_a_rule_with_max_age_drops_an_old_or_undated_event() -> None:
    now = datetime(2026, 10, 13, 8, 0, 30, tzinfo=timezone.utc)
    rule = Rule.from_dict(PERSON_RULE | {"max_age": 10})
    timed = Rule.from_dict(PERSON_RULE | {"max_age": 10, "time": "seen.when"})

    assert rule.stale({"at": "2026-10-13T08:00:25Z"}, now) is None
    assert rule.stale({"at": "2026-10-13T10:00:25+02:00"}, now) is None
    assert rule.stale({"at": now.timestamp() - 5}, now) is None
    assert rule.stale({"at": "2026-10-13T08:00:00Z"}, now) == (
        "it is 30 s old, more than its max_age of 10 s"
    )
    undated = ({}, {"at": "yesterday"}, {"at": "2026-10-13T08:00:25"}, {"at": True})
    for unreadable in (*undated, {"at": 1.7e12}, {"at": float("nan")}):
        assert rule.stale(unreadable, now) == "it has no time with a zone at at"
    assert timed.stale({"seen": {"when": "2026-10-13T08:00:25Z"}}, now) is None
    assert timed.stale({"at": "2026-10-13T08:00:25Z"}, now) is not None
    assert Rule.from_dict(PERSON_RULE).stale({}, now) is None
    for bad in (0, -1, "10", True):
        with pytest.raises(ValueError, match="max_age that is not a positive number"):
            Rule.from_dict(PERSON_RULE | {"max_age": bad})


@pytest.mark.parametrize("name", ["my-rule", "1st", 'x"y'])
def test_a_rule_name_that_is_not_letters_digits_and_underscores_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match="not a rule name"):
        Rule.from_dict({"name": name, "event": "", "calls": ""})


async def test_only_a_yes_in_time_confirms() -> None:
    channel = ChatChannel()

    async def answer(text: str) -> bool:
        pending = asyncio.ensure_future(channel.confirm("alice", "Lock the door?", 1.0))
        await asyncio.sleep(0)
        await channel.receive("alice", text)
        return await pending

    assert await answer("Yes") is True
    assert await answer("maybe") is False
    assert await channel.confirm("alice", "Still there?", 0.05) is False
    assert [m["text"] for m in channel.outbox] == ["Lock the door?"] * 2 + ["Still there?"]


async def test_a_second_question_to_one_person_waits_for_the_first() -> None:
    channel = ChatChannel()
    first = asyncio.ensure_future(channel.confirm("alice", "Lock the door?", 1.0))
    second = asyncio.ensure_future(channel.confirm("alice", "Sound the alarm?", 1.0))
    await eventually(lambda: channel.outbox, 1)
    assert [m["text"] for m in channel.outbox] == ["Lock the door?"]

    await channel.receive("alice", "yes")
    assert await first is True
    await eventually(lambda: len(channel.outbox) == 2, 1)
    await channel.receive("alice", "no")

    assert await second is False
    assert [m["text"] for m in channel.outbox] == ["Lock the door?", "Sound the alarm?"]


async def test_a_question_that_times_out_leaves_the_next_one_open() -> None:
    channel = ChatChannel()
    first = asyncio.ensure_future(channel.confirm("alice", "Lock the door?", 0.05))
    second = asyncio.ensure_future(channel.confirm("alice", "Sound the alarm?", 1.0))

    assert await first is False
    await eventually(lambda: len(channel.outbox) == 2, 1)
    await channel.receive("alice", "yes")

    assert await second is True


async def test_an_answer_about_another_action_leaves_the_question_open() -> None:
    channel = ChatChannel()
    first = asyncio.ensure_future(channel.confirm("alice", "Unlock?", 1.0, "door__unlock"))
    await eventually(lambda: channel.outbox, 1)

    refused = await channel.answer("alice", True, "alarm__sound")

    assert refused == {
        "answered": False,
        "refused": "alice's open question is not about alarm__sound",
    }
    assert not first.done()
    assert await channel.answer("alice", True, "door__unlock") == {"answered": True}
    assert await first is True
    second = asyncio.ensure_future(channel.confirm("alice", "Unlock?", 1.0, "door__unlock"))
    await eventually(lambda: len(channel.outbox) == 2, 1)
    await channel.receive("alice", "yes")
    assert await second is True


def test_the_command_checks_a_configuration(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "fleet").mkdir()
    for td in FLEET:
        (tmp_path / "fleet" / f"{td['id'].rsplit(':', 1)[-1]}.json").write_text(json.dumps(td))
    (tmp_path / "doorwatch.json").write_text(json.dumps(DOORWATCH))
    (tmp_path / "broken.json").write_text(json.dumps(DOORWATCH | {"reads": ["door__colour"]}))
    login = {"user": "doorwatch", "password_env": "UNUSED"}
    config = {
        "broker": "127.0.0.1:1883",
        "broker_binds_identity": True,
        "logins": {"doorwatch": login},
        "guard": {"issuers": ["https://issuer.test"], "audience": "api://fleet", "jwks": "k"},
        "policy": {},
        "fleet": "fleet",
        "agents": ["doorwatch.json"],
    }
    (tmp_path / "good.json").write_text(json.dumps(config))
    (tmp_path / "bad.json").write_text(json.dumps(config | {"agents": ["broken.json"]}))
    open_bus = {k: v for k, v in config.items() if k != "broker_binds_identity"}
    (tmp_path / "open.json").write_text(json.dumps(open_bus))
    (tmp_path / "opted.json").write_text(json.dumps(open_bus | {"open_bus": True}))

    assert main(["check", str(tmp_path / "good.json")]) == 0
    assert main(["check", str(tmp_path / "opted.json")]) == 0
    assert main(["check", str(tmp_path / "bad.json")]) == 1
    assert capsys.readouterr().out == "doorwatch reads door__colour, which no fleet TD has\n"
    assert main(["check", str(tmp_path / "open.json")]) == 1
    assert "doorwatch can act on the fleet, but the broker" in capsys.readouterr().out
    (tmp_path / "unguarded.json").write_text(json.dumps(config | {"guard": None}))
    assert main(["check", str(tmp_path / "unguarded.json")]) == 1
    assert "the agents' gateway has no guard, so" in capsys.readouterr().out


@pytest.mark.parametrize("value", ["not a name", "1ST", "hunter2!", 7])
def test_a_secret_key_that_names_no_variable_is_refused_without_its_value(
    tmp_path: Path, value: Any
) -> None:
    path = tmp_path / "thingagent.json"
    path.write_text(json.dumps({"login": {"user": "agents_gw", "password_env": value}}))
    with pytest.raises(SystemExit) as refused:
        Config(path)
    assert "login.password_env must name an environment variable" in str(refused.value)
    assert str(value) not in str(refused.value)
    path.write_text(json.dumps({"token_env": {"doorwatch": value}}))
    with pytest.raises(SystemExit, match=r"token_env.doorwatch must name"):
        Config(path)


def test_an_unset_secret_names_its_key_not_its_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DOORWATCH_PASSWORD", raising=False)
    monkeypatch.setenv("DOORWATCH_TOKEN", "t1")
    path = tmp_path / "thingagent.json"
    path.write_text(
        json.dumps(
            {
                "login": {"user": "agents_gw", "password_env": "DOORWATCH_PASSWORD"},
                "logins": {
                    "doorwatch": {"user": "doorwatch", "password_env": "DOORWATCH_PASSWORD"}
                },
                "token_env": {"doorwatch": "DOORWATCH_TOKEN"},
            }
        )
    )
    config = Config(path)

    with pytest.raises(
        SystemExit,
        match=r"^thingagent: set the environment variable that login.password_env names$",
    ):
        config.login()
    with pytest.raises(SystemExit, match=r"that logins.doorwatch.password_env names$"):
        config.logins()
    assert config.tokens() == {"doorwatch": "t1"}
    monkeypatch.delenv("DOORWATCH_TOKEN")
    with pytest.raises(SystemExit, match=r"that token_env.doorwatch names$"):
        config.tokens()


@pytest.mark.parametrize(
    ("broker", "address"),
    [
        ("127.0.0.1:1884", ("127.0.0.1", 1884, False)),
        ("broker.example", ("broker.example", 1883, False)),
        ("mqtt://broker.example:1885", ("broker.example", 1885, False)),
        ("mqtts://broker.example", ("broker.example", 8883, True)),
        ("mqtts://[::1]:8884", ("::1", 8884, True)),
    ],
)
def test_a_broker_is_host_port_or_an_mqtt_or_mqtts_url(
    broker: str, address: tuple[str, int, bool]
) -> None:
    assert broker_address(broker) == address


def test_a_broker_off_this_machine_needs_tls_or_an_opt_in(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    clear = "is not on this machine and the link has no TLS"
    for here in ("127.0.0.1:1883", "127.8.0.1:1883", "localhost", "mqtt://[::1]:1883"):
        assert plain_bus_risk(here, tls=False, insecure_bus=False) is None
    risk = plain_bus_risk("10.0.0.5:1883", tls=False, insecure_bus=False)
    assert risk is not None and clear in risk
    assert plain_bus_risk("10.0.0.5:1883", tls=True, insecure_bus=False) is None
    assert plain_bus_risk("mqtts://broker.example", tls=False, insecure_bus=False) is None
    assert plain_bus_risk("10.0.0.5:1883", tls=False, insecure_bus=True) is None
    assert check([], [], FLEET, broker="10.0.0.5:1883") == [risk]

    (tmp_path / "fleet").mkdir()
    for td in FLEET:
        (tmp_path / "fleet" / f"{td['id'].rsplit(':', 1)[-1]}.json").write_text(json.dumps(td))
    (tmp_path / "advisor.json").write_text(json.dumps(ADVISOR))
    config = {"broker": "10.0.0.5:1883", "fleet": "fleet", "agents": ["advisor.json"]}
    for name, entries in {
        "plain": {},
        "opted": {"insecure_bus": True},
        "tls": {"tls": {"ca": "ca.pem"}},
        "mqtts": {"broker": "mqtts://10.0.0.5"},
    }.items():
        (tmp_path / f"{name}.json").write_text(json.dumps(config | entries))
    assert main(["check", str(tmp_path / "plain.json")]) == 1
    assert clear in capsys.readouterr().out
    for name in ("opted", "tls", "mqtts"):
        assert main(["check", str(tmp_path / f"{name}.json")]) == 0, name
    # A fleet read from the bus is refused before anything connects.
    (tmp_path / "bus.json").write_text(json.dumps(config | {"fleet": "bus"}))
    with pytest.raises(SystemExit, match=clear):
        asyncio.run(Config(tmp_path / "bus.json").fleet())


def test_tls_files_are_relative_to_the_config(tmp_path: Path) -> None:
    path = tmp_path / "thingagent.json"
    entry = {"ca": "certs/ca.pem", "cert": "certs/me.pem", "key": "certs/me.key"}
    path.write_text(json.dumps({"broker": "10.0.0.5:8883", "tls": entry}))
    tls = Config(path).tls()
    assert tls == MqttTls(*(str(tmp_path / f) for f in entry.values()))
    path.write_text(json.dumps({"broker": "mqtts://10.0.0.5"}))
    assert Config(path).tls() == MqttTls()
    path.write_text(json.dumps({"broker": "127.0.0.1:1883"}))
    assert Config(path).tls() is None
    path.write_text(json.dumps({"broker": "127.0.0.1:1883", "tls": {"password": "x"}}))
    with pytest.raises(SystemExit, match="tls takes only ca, cert and key"):
        Config(path).tls()


def test_a_fleet_can_come_from_a_thing_directory(tmp_path: Path) -> None:
    class Directory(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200 if self.path == "/things" else 404)
            self.end_headers()
            self.wfile.write(json.dumps(FLEET).encode())

    server = HTTPServer(("127.0.0.1", 0), Directory)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    path = tmp_path / "thingagent.json"
    path.write_text(json.dumps({"fleet": f"tdd:http://127.0.0.1:{server.server_port}"}))
    try:
        assert asyncio.run(Config(path).fleet()) == FLEET
    finally:
        server.shutdown()


def test_an_agent_that_can_act_needs_a_bus_that_binds_its_login_or_an_opt_in() -> None:
    agents = [Agent.from_dict(DOORWATCH), Agent.from_dict(ADVISOR)]
    bus = {"broker": "127.0.0.1:1883", "guard": True, "policy": True}
    replay = "so anyone on the bus can replay a token read there or publish its events"
    bound = {"broker_binds_identity": True, "logins": {"doorwatch"}}

    unbound = check(agents, [], FLEET, **bus)
    no_login = check(agents, [], FLEET, **bus, broker_binds_identity=True)

    assert unbound == [
        (
            "doorwatch can act on the fleet, but the broker is not set to bind each login to "
            f"its identity, {replay}; set broker_binds_identity to true and give it a login, "
            'or set "open_bus": true to accept that risk'
        )
    ]
    assert no_login[0].startswith(f"doorwatch can act on the fleet, but it has no login, {replay}")
    assert len(no_login) == 1
    assert check(agents, [], FLEET, **bus, **bound) == []
    unguarded = bus | {"guard": False, "policy": False}
    assert check(agents, [], FLEET, **unguarded, **bound) == [
        (
            "doorwatch can act on the fleet, but the agents' gateway has no guard and no "
            'policy, so any login on the bus may call it; set guard and policy, or set "open_bus": '
            "true to accept that risk"
        )
    ]
    assert check(agents, [], FLEET, **bus, open_bus=True) == []
    assert check(agents, [], FLEET) == []


PLANT = {
    "@context": "https://www.w3.org/2022/wot/td/v1.1",
    "id": "urn:demo:plant",
    "actions": {
        "open_valve": {"input": {"type": "object", "properties": {"valve": {"type": "integer"}}}},
        "set_mode": {"input": {"type": "object", "properties": {"mode": {"type": "string"}}}},
    },
}


def test_a_table_fixes_the_arguments_of_an_action_only_it_takes() -> None:
    agent = Agent.from_dict(
        {
            "name": "executor",
            "questions": [{"type": "choice", "name": "run", "values": ["a", "b"]}],
            "act": {
                "run": {
                    "a": [{"invoke": "plant__open_valve", "args": {"valve": 3}}],
                    "b": [
                        {"invoke": "plant__open_valve", "args": {"valve": 3}},
                        {"invoke": "plant__open_valve", "args": {"valve": 4}},
                        {"invoke": "plant__set_mode", "args": {"mode": "auto"}},
                    ],
                }
            },
            "may": ["plant__set_mode"],
        }
    )

    assert grants([agent], [], [PLANT]) == {
        "executor": {
            limited(
                "urn:demo:plant",
                "open_valve",
                "invokeaction",
                {"enum": [{"valve": 3}, {"valve": 4}]},
            ),
            ("urn:demo:plant", "set_mode", "invokeaction"),
        }
    }


def test_a_limit_bounds_every_call_its_action_makes_steps_included() -> None:
    agent = Agent.from_dict(
        {
            "name": "executor",
            "questions": [{"type": "choice", "name": "run", "values": ["a"]}],
            "act": {
                "run": {
                    "a": [
                        {"invoke": "plant__set_mode", "args": {"mode": "auto"}},
                        {"invoke": "plant__open_valve", "args": {"valve": 2}},
                    ]
                }
            },
            "may": ["plant__set_mode"],
            "limits": {
                "plant__set_mode": {"properties": {"mode": {"enum": ["manual"]}}},
                "plant__open_valve": {"properties": {"valve": {"type": "integer", "maximum": 4}}},
            },
        }
    )

    # An action the agent may take is bounded by its limit alone, so its step's
    # "auto" is refused; one only steps take is bounded by the steps and the limit.
    assert grants([agent], [], [PLANT])["executor"] == {
        limited(
            "urn:demo:plant",
            "set_mode",
            "invokeaction",
            {"properties": {"mode": {"enum": ["manual"]}}},
        ),
        limited(
            "urn:demo:plant",
            "open_valve",
            "invokeaction",
            {
                "allOf": [
                    {"properties": {"valve": {"type": "integer", "maximum": 4}}},
                    {"enum": [{"valve": 2}]},
                ]
            },
        ),
    }


def test_limits_on_an_action_the_agent_never_takes_are_refused() -> None:
    with pytest.raises(ValueError, match="limits names door__unlock, which the agent may not take"):
        Agent.from_dict(DOORWATCH | {"limits": {"door__unlock": {}}})
    assert Agent.from_dict(DOORWATCH | {"limits": {"door__lock": {}}}).limits == {"door__lock": {}}


def test_a_numeric_bound_without_a_type_is_refused_at_load() -> None:
    untyped = {"door__lock": {"properties": {"force": {"maximum": 4}}}}
    with pytest.raises(ValueError, match=r"agent doorwatch, limits door__lock: .*without a 'type'"):
        Agent.from_dict(DOORWATCH | {"limits": untyped})


PERSON_RULE = {
    "name": "person_at_door",
    "event": "camera__person",
    "when": {"label": "person"},
    "calls": "doorwatch__decide",
}


def test_a_simple_rule_matches_the_values_it_names() -> None:
    rule = Rule.from_dict(PERSON_RULE)

    assert rule.event == "camera__person"
    assert rule.matches({"label": "person", "at": "t"})
    assert not rule.matches({"label": "cat"})
    assert not rule.matches("person")


def test_a_dotted_when_reaches_an_answer_in_a_decided_event() -> None:
    rule = Rule.from_dict(
        {"name": "escalate", "event": "safety__decided", "when": {"answers.air": "reject"}}
        | {"calls": "chair__decide"}
    )
    decided = {
        "answers": [
            {"type": "predicate", "name": "ok", "probability": 0.2},
            {"type": "choice", "name": "air", "choice": "reject"},
        ]
    }

    assert rule.matches(decided)
    assert not rule.matches({"answers": [{"type": "choice", "name": "air", "choice": "pass"}]})
    assert not rule.matches({"answers": [{"type": "refusal", "name": "air"}]})
    assert not rule.matches({"answers": "reject"})
    assert Rule.from_dict(PERSON_RULE | {"when": {"answers.ok": False}}).matches(decided)
    with pytest.raises(ValueError, match="is not a rule name"):
        Rule.from_dict(PERSON_RULE | {"when": {"answers..air": "reject"}})


def test_check_reads_the_first_step_of_a_dotted_when() -> None:
    agents = [Agent.from_dict(DOORWATCH)]
    dotted = Rule.from_dict(PERSON_RULE | {"when": {"label.kind": "person"}})
    odd = Rule.from_dict(PERSON_RULE | {"name": "odd", "when": {"colour.hue": "red"}})

    assert check(agents, [dotted], FLEET) == []
    assert check(agents, [odd], FLEET) == [
        "rule odd reads colour from camera__person, which its data lacks"
    ]


def test_grants_are_exactly_what_each_agent_and_rule_names() -> None:
    agents = [Agent.from_dict(DOORWATCH), Agent.from_dict(ADVISOR)]

    given = grants(agents, [Rule.from_dict(PERSON_RULE)], FLEET)

    assert given == {
        "doorwatch": {
            ("urn:demo:door", "locked", "readproperty"),
            ("urn:demo:camera", "snapshot", "readproperty"),
            limited("urn:demo:door", "lock", "invokeaction", {"enum": [{}]}),
            limited("urn:demo:alarm", "sound", "invokeaction", {"enum": [{}]}),
        },
        "advisor": {("urn:demo:thermostat", "temperature", "readproperty")},
        "person_at_door": {("urn:demo:camera", "person", "subscribeevent")},
    }


def _acl_rules(text: str) -> dict[str, set[tuple[str, str]]]:
    rules: dict[str, set[tuple[str, str]]] = {}
    user = ""
    for line in text.splitlines():
        if line.startswith("user "):
            user = line.removeprefix("user ")
            rules.setdefault(user, set())
        elif line.startswith("topic "):
            _, access, topic = line.split(" ", 2)
            rules[user].add((access, topic))
    return rules


def _project(tmp_path: Path, **entries: Any) -> Path:
    """A configuration of the test fleet, both agents and the person rule, with
    ``entries`` added."""
    (tmp_path / "fleet").mkdir()
    for td in FLEET:
        (tmp_path / "fleet" / f"{td['id'].rsplit(':', 1)[-1]}.json").write_text(json.dumps(td))
    (tmp_path / "doorwatch.json").write_text(json.dumps(DOORWATCH))
    (tmp_path / "advisor.json").write_text(json.dumps(ADVISOR))
    (tmp_path / "person.json").write_text(json.dumps(PERSON_RULE))
    config = {
        "broker": "127.0.0.1:1883",
        "fleet": "fleet",
        "agents": ["doorwatch.json", "advisor.json"],
        "rules": ["person.json"],
        **entries,
    }
    (tmp_path / "thingagent.json").write_text(json.dumps(config))
    return tmp_path / "thingagent.json"


def _generated(tmp_path: Path, config: Path) -> dict[str, set[tuple[str, str]]]:
    assert main(["generate", str(config), "--out", str(tmp_path / "out")]) == 0
    return _acl_rules((tmp_path / "out" / "acl").read_text())


def _serving(rules: dict[str, set[tuple[str, str]]], user: str) -> set[str]:
    """The Things whose action requests ``user`` may read."""
    return {t.split("/")[1] for a, t in rules[user] if a == "read" and t.endswith("/actions/+/+")}


def test_generate_writes_the_gateway_grants_and_the_broker_rules(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = _project(
        tmp_path,
        logins={"person_at_door": {"user": "rule_person", "password_env": "UNUSED"}},
        login={"user": "agents_gw", "password_env": "UNUSED"},
        gateways=["fleet_gw"],
        callers=["chat"],
    )

    rules = _generated(tmp_path, config)

    grants_file = json.loads((tmp_path / "out" / "grants.json").read_text())
    assert grants_file == {
        "doorwatch": [
            ["urn:demo:alarm", "sound", "invokeaction", {"enum": [{}]}],
            ["urn:demo:camera", "snapshot", "readproperty"],
            ["urn:demo:door", "lock", "invokeaction", {"enum": [{}]}],
            ["urn:demo:door", "locked", "readproperty"],
        ],
        "advisor": [["urn:demo:thermostat", "temperature", "readproperty"]],
        "rule_person": [["urn:demo:camera", "person", "subscribeevent"]],
    }
    given = loaded(grants_file)
    source = LocalPolicyGrantSource(given, claim="sub")
    assert limited("urn:demo:door", "lock", "invokeaction", {"enum": [{}]}) in given["doorwatch"]
    assert saved(given) == grants_file
    assert asyncio.run(source.grant_for({"sub": "advisor"})) == {
        ("urn:demo:thermostat", "temperature", "readproperty")
    }
    writes = {user: {t for a, t in r if a == "write"} for user, r in rules.items()}
    assert writes["doorwatch"] == {
        "tc/alarm/actions/sound/doorwatch",
        "tc/camera/props/snapshot/read/doorwatch",
        "tc/door/actions/lock/doorwatch",
        "tc/door/props/locked/read/doorwatch",
    }
    assert writes["advisor"] == {"tc/thermostat/props/temperature/read/advisor"}
    assert writes["rule_person"] == {
        "tc/camera/events/person/subscribe/rule_person",
        "tc/doorwatch/actions/decide/rule_person",
    }
    assert ("read", "tc/camera/events/person/subscribe/rule_person/stream") in rules["rule_person"]
    assert _serving(rules, "fleet_gw") == {"door", "camera", "alarm", "thermostat"}
    assert _serving(rules, "agents_gw") == {
        "doorwatch",
        "advisor",
        "chat_agents_gw",
    }
    assert ("read", "tc/+/td") in rules["agents_gw"]
    assert ("write", "tc/+/actions/+/chat") in rules["chat"]
    assert capsys.readouterr().out.count("wrote ") == 2


def test_generate_grants_a_client_what_it_reads_and_may_take(tmp_path: Path) -> None:
    config = _project(
        tmp_path,
        clients=[CLIENT],
        logins={"claude": {"user": "claude_user", "password_env": "UNUSED"}},
    )

    rules = _generated(tmp_path, config)

    grants_file = json.loads((tmp_path / "out" / "grants.json").read_text())
    assert grants_file["claude_user"] == [
        ["urn:demo:door", "lock", "invokeaction"],
        ["urn:demo:thermostat", "temperature", "readproperty"],
    ]
    assert {t for a, t in rules["claude_user"] if a == "write"} == {
        "tc/door/actions/lock/claude_user",
        "tc/thermostat/props/temperature/read/claude_user",
    }


async def test_serve_runs_the_agents_but_not_the_clients(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CLAUDE_TOKEN", raising=False)
    config = _project(
        tmp_path, broker=None, rules=[], clients=[CLIENT], token_env={"claude": "CLAUDE_TOKEN"}
    )

    async with running(Config(config)) as harness:
        assert set(harness.agents) == {"doorwatch", "advisor"}
        assert "urn:thingagent:agent:claude" not in {td["id"] for td in harness.own_tds()}


def test_generate_lets_a_rule_call_its_own_harness_over_the_bus(tmp_path: Path) -> None:
    rules = _generated(tmp_path, _project(tmp_path))

    assert ("write", "tc/doorwatch/actions/decide/person_at_door") in rules["person_at_door"]


def test_generate_limits_each_listed_gateway_to_its_own_things(tmp_path: Path) -> None:
    config = _project(
        tmp_path, gateways={"door_gw": ["urn:demo:door"], "cam_gw": ["urn:demo:camera"]}
    )

    rules = _generated(tmp_path, config)

    assert _serving(rules, "door_gw") == {"door"}
    assert _serving(rules, "cam_gw") == {"camera"}
    assert ("write", "tc/door/td") in rules["door_gw"]
    assert not any("/camera/" in t for _, t in rules["door_gw"])
