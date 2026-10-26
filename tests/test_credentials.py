# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Where an agent's and a rule's secrets may go: only to the scheme of their own
kind, and only to thingagent's own broker. Each refusal comes before anything
connects."""

from __future__ import annotations

from typing import Any

import pytest
from conftest import DOORWATCH
from thingctx.thing import MQTT_USER_NAME

from thingagent import Agent, Harness, Rule
from thingagent.cli import not_in_fleet
from thingagent.rules import credentials, other_brokers

BROKER = "127.0.0.1:1884"
GUARDED: dict[str, Any] = {
    "@context": "https://www.w3.org/2022/wot/td/v1.1",
    "securityDefinitions": {
        "bus_basic": {"scheme": "basic"},
        "bus_bearer": {"scheme": "bearer", "in": "header", "name": "authorization"},
    },
    "security": ["bus_basic", "bus_bearer"],
    "uriVariables": {"mqttUserName": {"@type": MQTT_USER_NAME, "type": "string"}},
}
PERSON = {"type": "object", "properties": {"label": {"type": "string"}}}
STREAM = f"mqtt://{BROKER}/tc/camera/events/person/subscribe/{{+mqttUserName}}/stream"
CAMERA = GUARDED | {
    "id": "urn:demo:camera",
    "title": "Door camera",
    "events": {"person": {"data": PERSON, "forms": [{"href": STREAM}]}},
}
RULE = Rule.from_dict(
    {"name": "person_at_door", "event": "camera__person", "calls": "doorwatch__decide"}
)


def _camera_at(href: str) -> dict[str, Any]:
    return CAMERA | {"events": {"person": {"data": PERSON, "forms": [{"href": href}]}}}


def test_each_secret_goes_only_to_a_scheme_of_its_own_kind() -> None:
    assert credentials([CAMERA], "TOKEN", ("person_at_door", "pw")) == {
        "bus_basic": ("person_at_door", "pw"),
        "bus_bearer": "TOKEN",
    }


def test_one_scheme_name_given_two_kinds_is_refused() -> None:
    clash = CAMERA | {
        "id": "urn:demo:other",
        "securityDefinitions": {"bus_basic": {"scheme": "bearer"}},
    }
    with pytest.raises(ValueError, match="'bus_basic' as both basic and bearer"):
        credentials([CAMERA, clash], "TOKEN", ("person_at_door", "pw"))


def test_a_form_that_names_another_broker_is_found() -> None:
    evil = "mqtt://evil.example:1883/tc/camera/events/person"
    port = "mqtt://127.0.0.1:1999/tc/camera/events/person"
    assert other_brokers([CAMERA], BROKER) == []
    assert other_brokers([_camera_at("mqtt://127.0.0.1/tc/x")], "127.0.0.1:1883") == []
    assert other_brokers([_camera_at(evil), _camera_at(port)], BROKER) == [evil, port]


async def test_thingagent_logs_in_to_no_broker_but_its_own() -> None:
    harness = Harness(
        [Agent.from_dict(DOORWATCH)],
        fleet=[_camera_at("mqtt://evil.example:1883/tc/camera/events/person")],
        broker=BROKER,
        tokens={"doorwatch": "TOKEN"},
        logins={"doorwatch": ("doorwatch", "pw")},
    )
    with pytest.raises(ValueError, match=f"logs in only to {BROKER}"):
        await harness.start()


async def test_a_rule_with_no_token_cannot_ask_for_a_guarded_stream() -> None:
    harness = Harness(
        [Agent.from_dict(DOORWATCH)],
        fleet=[CAMERA],
        broker=BROKER,
        rules=[RULE],
        tokens={"doorwatch": "TOKEN"},
    )
    with pytest.raises(ValueError, match="give person_at_door a token"):
        await harness.start()


async def test_an_agent_that_can_act_starts_on_no_open_bus_unless_told_to() -> None:
    harness = Harness([Agent.from_dict(DOORWATCH)], fleet=[CAMERA], broker=BROKER)

    with pytest.raises(ValueError, match=r'anyone on the bus can replay.*"open_bus": true'):
        await harness.start()


async def test_an_agent_refuses_a_table_step_with_other_arguments_before_sending() -> None:
    lock = f"mqtt://{BROKER}/tc/door/actions/lock/{{+mqttUserName}}"
    door = GUARDED | {"id": "urn:demo:door", "actions": {"lock": {"forms": [{"href": lock}]}}}
    harness = Harness([Agent.from_dict(DOORWATCH)], fleet=[door], broker=BROKER, open_bus=True)
    harness._reach_over_bus(BROKER)
    try:
        refused = await harness._actors["doorwatch"].invoke("door__lock", {"force": True})
    finally:
        await harness.aclose()

    assert refused == {
        "error": "authorization denied",
        "thing": "urn:demo:door",
        "affordance": "lock",
        "op": "invokeaction",
        "reason": "arguments outside the grant's limits for "
        "('urn:demo:door', 'lock', 'invokeaction'): {'force': True} is not one of [{}]",
    }


def test_a_td_on_the_bus_counts_under_its_own_thing() -> None:
    assert not_in_fleet("tc/camera/td", CAMERA, BROKER) is None


@pytest.mark.parametrize(
    ("topic", "td", "reason"),
    [
        ("tc/door/td", CAMERA, "describes urn:demo:camera, not the Thing door"),
        ("tc/camera/td", _camera_at("mqtt://evil.example/tc/x"), "names another broker"),
        ("tc/camera/td", ["not", "a", "td"], "not a TD"),
    ],
)
def test_a_td_on_the_bus_is_left_out_with_its_reason(topic: str, td: Any, reason: str) -> None:
    assert reason in (not_in_fleet(topic, td, BROKER) or "")
