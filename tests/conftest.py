# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""A fleet behind a guarded thingctx gateway, signed tokens, and the agents the
tests serve. The live tests need an MQTT 5 broker; the default matches
compose.yaml, THINGAGENT_TEST_BROKER overrides it, and they skip when it is down.
Every client logs in under its token's sub. For a locked broker,
THINGAGENT_TEST_PASSWORDS names a JSON file of user to password; an open broker
takes any password. With that file set, the rig first checks that the broker
refuses an anonymous client and a wrong password."""

from __future__ import annotations

import asyncio
import functools
import json
import os
import socket
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from thingctx import ThingClient
from thingctx.bindings import LocalBinding
from thingctx.gateways import Gateway
from thingctx.identity import JwtGatewayGuard
from thingctx.models.sosa.recording import recorder
from thingctx.thing import thing_slug

from thingagent import Agent, Harness, Rule, grants
from thingagent.harness import guarded_gateway
from thingagent.rules import client_as

BROKER = os.environ.get("THINGAGENT_TEST_BROKER", "127.0.0.1:1884")
_PASSWORDS = os.environ.get("THINGAGENT_TEST_PASSWORDS")
PASSWORDS: dict[str, str] = json.loads(Path(_PASSWORDS).read_text()) if _PASSWORDS else {}
# A locked broker runs the rules thingagent generate compiles, so it refuses an
# ungranted request itself; an open broker leaves every refusal to the gateway.
LOCKED = bool(PASSWORDS)
ISSUER = "https://issuer.test"
AUDIENCE = "api://fleet"
# Stands in for a camera frame; the tests' models never decode it.
SNAPSHOT = "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQ=="

CTX = "https://www.w3.org/2022/wot/td/v1.1"
NOSEC = {"securityDefinitions": {"nosec_sc": {"scheme": "nosec"}}, "security": ["nosec_sc"]}
DONE = {"type": "object"}


def _local(name: str) -> list[dict[str, str]]:
    return [{"href": f"local://{name}"}]


FLEET = [
    {
        "@context": CTX,
        "id": "urn:demo:door",
        "title": "Front door",
        **NOSEC,
        "properties": {"locked": {"type": "boolean", "readOnly": True, "forms": _local("locked")}},
        "actions": {
            "lock": {"output": DONE, "forms": _local("lock")},
            "unlock": {"output": DONE, "forms": _local("unlock")},
        },
    },
    {
        "@context": CTX,
        "id": "urn:demo:camera",
        "title": "Door camera",
        **NOSEC,
        "properties": {
            "snapshot": {
                "type": "string",
                "contentMediaType": "image/jpeg",
                "readOnly": True,
                "forms": _local("snapshot"),
            }
        },
        "events": {
            "person": {
                "data": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string"},
                        "at": {"type": "string", "format": "date-time"},
                    },
                },
                "forms": _local("person"),
            }
        },
    },
    {
        "@context": CTX,
        "id": "urn:demo:alarm",
        "title": "Alarm",
        **NOSEC,
        "actions": {"sound": {"output": DONE, "forms": _local("sound")}},
    },
    {
        "@context": CTX,
        "id": "urn:demo:thermostat",
        "title": "Hall thermostat",
        **NOSEC,
        "properties": {
            "temperature": {"type": "number", "readOnly": True, "forms": _local("temperature")}
        },
    },
]

AGENTS_POLICY = {
    "rule": {
        ("urn:thingagent:agent:doorwatch", "decide", "invokeaction"),
        ("urn:thingagent:agent:chair", "decide", "invokeaction"),
    },
    "channel": {("urn:thingagent:channel:chat", "receive", "invokeaction")},
    "observer": {("urn:thingagent:agent:doorwatch", "decided", "subscribeevent")},
}

DOORWATCH = {
    "name": "doorwatch",
    "title": "Door watch",
    "instructions": "You watch the front door.",
    "questions": [
        {
            "type": "choice",
            "name": "who",
            "instructions": "Who is at the door?",
            "values": ["resident", "delivery", "stranger", "unclear"],
        }
    ],
    "act": {
        "who": {
            "stranger": [{"invoke": "door__lock"}, {"invoke": "alarm__sound"}],
            "delivery": [{"invoke": "door__lock"}],
        }
    },
    "reads": ["door__locked"],
    "image": "camera__snapshot",
    "person": "alice",
}

ADVISOR = {
    "name": "advisor",
    "title": "Home advisor",
    "instructions": "You look after the house and tell Alice only what matters.",
    "reads": ["thermostat__temperature"],
    "person": "alice",
}

# A client calls from outside, through thingagent mcp; it only reads and takes actions.
CLIENT = {"name": "claude", "reads": ["thermostat__temperature"], "may": ["door__lock"]}


# The logins the rules in these tests use; each reads the camera's person event.
RULE_LOGINS = ("stranger", "person_at_door")


def fleet_grants() -> dict[str, set[tuple[str, str, str]]]:
    """Exactly what each agent, client and rule definition names, keyed by its
    token's sub, as ``thingagent generate`` writes them for the fleet's gateway."""
    rules = [
        Rule.from_dict({"name": name, "event": "camera__person", "calls": "doorwatch__decide"})
        for name in RULE_LOGINS
    ]
    agents = [Agent.from_dict(raw) for raw in (DOORWATCH, ADVISOR, CLIENT)]
    return grants(agents, rules, FLEET)


def login(user: str) -> tuple[str, str]:
    """The broker login of ``user``."""
    return user, PASSWORDS.get(user, "unused")


class Keys:
    """One signing key, its JWKS, and tokens for any subject and roles."""

    def __init__(self) -> None:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self._pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
        self.jwks = {"keys": [public | {"kid": "k1", "alg": "RS256", "use": "sig"}]}

    def token(self, sub: str, *roles: str) -> str:
        now = int(time.time())
        claims = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": sub,
            "roles": list(roles),
            "iat": now,
            "nbf": now - 5,
            "exp": now + 600,
        }
        return jwt.encode(claims, self._pem, algorithm="RS256", headers={"kid": "k1"})

    def guard(self) -> JwtGatewayGuard:
        return JwtGatewayGuard(issuers=[ISSUER], audience=AUDIENCE, jwks=self.jwks)


class Devices:
    """The fleet's devices, in process."""

    def __init__(self) -> None:
        self.locked = False
        self.sounded = 0
        self.temperature = 26.5
        self.camera = LocalBinding({"snapshot": lambda: SNAPSHOT})

    def lock(self) -> dict[str, bool]:
        self.locked = True
        return {"locked": True}

    def unlock(self) -> dict[str, bool]:
        self.locked = False
        return {"locked": False}

    def sound(self) -> dict[str, bool]:
        self.sounded += 1
        return {"sounding": True}

    def binding(self) -> LocalBinding:
        local = LocalBinding()
        local.register_thing(
            "door", {"locked": lambda: self.locked, "lock": self.lock, "unlock": self.unlock}
        )
        local.register_thing("camera", self.camera)
        local.register_thing("alarm", {"sound": self.sound})
        local.register_thing("thermostat", {"temperature": lambda: self.temperature})
        return local


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


@functools.cache
def _lets_in(user: str | None, password: str = "") -> bool:
    """True when the broker accepts a CONNECT with ``user`` and ``password``, or
    with no login when ``user`` is None."""
    import paho.mqtt.client as mqtt

    host, _, port = BROKER.partition(":")
    answered = threading.Event()
    reasons: list[Any] = []

    def on_connect(_c: Any, _u: Any, _flags: Any, reason: Any, _p: Any) -> None:
        reasons.append(reason)
        answered.set()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    if user is not None:
        client.username_pw_set(user, password)
    client.connect(host, int(port or 1883))
    client.loop_start()
    try:
        answered.wait(5)
    finally:
        client.loop_stop()
        client.disconnect()
    return bool(reasons) and not reasons[0].is_failure


def publish_as(user: str, topic: str, token: str) -> int:
    """The PUBACK reason code for a QoS 1 request on ``topic`` carrying ``token``,
    sent as ``user`` by a raw client that picks its own topic."""
    import paho.mqtt.client as mqtt
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties

    host, _, port = BROKER.partition(":")
    acked = threading.Event()
    reasons: list[int] = []

    def on_publish(_c: Any, _u: Any, _mid: Any, reason: Any, _p: Any) -> None:
        reasons.append(reason.value)
        acked.set()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.on_publish = on_publish
    client.username_pw_set(*login(user))
    properties = Properties(PacketTypes.PUBLISH)
    properties.UserProperty = [("authorization", f"Bearer {token}")]
    properties.ResponseTopic = f"{topic}/reply"
    properties.CorrelationData = uuid.uuid4().bytes
    client.connect(host, int(port or 1883))
    client.loop_start()
    try:
        client.publish(topic, b"{}", qos=1, properties=properties)
        acked.wait(5)
    finally:
        client.loop_stop()
        client.disconnect()
    return reasons[0] if reasons else -1


def _clear_retained(prefix: str, slugs: list[str], user: str) -> None:
    """Clear the TDs retained for ``slugs`` as ``user``, the gateway that serves
    them, since only it may write them on a locked broker."""
    import paho.mqtt.client as mqtt

    host, _, port = BROKER.partition(":")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.username_pw_set(*login(user))
    client.connect(host, int(port or 1883))
    client.loop_start()
    for slug in slugs:
        client.publish(f"{prefix}/{slug}/td", b"", qos=1, retain=True).wait_for_publish(5)
    client.loop_stop()
    client.disconnect()


async def eventually(check: Callable[[], Any], timeout: float = 15.0, what: str = "") -> Any:
    """Wait until ``check()`` is truthy and return it."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what or check}")


class Rig:
    """A fleet behind a guarded gateway on the live broker, and thingagent
    instances started against it."""

    def __init__(self) -> None:
        self.run = uuid.uuid4().hex[:8]
        self.prefix = f"thingagent_test_{self.run}"
        self.keys = Keys()
        self.devices = Devices()
        self.records: list[Any] = []
        self.fleet: list[dict[str, Any]] = []
        self._gateway: Gateway | None = None
        self._harnesses: list[Harness] = []
        self._clients: list[ThingClient] = []

    async def start(self) -> None:
        self._gateway = guarded_gateway(
            FLEET,
            [self.devices.binding()],
            BROKER,
            grants=fleet_grants(),
            claim="sub",
            guard=self.keys.guard(),
            record=recorder(self.records.append),
            login=login("fleet_gw"),
            prefix=self.prefix,
            broker_binds_identity=True,
        )
        await self._gateway.start()
        self.fleet = list(self._gateway.projected_tds.values())

    async def restart_gateway(self) -> None:
        """Stop the fleet's gateway and start a new one, as a restart does."""
        if self._gateway is not None:
            await self._gateway.aclose()
        await self.start()

    async def harness(self, **options: Any) -> Harness:
        tokens = options.pop("tokens", None) or {
            "doorwatch": self.keys.token("doorwatch", "agent"),
            "advisor": self.keys.token("advisor", "agent"),
        }
        settings: dict[str, Any] = {
            "broker": BROKER,
            "fleet": self.fleet,
            "tokens": tokens,
            "logins": {name: login(name) for name in tokens},
            "login": login("agents_gw"),
            "allow": {"alice": ["doorwatch", "advisor"]},
            "prefix": self.prefix,
            "guard": self.keys.guard(),
            "broker_binds_identity": True,
            "policy": AGENTS_POLICY,
        }
        agents = options.pop("agents", None) or [
            Agent.from_dict(DOORWATCH),
            Agent.from_dict(ADVISOR),
        ]
        harness = Harness(agents, **settings | options)
        await harness.start()
        self._harnesses.append(harness)
        return harness

    def client(
        self, td: dict[str, Any], sub: str, *roles: str, user: str | None = None
    ) -> ThingClient:
        """A client of one served Thing that calls with a token for ``sub`` and
        ``roles``, logged in as ``user``, which is ``sub`` unless given."""
        client = client_as(
            [td], self.keys.token(sub, *roles), login(user or sub), qos=1, timeout=10.0
        )
        self._clients.append(client)
        return client

    def actuations(self, affordance: str) -> list[Any]:
        return self._records("Actuation", affordance)

    def refusals(self, affordance: str) -> list[Any]:
        return self._records("Refusal", affordance)

    def _records(self, kind: str, affordance: str) -> list[Any]:
        return [
            r
            for r in self.records
            if type(r).__name__ == kind
            and (getattr(r, "acts_on_property", None) or getattr(r, "operation", None))
            == affordance
        ]

    async def aclose(self) -> None:
        own = [thing_slug(td["id"]) for h in self._harnesses for td in h.own_tds()]
        for client in self._clients:
            await client.aclose()
        for harness in self._harnesses:
            await harness.aclose()
        if self._gateway is not None:
            await self._gateway.aclose()
        _clear_retained(self.prefix, ["door", "camera", "alarm", "thermostat"], "fleet_gw")
        _clear_retained(self.prefix, sorted(set(own)), "agents_gw")


@pytest.fixture
async def rig() -> AsyncIterator[Rig]:
    host, _, port = BROKER.partition(":")
    if not _reachable(host, int(port or 1883)):
        pytest.skip(f"no MQTT broker at {BROKER}")
    if LOCKED and (_lets_in(None) or _lets_in("fleet_gw", "not the password")):
        pytest.fail(f"THINGAGENT_TEST_PASSWORDS is set, but the broker at {BROKER} is not locked")
    rig = Rig()
    await rig.start()
    try:
        yield rig
    finally:
        await rig.aclose()


@pytest.fixture
async def in_process() -> AsyncIterator[tuple[Devices, Callable[..., Any]]]:
    """Devices and a factory for thingagent instances that reach them with no broker."""
    devices = Devices()
    harnesses: list[Harness] = []

    async def make(**options: Any) -> Harness:
        settings: dict[str, Any] = {
            "fleet": FLEET,
            "bindings": [devices.binding()],
            "allow": {"alice": ["doorwatch", "advisor"]},
        }
        agents = options.pop("agents", None) or [
            Agent.from_dict(DOORWATCH),
            Agent.from_dict(ADVISOR),
        ]
        harness = Harness(agents, **settings | options)
        await harness.start()
        harnesses.append(harness)
        return harness

    try:
        yield devices, make
    finally:
        for harness in harnesses:
            await harness.aclose()
