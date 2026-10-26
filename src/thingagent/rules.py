# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Rules: which fleet events start which agent.

A rule names one event (or an observable property whose changes it reads), the
values its data must hold, and the agent action it calls: ``decide`` with the data
as the trigger, ``do`` with a task, or ``approve`` with a person's answer and the
action it answers for. A dotted name reaches into the data: ``answers.air`` is the
answer named ``air`` in a decided event. thingagent runs every rule itself. A
guarded gateway streams an event only after an authorized request, which
``open_stream`` sends as the rule.

Each MQTT client logs in to the broker its form names, with the login of the
identity it calls as, so a fleet reached with logins must name only the broker
thingagent is configured with.
"""

from __future__ import annotations

import ipaddress
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from thingctx import ThingClient, TransportError
from thingctx.bindings import MqttBinding, MqttTls
from thingctx.operations import Recorder
from thingctx.thing import WoTForm, parse_thing, thing_slug

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# MQTT 5 reason codes of 0x80 or more report a failure (OASIS MQTT 5.0, 2.4).
_REFUSED = 0x80


def _name(value: str) -> str:
    if not _NAME.fullmatch(value):
        raise ValueError(f"{value!r} is not a rule name: use letters, digits and _")
    return value


def _path(value: str) -> str:
    return ".".join(_name(part) for part in value.split("."))


def _at(data: Any, path: str) -> Any:
    """The value at the dotted ``path`` in ``data``, or None. A step into a list takes
    the item with that ``name``; for an answer, that is its choice or score, or for a
    predicate whether its probability is 0.5 or more."""
    for key in path.split("."):
        if isinstance(data, list):
            data = next((i for i in data if isinstance(i, dict) and i.get("name") == key), None)
            if isinstance(data, dict) and data.get("type") == "predicate":
                probability = data.get("probability")
                data = isinstance(probability, int | float) and probability >= 0.5
            elif isinstance(data, dict) and "type" in data:
                data = data.get(data["type"])
        else:
            data = data.get(key) if isinstance(data, dict) else None
    return data


def _time(value: Any) -> datetime | None:
    """``value`` as an aware time: an ISO 8601 string with a zone, or Unix seconds.
    None for anything else, including a number no time can hold."""
    try:
        if isinstance(value, int | float) and not isinstance(value, bool):
            return datetime.fromtimestamp(value, timezone.utc)
        if not isinstance(value, str):
            return None
        # fromisoformat reads a trailing Z only from Python 3.11.
        text = f"{value[:-1]}+00:00" if value.endswith("Z") else value
        when = datetime.fromisoformat(text)
    except (ValueError, OverflowError, OSError):
        return None
    return when if when.tzinfo is not None else None


@dataclass(frozen=True)
class Rule:
    """A rule: when ``event`` carries the values ``when`` names, call ``calls``, an
    agent's ``<agent>__decide``, ``<agent>__do`` or ``<agent>__approve``, with
    ``args`` as that action's input. With ``max_age``, an event whose ``time`` (a
    dotted name, ``at`` by default) is missing, unreadable or more than ``max_age``
    seconds old starts nothing."""

    name: str
    calls: str
    event: str
    when: dict[str, Any] = field(default_factory=dict)
    args: dict[str, Any] = field(default_factory=dict)
    max_age: float | None = None
    time: str = "at"

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Rule:
        """Build a rule from its definition. Raises ``ValueError`` for a name that is
        not letters, digits and _, a ``max_age`` that is not a positive number, or
        an ``approve`` call whose args do not name the action it approves."""
        name = _name(raw["name"])
        when = {_path(k): v for k, v in raw.get("when", {}).items()}
        args = raw.get("args", {})
        if raw["calls"].endswith("__approve") and not isinstance(args.get("action"), str):
            raise ValueError(f"rule {name} calls {raw['calls']} without the action it approves")
        max_age = raw.get("max_age")
        if max_age is not None and (
            isinstance(max_age, bool) or not isinstance(max_age, int | float) or max_age <= 0
        ):
            raise ValueError(f"rule {name} has a max_age that is not a positive number of seconds")
        return cls(
            name,
            raw["calls"],
            event=raw["event"],
            when=when,
            args=args,
            max_age=max_age,
            time=_path(raw.get("time", "at")),
        )

    @classmethod
    def load(cls, path: str | Path) -> Rule:
        return cls.from_dict(json.loads(Path(path).read_text()))

    def matches(self, data: Any) -> bool:
        """True when ``data`` holds every value ``when`` names."""
        return isinstance(data, dict) and all(_at(data, k) == v for k, v in self.when.items())

    def stale(self, data: Any, now: datetime | None = None) -> str | None:
        """Why ``data`` is too old to start anything, or None when it is not, or the
        rule has no ``max_age``."""
        if self.max_age is None:
            return None
        when = _time(_at(data, self.time))
        if when is None:
            return f"it has no time with a zone at {self.time}"
        age = ((now or datetime.now(timezone.utc)) - when).total_seconds()
        if age > self.max_age:
            return f"it is {age:.0f} s old, more than its max_age of {self.max_age:g} s"
        return None


def stream_href(tds: list[dict[str, Any]], tool: str) -> tuple[dict[str, Any], str]:
    """The TD and the MQTT href that streams ``tool``: an event's form, or the form
    of an observable property that declares ``observeproperty``. Raises
    ``KeyError`` when no TD streams it over MQTT."""
    slug, _, name = tool.partition("__")
    for td in tds:
        if thing_slug(td["id"]) != slug:
            continue
        forms = td.get("events", {}).get(name, {}).get("forms", [])
        prop = td.get("properties", {}).get(name, {})
        forms += [f for f in prop.get("forms", []) if "observeproperty" in f.get("op", [])]
        for form in forms:
            if form.get("href", "").startswith("mqtt"):
                return td, form["href"]
    raise KeyError(f"no MQTT event or observable property {tool!r} in the TDs")


def topic(href: str) -> str:
    return urlsplit(href).path.lstrip("/")


def on_request(href: str) -> bool:
    """True when ``href`` names a stream a guarded gateway starts only on an
    authorized request, which :func:`open_stream` sends."""
    return href.endswith("/stream")


def _authority(href: str) -> tuple[str, int] | None:
    parts = urlsplit(href)
    try:
        return parts.hostname or "", parts.port or (8883 if parts.scheme == "mqtts" else 1883)
    except ValueError:  # a port that is not a number names no broker
        return None


def broker_address(broker: str) -> tuple[str, int, bool]:
    """The host and port of ``broker`` (``host[:port]``, ``mqtt://host[:port]`` or
    ``mqtts://host[:port]``), and whether it uses TLS. The port is 1883 by default,
    8883 with TLS. Raises ``ValueError`` for anything else."""
    href = broker if "://" in broker else f"mqtt://{broker}"
    found = _authority(href)
    if urlsplit(href).scheme not in ("mqtt", "mqtts") or not found or not found[0]:
        raise ValueError(f"{broker} is not host:port, mqtt://host:port or mqtts://host:port")
    return found[0], found[1], href.startswith("mqtts://")


def loopback(broker: str) -> bool:
    """True when ``broker``'s host is this machine: 127.0.0.0/8, ::1 or localhost."""
    host = broker_address(broker)[0]
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def bus_tls(broker: str, tls: MqttTls | None) -> MqttTls | None:
    """The TLS a connection to ``broker`` uses: ``tls``, else the system CA store's
    for an ``mqtts`` broker, else none."""
    return tls if tls is not None or not broker_address(broker)[2] else MqttTls()


def _forms(td: dict[str, Any]) -> list[WoTForm]:
    thing = parse_thing(td)
    return [
        *thing.forms,
        *(form for p in thing.properties.values() for form in p.forms),
        *(form for a in thing.actions.values() for form in a.forms),
        *(form for e in thing.events.values() for form in e.forms),
    ]


def other_brokers(tds: list[dict[str, Any]], broker: str) -> list[str]:
    """Each MQTT form href in ``tds``, as thingctx's client resolves it, that names
    a broker other than ``broker``, as :func:`broker_address` reads it."""
    here = broker_address(broker)[:2]
    return [
        form.href
        for td in tds
        for form in _forms(td)
        if urlsplit(form.href).scheme in ("mqtt", "mqtts") and _authority(form.href) != here
    ]


def not_in_fleet(topic: str, td: Any, broker: str) -> str | None:
    """Why the TD retained on ``topic`` is not one of the fleet's, or None when it
    is: a TD of the Thing that topic names, whose MQTT forms all name ``broker``.
    On a broker that lets a gateway retain TDs only for its own Things, the topic
    vouches for the Thing."""
    if not isinstance(td, dict) or not isinstance(td.get("id"), str):
        return "it is not a TD with an id"
    level = topic.rsplit("/", 2)[-2]
    if thing_slug(td["id"]) != level:
        return f"it describes {td['id']}, not the Thing {level}"
    elsewhere = other_brokers([td], broker)
    if elsewhere:
        return f"it names another broker: {', '.join(elsewhere)}"
    return None


def credentials(
    tds: list[dict[str, Any]], token: str | None, login: tuple[str, str] | None
) -> dict[str, Any]:
    """``token`` under every bearer scheme and ``login`` (user, password) under every
    basic scheme the TDs define, keyed by scheme name so neither secret is offered
    to the other's scheme. Raises ``ValueError`` when the TDs give one scheme name
    two schemes, since its secret would then reach both."""
    out: dict[str, Any] = {}
    kinds: dict[str, Any] = {}
    for td in tds:
        for name, scheme in td.get("securityDefinitions", {}).items():
            kind = scheme.get("scheme")
            if kinds.setdefault(name, kind) != kind:
                raise ValueError(f"the TDs define {name!r} as both {kinds[name]} and {kind}")
            if kind == "bearer" and token is not None:
                out[name] = token
            elif kind == "basic" and login is not None:
                out[name] = login
    return out


def client_as(
    tds: list[dict[str, Any]],
    token: str | None,
    login: tuple[str, str] | None,
    record: Recorder | None = None,
    **options: Any,
) -> ThingClient:
    """A client of ``tds`` over MQTT that calls as one identity, with ``token`` and
    ``login`` given as :func:`credentials` does. ``record`` receives every call it
    makes. ``options`` go to its binding."""
    secrets = credentials(tds, token, login)
    binding = MqttBinding(credentials=secrets, **options)
    return ThingClient(tds=tds, bindings=[binding], record=record)


async def open_stream(
    td: dict[str, Any],
    href: str,
    secrets: dict[str, Any],
    *,
    timeout: float = 10.0,
    tls: MqttTls | None = None,
) -> str:
    """Ask the guarded gateway that serves ``td`` to start the stream whose form is
    ``href``, and return the topic it streams to. The request logs in and
    authorizes with ``secrets`` (from :func:`credentials`). Raises
    ``PermissionError`` when the gateway refuses.

    A guarded form names the caller's stream, ``<request>/stream``. The gateway
    starts it after an authorized request on ``<request>``, so the request is sent
    as an action on that topic, with a declared output so the client waits for the
    answer. ``tls`` puts the request on TLS."""
    if not on_request(href):
        raise ValueError(f"{href} names no stream a guarded gateway starts on request")
    request = {
        "@context": td.get("@context", "https://www.w3.org/2022/wot/td/v1.1"),
        "id": td["id"],
        "securityDefinitions": td["securityDefinitions"],
        "security": td["security"],
        "uriVariables": td.get("uriVariables", {}),
        "actions": {
            "subscribe": {
                "output": {"type": "object"},
                "forms": [{"href": href.removesuffix("/stream")}],
            }
        },
    }
    binding = MqttBinding(qos=1, timeout=timeout, credentials=secrets, tls=tls)
    client = ThingClient(tds=[request], bindings=[binding])
    try:
        reply = await client.invoke(f"{thing_slug(td['id'])}__subscribe", {})
    finally:
        await client.aclose()
    if not isinstance(reply, dict) or not reply.get("subscribed"):
        raise PermissionError(f"{td['id']} refused the stream at {href}: {reply}")
    return str(reply["stream"])


def probe_topics(tds: list[dict[str, Any]], prefix: str) -> list[str]:
    """Every request topic a guarded gateway serves for ``tds``, before its trailing
    user level, for :func:`binds_logins` to probe: each action, each property's read,
    write and observe, and each event's subscribe. With no TDs, one topic no Thing has."""
    topics = []
    for td in tds:
        base = f"{prefix}/{thing_slug(td['id'])}"
        topics += [f"{base}/actions/{name}" for name in td.get("actions", {})]
        for name in td.get("properties", {}):
            topics += [f"{base}/props/{name}/{kind}" for kind in ("read", "write", "observe")]
        topics += [f"{base}/events/{name}/subscribe" for name in td.get("events", {})]
    return topics or [f"{prefix}/thingagent/actions/thingagent_probe_{uuid.uuid4().hex[:8]}"]


async def binds_logins(
    broker: str,
    topics: list[str],
    logins: list[tuple[str, str]],
    *,
    tls: MqttTls | None = None,
    timeout: float = 5.0,
) -> list[str]:
    """Each user in ``logins`` the broker lets publish a request under another name,
    as a sentence; an empty list means it refused every one. Each login publishes a
    request on each of ``topics`` (from :func:`probe_topics`) ending in the user of
    the next login in the list with another user, or, when there is none, in a name
    no login has. Only a refusal in
    the PUBACK (reason 0x80 or higher) proves the broker binds the login on that
    topic; a publish it takes (0x00 or 0x10) does not. A request it takes reaches
    the gateway with no token, which refuses it. Raises ``TransportError`` when a
    login cannot connect."""
    host, port, secure = broker_address(broker)
    unused = f"thingagent_probe_{uuid.uuid4().hex[:8]}"
    users = [user for user, _ in logins]
    published: list[str] = []
    for i, login in enumerate(logins):
        others = [user for user in users[i + 1 :] + users[:i] if user != login[0]]
        name = others[0] if others else unused
        scheme = "mqtts" if secure else "mqtt"
        probe = {
            "@context": "https://www.w3.org/2022/wot/td/v1.1",
            "id": "urn:thingagent:probe",
            "securityDefinitions": {"bus_basic": {"scheme": "basic"}},
            "security": ["bus_basic"],
            # A declared output makes the client read the PUBACK and wait for a reply.
            "actions": {
                f"p{n}": {
                    "output": {"type": "object"},
                    "forms": [{"href": f"{scheme}://{host}:{port}/{topic}/{name}"}],
                }
                for n, topic in enumerate(topics)
            },
        }
        binding = MqttBinding(qos=1, timeout=timeout, credentials={"bus_basic": login}, tls=tls)
        client = ThingClient(tds=[probe], bindings=[binding])
        try:
            for n, topic in enumerate(topics):
                try:
                    await client.invoke(f"probe__p{n}", {})
                except TransportError as exc:
                    if exc.method == "CONNECT":
                        raise
                    if exc.status is not None and exc.status >= _REFUSED:
                        continue
                published.append(
                    f"the broker let {login[0]} publish a request under another name on "
                    f"{topic}, so it does not bind each login to its name"
                )
                break
        finally:
            await client.aclose()
    return published
