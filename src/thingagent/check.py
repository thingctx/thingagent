# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Check agents and rules against the fleet's TDs before anything runs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Collection, Mapping
from typing import Any

from thingctx.thing import thing_slug

from thingagent.agent import Agent
from thingagent.rules import Rule, broker_address, loopback


def _index(tds: list[dict[str, Any]], kind: str) -> dict[str, dict[str, Any]]:
    return {
        f"{thing_slug(td['id'])}__{name}": affordance
        for td in tds
        for name, affordance in td.get(kind, {}).items()
    }


def _only_mqtt(affordance: dict[str, Any]) -> bool:
    # thingctx sends an MQTT action with no declared output without waiting for a
    # reply, unless the call carries the caller's token.
    forms = affordance.get("forms", [])
    return bool(forms) and all(f.get("href", "").startswith("mqtt") for f in forms)


def _carries_token(td: dict[str, Any]) -> bool:
    """True when thingctx sends the caller's token with each message to ``td``'s
    Thing, so each call waits for an answer: an active scheme is a header bearer
    scheme that names its parameter."""
    defs = td.get("securityDefinitions", {})
    active = td.get("security", [])
    schemes = [defs.get(name, {}) for name in ([active] if isinstance(active, str) else active)]
    return any(
        s.get("scheme") == "bearer" and s.get("in", "header") == "header" and bool(s.get("name"))
        for s in schemes
    )


def _observable(prop: dict[str, Any] | None) -> bool:
    return prop is not None and any(
        "observeproperty" in f.get("op", []) for f in prop.get("forms", [])
    )


def _calls(rule: Rule, agents: dict[str, Agent]) -> list[str]:
    """What is wrong with the agent action ``rule`` calls and the input it gives."""
    name, _, action = rule.calls.partition("__")
    agent = agents.get(name)
    if agent is None or action not in ("decide", "do", "approve"):
        return [
            f"rule {rule.name} calls {rule.calls}, which is not an agent's decide, do or approve"
        ]
    if action == "do" and not agent.may:
        return [f"rule {rule.name} calls {rule.calls}, but {name} may take no action"]
    if action == "do" and not isinstance(rule.args.get("task"), str):
        return [f"rule {rule.name} calls {rule.calls} with no task in its args"]
    if action == "approve" and not (
        isinstance(rule.args.get("person"), str)
        and isinstance(rule.args.get("yes"), bool)
        and isinstance(rule.args.get("action"), str)
    ):
        return [
            f"rule {rule.name} calls {rule.calls} without a person, a yes and an action in its args"
        ]
    return []


def plain_bus_risk(broker: str, *, tls: bool, insecure_bus: bool) -> str | None:
    """Why ``broker`` may not be used, or None: its host is not this machine and the
    link has no TLS (neither ``mqtts://`` nor ``tls``), unless ``insecure_bus``."""
    try:
        secure = tls or broker_address(broker)[2]
    except ValueError as exc:
        return str(exc)
    if secure or insecure_bus or loopback(broker):
        return None
    return (
        f"the broker {broker} is not on this machine and the link has no TLS, so passwords "
        'and tokens cross the network in the clear; use mqtts:// or tls, or set "insecure_bus": '
        "true to accept that risk"
    )


def open_bus_risks(
    agents: list[Agent],
    *,
    broker_binds_identity: bool,
    logins: Collection[str],
    guard: bool,
    policy: bool,
) -> list[str]:
    """Each agent that can act on a bus where a token read off it can be replayed
    (the broker is not said to bind each login to its identity, or the agent has no
    login of its own) or where any login can call it (the agents' own gateway has
    no ``guard`` or no ``policy``)."""
    why = (
        "it has no login"
        if broker_binds_identity
        else "the broker is not set to bind each login to its identity"
    )
    lacks = " and ".join(f"no {n}" for n, has in (("guard", guard), ("policy", policy)) if not has)
    risks: list[str] = []
    for agent in (a for a in agents if a.steps() or a.may):
        if not (broker_binds_identity and agent.name in logins):
            risks.append(
                f"{agent.name} can act on the fleet, but {why}, so anyone on the bus can "
                "replay a token read there or publish its events; set broker_binds_identity "
                'to true and give it a login, or set "open_bus": true to accept that risk'
            )
        if lacks:
            risks.append(
                f"{agent.name} can act on the fleet, but the agents' gateway has {lacks}, so "
                'any login on the bus may call it; set guard and policy, or set "open_bus": '
                "true to accept that risk"
            )
    return risks


def check(
    agents: list[Agent],
    rules: list[Rule],
    fleet: list[dict[str, Any]],
    *,
    tokens: Collection[str] = (),
    allow: Mapping[str, Collection[str]] | None = None,
    broker: str | None = None,
    broker_binds_identity: bool = False,
    logins: Collection[str] = (),
    guard: bool = False,
    policy: bool = False,
    open_bus: bool = False,
    tls: bool = False,
    insecure_bus: bool = False,
    clients: Collection[Agent] = (),
) -> list[str]:
    """Every problem found, once each, as a sentence; an empty list means none.
    ``tokens`` names the agents that call with a token. ``allow`` maps a person to
    the agents they may chat with. With ``broker``, an agent that can act needs
    ``broker_binds_identity``, a login in ``logins``, and a ``guard`` and a
    ``policy`` on the agents' gateway, unless ``open_bus``, and a broker that is not
    on this machine needs ``tls`` or an ``mqtts://`` address, unless
    ``insecure_bus`` (see :func:`plain_bus_risk`). A rule calls an agent
    this harness runs. ``clients`` call from outside, so each only reads and takes
    actions; one with a model, an act table or questions is refused."""
    properties, actions, events = (_index(fleet, k) for k in ("properties", "actions", "events"))
    owners = {
        f"{thing_slug(td['id'])}__{name}": td for td in fleet for name in td.get("actions", {})
    }
    problems: list[str] = []
    # A name is an identity: the token's sub, the broker login and the grants' key.
    names = Counter([*(a.name for a in (*agents, *clients)), *(r.name for r in rules)])
    problems += [
        f"{name} names more than one agent, client or rule; each needs a name of its own"
        for name, count in names.items()
        if count > 1
    ]
    plain = broker and plain_bus_risk(broker, tls=tls, insecure_bus=insecure_bus)
    if plain:
        problems.append(plain)
    if broker and not open_bus:
        problems += open_bus_risks(
            agents,
            broker_binds_identity=broker_binds_identity,
            logins=logins,
            guard=guard,
            policy=policy,
        )
    problems += [
        f"client {client.name} has {', '.join(has)}, but a client only reads and takes actions"
        for client in clients
        if (has := [k for k in ("model", "act", "questions") if getattr(client, k)])
    ]
    for agent in (*agents, *clients):
        if agent.decides_only:
            # A decisions model answers only decide; every other call needs a chat model.
            only = f"{agent.name} has a decisions model, which can only decide,"
            if agent.may:
                problems.append(f"{only} but it may take actions on a task")
            if not agent.questions:
                problems.append(f"{only} but it has no questions")
            problems += [
                f"{only} but {person} may chat with it"
                for person, names in (allow or {}).items()
                if agent.name in names
            ]
        problems += [
            f"{agent.name} reads {name}, which no fleet TD has"
            for name in (*agent.reads, *([agent.image] if agent.image else []))
            if name not in properties
        ]
        for invoke in (*(step.invoke for step in agent.steps()), *agent.may):
            action = actions.get(invoke)
            if action is None:
                problems.append(f"{agent.name} invokes {invoke}, which no fleet TD has")
            elif action is not None and (
                "output" not in action
                and _only_mqtt(action)
                and not (agent.name in tokens and _carries_token(owners[invoke]))
            ):
                problems.append(
                    f"{agent.name} invokes {invoke}, which declares no output, "
                    "so over MQTT nothing confirms it ran or was refused"
                )
    by_name = {agent.name: agent for agent in agents}
    for rule in rules:
        problems += _calls(rule, by_name)
        event = rule.event
        observed = event not in events and _observable(properties.get(event))
        data = properties[event] if observed else events.get(event, {}).get("data")
        fields = data.get("properties", {}) if isinstance(data, dict) else {}
        needed = [k.split(".")[0] for k in rule.when]
        if event not in events and not observed:
            problems.append(
                f"rule {rule.name} reads {event}, which no fleet TD has as an event or "
                "an observable property"
            )
        elif not isinstance(data, dict) or data.get("type") != "object":
            problems.append(f"rule {rule.name} reads {event}, whose data is not an object")
        else:
            problems += [
                f"rule {rule.name} reads {name} from {event}, which its data lacks"
                for name in needed
                if name not in fields
            ]
    return list(dict.fromkeys(problems))
