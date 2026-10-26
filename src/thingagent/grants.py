# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Least privilege grants, derived from what agents and rules name.

An agent may read exactly the properties it reads and invoke exactly the actions
its table, its approval signals and its ``may`` name. A rule may subscribe to
exactly the events it reads and observe exactly the properties it reads. Grants
are keyed by identity name (each token's ``sub``), in the form thingctx's policy
decision point takes.

A grant bounds an action's arguments when the agent's ``limits`` name the action,
or when only its steps take it: then it permits only the arguments those steps
send, within any limit. thingctx checks them in process with no broker, and before
a table step is sent with one; the fleet's gateway checks them from ``grants.json``
(see :func:`saved`).
"""

from __future__ import annotations

import json
from typing import Any

from thingctx.authz import GrantSet, limited
from thingctx.thing import SCALAR_INPUT_KEY, fill_declared, is_wrapped_input, thing_slug

from thingagent.agent import Agent
from thingagent.rules import Rule


def _sent(action: dict[str, Any], args: dict[str, Any]) -> Any:
    """``args`` as thingctx's decision point sees them: the TD's const and default
    values filled, a scalar input bare."""
    schema = action.get("input") or {}
    filled = fill_declared(schema, args)
    return filled.get(SCALAR_INPUT_KEY) if is_wrapped_input(schema) else filled


def grants(
    agents: list[Agent], rules: list[Rule], fleet: list[dict[str, Any]]
) -> dict[str, GrantSet]:
    """Each agent's and rule's grants on ``fleet``, keyed by name. A name no fleet
    TD has grants nothing; ``thingagent check`` reports it."""
    found = {
        (kind, f"{thing_slug(td['id'])}__{name}"): (td["id"], name, affordance)
        for td in fleet
        for kind in ("properties", "actions", "events")
        for name, affordance in td.get(kind, {}).items()
    }

    def grant(kind: str, tool: str, op: str, schema: dict[str, Any] | None = None) -> GrantSet:
        if (kind, tool) not in found:
            return set()
        thing, name, _ = found[kind, tool]
        return {(thing, name, op) if schema is None else limited(thing, name, op, schema)}

    out: dict[str, GrantSet] = {}
    for agent in agents:
        given: GrantSet = set()
        for name in (*agent.reads, *([agent.image] if agent.image else [])):
            given |= grant("properties", name, "readproperty")
        steps: dict[str, list[Any]] = {}
        for step in agent.steps():
            if ("actions", step.invoke) in found:
                sent = steps.setdefault(step.invoke, [])
                args = _sent(found["actions", step.invoke][2], step.args)
                if args not in sent:
                    sent.append(args)
        for action in dict.fromkeys((*steps, *agent.may)):
            # A limit only narrows: an action the agent may take is bounded by its
            # limit alone, one only steps take by the step arguments and the limit.
            schema = agent.limits.get(action)
            if action not in agent.may:
                exact = {"enum": steps[action]}
                schema = {"allOf": [schema, exact]} if schema is not None else exact
            given |= grant("actions", action, "invokeaction", schema)
        out[agent.name] = given
    for rule in rules:
        # A name that is no event is read as an observable property.
        out[rule.name] = grant("events", rule.event, "subscribeevent") or grant(
            "properties", rule.event, "observeproperty"
        )
    return out


def saved(given: dict[str, GrantSet]) -> dict[str, list[list[Any]]]:
    """``given`` as ``grants.json`` holds it: each grant a ``[thing, affordance, op]``
    list, with a fourth item, the JSON Schema its arguments must satisfy, when the
    grant limits them."""
    return {
        user: [[*g[:3], *map(json.loads, g[3:])] for g in sorted(granted)]
        for user, granted in given.items()
    }


def loaded(raw: dict[str, list[list[Any]]]) -> dict[str, GrantSet]:
    """The grants :func:`saved` wrote."""
    return {
        user: {limited(*g) if len(g) > 3 else (g[0], g[1], g[2]) for g in rows}
        for user, rows in raw.items()
    }
