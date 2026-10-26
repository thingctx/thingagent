# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""An agent's definition, and the Thing Description it is served under.

The definition holds the behavior: the model, instructions, questions, the table
from each answer to its steps, the properties it reads, the actions it may take on
a task, the limits on their arguments, and which of them a person must approve.
The TD holds only the interface (``decide``, ``ask``, ``approve``, ``do`` for an
agent that may act, and the ``decided`` and ``did`` events) plus every value a
decision may choose, so a reader sees what the agent can decide without seeing how.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from thingctx.authz import limited

QUESTION_TYPES = ("predicate", "choice", "score")
# Every agent's Thing id starts with this, whichever harness serves it.
AGENT_ID = "urn:thingagent:agent:"


@dataclass(frozen=True)
class Step:
    """One action a decision takes: a fleet action tool, its arguments, and the
    question a person must approve first, if any."""

    invoke: str
    args: dict[str, Any] = field(default_factory=dict)
    approve: str | None = None


@dataclass(frozen=True)
class Question:
    """A question shaped like OpenAI's Decisions API: a ``predicate`` answered
    with a probability, a ``choice`` among ``values``, or a ``score`` among
    ordered ``values``."""

    type: str
    name: str
    instructions: str
    values: tuple[str, ...] = ()


@dataclass(frozen=True)
class Approval:
    """How a person approves a step: how long to wait, and the steps that show a
    question is waiting (``signal``) and answered (``clear``)."""

    timeout: float | None = None
    signal: tuple[Step, ...] = ()
    clear: tuple[Step, ...] = ()


def _steps(raw: list[dict[str, Any]]) -> tuple[Step, ...]:
    return tuple(Step(s["invoke"], s.get("args", {}), s.get("approve")) for s in raw)


@dataclass(frozen=True)
class Agent:
    """One agent. ``act`` maps a question name, then an answer value, to the steps
    that answer takes; an answer with no entry takes none. ``may`` names the fleet
    actions the agent may take on a task, with arguments it chooses. ``limits``
    maps any action it may take or its steps take to a JSON Schema its arguments
    must satisfy. ``approve`` maps any action it may take to the question its
    person must say yes to first. ``options`` configure the model library or
    Decisions endpoint with every call."""

    name: str
    title: str
    instructions: str
    model: str = ""
    questions: tuple[Question, ...] = ()
    act: dict[str, dict[str, tuple[Step, ...]]] = field(default_factory=dict)
    reads: tuple[str, ...] = ()
    image: str | None = None
    person: str | None = None
    may: tuple[str, ...] = ()
    limits: dict[str, dict[str, Any]] = field(default_factory=dict)
    approve: dict[str, str] = field(default_factory=dict)
    approval: Approval = field(default_factory=Approval)
    rounds: int = 12
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Agent:
        """Build an agent from its definition. Raises ``ValueError`` for a question
        of an unknown type, a step under an answer the question cannot give, a limit
        or an approval for an action the agent may not take, or a limit with a
        numeric bound and no ``type``, which a value that is no number would pass."""
        questions = tuple(
            Question(q["type"], q["name"], q.get("instructions", ""), tuple(q.get("values", ())))
            for q in raw.get("questions", ())
        )
        for q in questions:
            if q.type not in QUESTION_TYPES:
                raise ValueError(f"question {q.name!r} has unknown type {q.type!r}")
        allowed = {q.name: set(q.values) for q in questions}
        act: dict[str, dict[str, tuple[Step, ...]]] = {}
        for qname, by_value in raw.get("act", {}).items():
            for value in by_value:
                if value not in allowed.get(qname, set()):
                    raise ValueError(f"act names {qname}={value!r}, which no question can answer")
            act[qname] = {value: _steps(steps) for value, steps in by_value.items()}
        may = tuple(raw.get("may", ()))
        approve = dict(raw.get("approve", {}))
        for action in approve:
            if action not in may:
                raise ValueError(f"approve names {action}, which the agent may not take")
        approval = raw.get("approval", {})
        agent = cls(
            name=raw["name"],
            title=raw.get("title", raw["name"]),
            instructions=raw.get("instructions", ""),
            model=raw.get("model", ""),
            questions=questions,
            act=act,
            reads=tuple(raw.get("reads", ())),
            image=raw.get("image"),
            person=raw.get("person"),
            may=may,
            limits=dict(raw.get("limits", {})),
            approve=approve,
            approval=Approval(
                approval.get("timeout"),
                _steps(approval.get("signal", [])),
                _steps(approval.get("clear", [])),
            ),
            rounds=raw.get("rounds", 12),
            options=dict(raw.get("options", {})),
        )
        taken = {*agent.may, *(step.invoke for step in agent.steps())}
        for action in agent.limits:
            if action not in taken:
                raise ValueError(f"limits names {action}, which the agent may not take")
            try:
                limited(agent.name, action, "invokeaction", agent.limits[action])
            except ValueError as exc:
                raise ValueError(f"agent {agent.name}, limits {action}: {exc}") from None
        return agent

    @classmethod
    def load(cls, path: str | Path) -> Agent:
        return cls.from_dict(json.loads(Path(path).read_text()))

    @property
    def decides_only(self) -> bool:
        """True for a Decisions model, which answers only decide."""
        return self.model.startswith("decisions/")

    def steps(self) -> tuple[Step, ...]:
        """Every step the act table and approvals can run, each with fixed
        arguments."""
        table = (s for by_value in self.act.values() for steps in by_value.values() for s in steps)
        return (*table, *self.approval.signal, *self.approval.clear)

    def _decision(self) -> dict[str, Any]:
        """The schema of what ``decide`` returns and ``decided`` reports: every
        answer it may give, each in its Decisions shape, or a refusal and its reason,
        and the usage and cost the model reports."""
        shapes: list[dict[str, Any]] = []
        for q in self.questions:
            value: dict[str, Any] = (
                {"type": "number", "minimum": 0, "maximum": 1}
                if q.type == "predicate"
                else {"enum": list(q.values)}
            )
            field = "probability" if q.type == "predicate" else q.type
            properties = {"type": {"const": q.type}, "name": {"const": q.name}, field: value}
            if q.type != "predicate":
                properties["confidence"] = {"type": "number"}
            shapes.append(
                {"type": "object", "properties": properties, "required": ["type", "name", field]}
            )
        answers: dict[str, Any] = {"type": "array", "maxItems": 0}
        if self.questions:
            refusal = {
                "type": "object",
                "properties": {
                    "type": {"const": "refusal"},
                    "name": {"enum": [q.name for q in self.questions]},
                    "reason": {"enum": ["model_refused", "not_a_choice", "request_failed"]},
                },
                "required": ["type", "name"],
            }
            answers = {"type": "array", "items": {"oneOf": [*shapes, refusal]}}
        return {
            "type": "object",
            "properties": {
                "agent": {"const": self.name},
                "correlation": {"type": "string"},
                "trigger": {"type": "object"},
                "answers": answers,
                "acted": {"type": "array", "items": {"type": "object"}},
                "at": {"type": "string", "format": "date-time"},
                "usage": {
                    "type": "object",
                    "properties": {
                        "input_tokens": {"type": "integer"},
                        "output_tokens": {"type": "integer"},
                    },
                },
                "cost": {"type": "number"},
            },
            "required": ["agent", "correlation", "trigger", "answers", "acted", "at"],
        }

    def td(self) -> dict[str, Any]:
        """The agent's Thing Description: its interface and its allowed answers."""
        decision = self._decision()
        td: dict[str, Any] = {
            "@context": "https://www.w3.org/2022/wot/td/v1.1",
            "id": f"{AGENT_ID}{self.name}",
            "title": self.title,
            "securityDefinitions": {"nosec_sc": {"scheme": "nosec"}},
            "security": ["nosec_sc"],
            "actions": {
                "decide": {
                    "description": (
                        "Answer the agent's questions about a trigger and take the steps "
                        "the answers call for. A trigger whose correlation was already "
                        "decided gets that decision again and runs nothing."
                    ),
                    "input": {"type": "object"},
                    "output": decision,
                    "forms": [{"href": "local://decide"}],
                },
                "ask": {
                    "description": "Read Things and answer. Never acts.",
                    "synchronous": True,
                    "input": {
                        "type": "object",
                        "properties": {"prompt": {"type": "string"}},
                        "required": ["prompt"],
                    },
                    "output": {"type": "object", "properties": {"answer": {"type": "string"}}},
                    "forms": [{"href": "local://ask"}],
                },
                "approve": {
                    "description": (
                        "A person's answer to the question the agent asked before taking "
                        "action, a tool name. It answers no question about another action."
                    ),
                    "input": {
                        "type": "object",
                        "properties": {
                            "person": {"type": "string"},
                            "yes": {"type": "boolean"},
                            "action": {"type": "string"},
                        },
                        "required": ["person", "yes", "action"],
                    },
                    "output": {"type": "object"},
                    "forms": [{"href": "local://approve"}],
                },
            },
            "events": {"decided": {"data": decision, "forms": [{"href": "local://decided"}]}},
        }
        if self.may:
            did = {
                "type": "object",
                "properties": {
                    "agent": {"const": self.name},
                    "correlation": {"type": "string"},
                    "task": {"type": "string"},
                    "answer": {"type": "string"},
                    "acted": {"type": "array", "items": {"type": "object"}},
                    "at": {"type": "string", "format": "date-time"},
                },
                "required": ["agent", "correlation", "task", "answer", "acted", "at"],
            }
            td["actions"]["do"] = {
                "description": (
                    "Carry out a task with the actions the agent may take, each checked "
                    "where it lands, some after a person approves. A repeated correlation "
                    "gets the first result again and runs nothing."
                ),
                "synchronous": True,
                "input": {
                    "type": "object",
                    "properties": {
                        "task": {"type": "string"},
                        "correlation": {"type": "string"},
                        "trigger": {"type": "object"},
                    },
                    "required": ["task"],
                },
                "output": did,
                "forms": [{"href": "local://do"}],
            }
            td["events"]["did"] = {"data": did, "forms": [{"href": "local://did"}]}
        return td
