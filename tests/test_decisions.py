# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any

import httpx
import jsonschema
import pytest
from conftest import DOORWATCH, FLEET

from thingagent import Agent, Harness, Question, Rule, check
from thingagent.cli import main
from thingagent.decide import ChatDecider, DecisionsDecider, metered

AGENT = Agent(
    "airwatch",
    "Air watch",
    "Humidity above 70 is alarming.",
    model="decisions/openai/gpt-6-luna-decisions",
    questions=(
        Question("predicate", "wet", "Is humidity above 70?"),
        Question("choice", "air", "Is the air normal or alarming?", ("normal", "alarming")),
        Question("score", "urgency", "Rate urgency.", ("low", "high")),
    ),
)


def refused(*reasons: str) -> dict[str, Any]:
    """Every question of AGENT refused, for one reason or one reason each."""
    each = reasons * len(AGENT.questions) if len(reasons) == 1 else reasons
    return {
        "answers": [
            {"type": "refusal", "name": q.name, "reason": r}
            for q, r in zip(AGENT.questions, each, strict=True)
        ]
    }


def mock_http(
    monkeypatch: pytest.MonkeyPatch, response: httpx.Response | httpx.RequestError
) -> list[httpx.Request]:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if isinstance(response, httpx.RequestError):
            raise response
        return response

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setattr(
        httpx, "AsyncClient", partial(httpx.AsyncClient, transport=httpx.MockTransport(handle))
    )
    return requests


async def test_request_carries_the_questions_context_and_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = mock_http(monkeypatch, httpx.Response(200, json={"answers": {}}))
    monkeypatch.setenv("DECISIONS_TEST_KEY", "other-key")
    agent = replace(
        AGENT,
        options={
            "api_base": "https://decisions.test/evaluate",
            "api_key_env": "DECISIONS_TEST_KEY",
        },
    )
    context = {"trigger": {"humidity": 82}}
    image = "data:image/png;base64,aW1hZ2U="

    await DecisionsDecider().answer(agent, context, image)

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == "https://decisions.test/evaluate"
    assert request.headers["authorization"] == "Bearer other-key"
    body = json.loads(request.content)
    assert body == {
        "model": "openai/gpt-6-luna-decisions",
        "state": [
            {"type": "text", "text": json.dumps({"context": context})},
            {"type": "image_url", "image_url": {"url": image}},
        ],
        "questions": {
            "wet": {
                "type": "noul",
                "instructions": f"{agent.instructions}\n\n{agent.questions[0].instructions}",
            },
            "air": {
                "type": "choice",
                "instructions": f"{agent.instructions}\n\n{agent.questions[1].instructions}",
                "criteria": {"normal": "normal", "alarming": "alarming"},
            },
            "urgency": {
                "type": "score",
                "instructions": f"{agent.instructions}\n\n{agent.questions[2].instructions}",
                "criteria": ["low", "high"],
            },
        },
    }


async def test_valid_answers_are_checked_in_question_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = mock_http(
        monkeypatch,
        httpx.Response(
            200,
            json={
                "answers": {
                    "air": {"type": "choice", "choice": "alarming", "confidence": 0.99},
                    "urgency": {
                        "type": "score",
                        "score": 0.97,
                        "legend": {"0": "low", "1": "high"},
                        "confidence": 0.9,
                    },
                    "wet": {"type": "noul", "noul": 0.98},
                    "unknown": {"type": "choice", "choice": "unexpected"},
                },
                "usage": {"input_tokens": 614, "output_tokens": 0, "cost": 6.14e-05},
            },
        ),
    )

    answers = await DecisionsDecider().answer(AGENT, {"trigger": {"humidity": 82}}, None)

    assert answers == {
        "answers": [
            {"type": "predicate", "name": "wet", "probability": 0.98},
            {"type": "choice", "name": "air", "choice": "alarming", "confidence": 0.99},
            {"type": "score", "name": "urgency", "score": "high", "confidence": 0.9},
        ],
        "usage": {"input_tokens": 614, "output_tokens": 0},
        "cost": 6.14e-05,
    }
    assert str(requests[0].url) == "https://openrouter.ai/api/alpha/decisions"
    assert json.loads(requests[0].content)["state"] == {"context": {"trigger": {"humidity": 82}}}


NOT = "not_a_choice"
SILENT = "model_refused"


@pytest.mark.parametrize(
    ("answers", "reasons"),
    [
        (
            {
                "wet": {"type": "noul", "noul": 1.5},
                "air": {"type": "choice", "choice": "invented"},
                "urgency": {"type": "score", "score": 1.6},
            },
            (NOT,),
        ),
        (
            {
                "wet": {"type": "noul", "noul": "yes"},
                "air": {"type": "choice"},
                "urgency": {"type": "score", "score": "high"},
            },
            (NOT,),
        ),
        (
            {
                "wet": {"type": "refusal", "noul": 0.9},
                "air": {"type": "refusal", "choice": "normal"},
                "urgency": {"type": "choice", "score": "high"},
            },
            (SILENT, SILENT, NOT),
        ),
        ({"wet": None, "air": "alarming", "urgency": []}, (SILENT, NOT, NOT)),
        ({}, (SILENT,)),
    ],
)
async def test_unlisted_missing_or_wrongly_typed_answers_are_refusals(
    monkeypatch: pytest.MonkeyPatch, answers: dict[str, Any], reasons: tuple[str, ...]
) -> None:
    mock_http(monkeypatch, httpx.Response(200, json={"answers": answers}))

    assert await DecisionsDecider().answer(AGENT, {}, None) == refused(*reasons)


@pytest.mark.parametrize("status", [302, 400, 401, 429, 500, 502, 503])
async def test_http_errors_refuse_every_question(
    monkeypatch: pytest.MonkeyPatch, status: int, caplog: pytest.LogCaptureFixture
) -> None:
    mock_http(monkeypatch, httpx.Response(status))

    assert await DecisionsDecider().answer(AGENT, {}, None) == refused("request_failed")
    assert "Decisions request failed" in caplog.text
    assert "test-key" not in caplog.text


async def test_a_model_refusal_is_not_a_failed_request(monkeypatch: pytest.MonkeyPatch) -> None:
    error = {"error": {"message": 'OpenAI refused to answer question "air"', "code": 502}}
    mock_http(monkeypatch, httpx.Response(502, json=error))

    assert await DecisionsDecider().answer(AGENT, {}, None) == refused(SILENT)


@pytest.mark.parametrize("error", [httpx.ConnectError("offline"), httpx.ReadTimeout("timeout")])
async def test_transport_errors_refuse_every_question(
    monkeypatch: pytest.MonkeyPatch, error: httpx.RequestError
) -> None:
    mock_http(monkeypatch, error)

    assert await DecisionsDecider().answer(AGENT, {}, None) == refused("request_failed")


@pytest.mark.parametrize("body", ["not JSON", "null", "[]", "{}", '{"answers": []}'])
async def test_malformed_responses_refuse_every_question(
    monkeypatch: pytest.MonkeyPatch, body: str
) -> None:
    mock_http(monkeypatch, httpx.Response(200, text=body))

    assert await DecisionsDecider().answer(AGENT, {}, None) == refused("request_failed")


async def test_a_missing_key_refuses_without_a_request(monkeypatch: pytest.MonkeyPatch) -> None:
    requests = mock_http(monkeypatch, httpx.Response(200))
    monkeypatch.delenv("OPENROUTER_API_KEY")

    assert await DecisionsDecider().answer(AGENT, {}, None) == refused("request_failed")
    assert requests == []


async def test_no_questions_need_no_request(monkeypatch: pytest.MonkeyPatch) -> None:
    requests = mock_http(monkeypatch, httpx.Response(200))

    assert await DecisionsDecider().answer(replace(AGENT, questions=()), {}, None) == {
        "answers": []
    }
    assert requests == []


CHAT = replace(AGENT, name="chat", model="openrouter/google/gemma-4-31b-it")


def mock_chat(monkeypatch: pytest.MonkeyPatch, reply: Any) -> None:
    import litellm

    async def acompletion(**_kwargs: Any) -> Any:
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(litellm, "acompletion", acompletion)


async def test_a_chat_answer_carries_the_usage_and_cost_the_model_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import litellm

    text = json.dumps(
        {
            "answers": [
                {"name": "wet", "probability": 0.9},
                {"name": "air", "choice": "stormy"},
            ]
        }
    )
    usage = {"prompt_tokens": 400, "completion_tokens": 30, "total_tokens": 430, "cost": 0.0002}
    reply = litellm.ModelResponse(
        choices=[{"message": {"role": "assistant", "content": text}}], usage=usage
    )
    mock_chat(monkeypatch, reply)

    assert await ChatDecider().answer(CHAT, {}, None) == {
        "answers": [
            {"type": "predicate", "name": "wet", "probability": 0.9},
            {"type": "refusal", "name": "air", "reason": NOT},
            {"type": "refusal", "name": "urgency", "reason": SILENT},
        ],
        "usage": {"input_tokens": 400, "output_tokens": 30},
        "cost": 0.0002,
    }


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        ("ContentPolicyViolationError", SILENT),
        ("APIConnectionError", "request_failed"),
        ("ServiceUnavailableError", "request_failed"),
    ],
)
async def test_a_failed_chat_request_refuses_every_question(
    monkeypatch: pytest.MonkeyPatch, error: str, reason: str, caplog: pytest.LogCaptureFixture
) -> None:
    import litellm

    exc = getattr(litellm, error)(message="no", llm_provider="openrouter", model=CHAT.model)
    mock_chat(monkeypatch, exc)

    assert await ChatDecider().answer(CHAT, {}, None) == refused(reason)
    assert "Chat request failed" in caplog.text


async def test_the_decision_and_decided_event_carry_reasons_usage_and_cost() -> None:
    answers = [{"type": "refusal", "name": q.name, "reason": NOT} for q in AGENT.questions]

    class Metered:
        async def answer(self, *_args: Any) -> dict[str, Any]:
            return metered(answers, {"input_tokens": 9, "output_tokens": 1}, 0.25)

    harness = Harness([AGENT], fleet=[], decider=Metered())
    await harness.start()
    emitted: list[tuple[str, dict[str, Any]]] = []
    harness._bindings[AGENT.name].emit = lambda *e: emitted.append(e)  # type: ignore[method-assign]
    try:
        decision = await harness.decide(AGENT, {"humidity": 82})
    finally:
        await harness.aclose()

    assert decision["answers"] == answers
    assert decision["usage"] == {"input_tokens": 9, "output_tokens": 1}
    assert decision["cost"] == 0.25
    assert emitted == [("decided", decision)]
    jsonschema.validate(decision, AGENT.td()["events"]["decided"]["data"])


async def test_harness_selects_per_agent_and_preserves_the_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    async def answer(
        self: ChatDecider | DecisionsDecider,
        agent: Agent,
        context: dict[str, Any],
        image: str | None,
    ) -> dict[str, Any]:
        calls.append((type(self), agent.name, context, image))
        return {"answers": []}

    monkeypatch.setattr(ChatDecider, "answer", answer)
    monkeypatch.setattr(DecisionsDecider, "answer", answer)
    chat = CHAT
    for override in (None, ChatDecider()):
        harness = Harness([AGENT, chat], fleet=[], decider=override)
        await harness.start()
        try:
            await harness.decide(AGENT, {"humidity": 82})
            await harness.decide(chat, {"humidity": 50})
        finally:
            await harness.aclose()

    assert calls == [
        (DecisionsDecider, "airwatch", {"trigger": {"humidity": 82}}, None),
        (ChatDecider, "chat", {"trigger": {"humidity": 50}}, None),
        (ChatDecider, "airwatch", {"trigger": {"humidity": 82}}, None),
        (ChatDecider, "chat", {"trigger": {"humidity": 50}}, None),
    ]


DECIDER = DOORWATCH | {"model": "decisions/openai/gpt-6-luna-decisions"}
ONLY = "doorwatch has a decisions model, which can only decide,"
DEMO = Path(__file__).resolve().parents[2] / "watcher" / "demo"


def test_check_passes_a_decisions_agent_that_only_decides() -> None:
    assert check([Agent.from_dict(DECIDER)], [], FLEET, allow={"alice": ["advisor"]}) == []


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"may": ["door__lock"]}, f"{ONLY} but it may take actions on a task"),
        ({"questions": [], "act": {}}, f"{ONLY} but it has no questions"),
    ],
)
def test_check_reports_a_decisions_agent_that_would_chat(
    change: dict[str, Any], problem: str
) -> None:
    assert check([Agent.from_dict(DECIDER | change)], [], FLEET) == [problem]


def test_check_reports_each_person_who_may_chat_with_a_decisions_agent_once() -> None:
    allow = {"alice": ["doorwatch", "doorwatch"], "bob": ["doorwatch"], "carol": ["advisor"]}

    assert check([Agent.from_dict(DECIDER)], [], FLEET, allow=allow) == [
        f"{ONLY} but alice may chat with it",
        f"{ONLY} but bob may chat with it",
    ]
    assert check([Agent.from_dict(DOORWATCH)], [], FLEET, allow=allow) == []


@pytest.mark.skipif(not DEMO.is_dir(), reason="needs the watcher demo checked out beside")
def test_the_watcher_demo_airwatch_passes_check() -> None:
    airwatch = Agent.load(DEMO / "agents" / "airwatch.json")
    rule = Rule.from_dict(json.loads((DEMO / "rules" / "air_reading.json").read_text()))
    watcher = json.loads((DEMO / "sim" / "watcher.td.json").read_text())

    assert airwatch.model.startswith("decisions/")
    assert check([airwatch], [rule], [watcher]) == []


@pytest.mark.parametrize("command", [["ask", "Is the air dry?"], ["do", "Turn the light red."]])
def test_the_command_refuses_to_chat_with_a_decisions_agent(
    tmp_path: Path, command: list[str]
) -> None:
    (tmp_path / "airwatch.json").write_text(json.dumps(DECIDER | {"name": "airwatch"}))
    config = {"broker": "127.0.0.1:1883", "fleet": "bus", "agents": ["airwatch.json"]}
    (tmp_path / "config.json").write_text(json.dumps(config))

    with pytest.raises(SystemExit, match="airwatch has a decisions model, so it can only decide"):
        main([command[0], str(tmp_path / "config.json"), "airwatch", command[1]])


def test_the_check_command_reads_who_may_chat(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "fleet").mkdir()
    for td in FLEET:
        (tmp_path / "fleet" / f"{td['id'].rsplit(':', 1)[-1]}.json").write_text(json.dumps(td))
    (tmp_path / "doorwatch.json").write_text(json.dumps(DECIDER))
    config = {"fleet": "fleet", "agents": ["doorwatch.json"], "allow": {"alice": ["doorwatch"]}}
    (tmp_path / "config.json").write_text(json.dumps(config))

    assert main(["check", str(tmp_path / "config.json")]) == 1
    assert capsys.readouterr().out == f"{ONLY} but alice may chat with it\n"


async def test_the_harness_refuses_to_chat_with_a_decisions_agent() -> None:
    harness = Harness([AGENT], fleet=[])
    await harness.start()
    try:
        for call in (
            partial(harness.ask, AGENT, "Is the air dry?"),
            partial(harness.do, AGENT, "Turn the light red."),
        ):
            with pytest.raises(ValueError, match="airwatch has a decisions model"):
                await call()
    finally:
        await harness.aclose()
