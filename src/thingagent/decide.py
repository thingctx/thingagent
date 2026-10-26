# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Answers to an agent's questions, in the shapes of OpenAI's Decisions API.

A ``Decider`` turns a trigger and what the agent read into one answer per
question. Every answer is checked against the values the question lists: a value
outside them becomes a refusal, so no model output can name an action of its own.
Each refusal gives its ``reason``: ``model_refused``, ``not_a_choice`` or
``request_failed``. A chat model's stated confidence is passed through as
``confidence``; it is not a probability. The result also carries the token
``usage`` and the ``cost`` the model reports.
"""

from __future__ import annotations

import json
import logging
import math
import os
from typing import Any, Protocol

import httpx

from thingagent.agent import Agent, Question

_log = logging.getLogger("thingagent")


class Decider(Protocol):
    async def answer(
        self, agent: Agent, context: dict[str, Any], image: str | None
    ) -> dict[str, Any]:
        """``answers``, one per question in the shapes ``checked`` returns, and the
        ``usage`` and ``cost`` the model reports, as ``metered`` returns them."""
        ...


def refusal(question: Question, reason: str) -> dict[str, Any]:
    """A refusal to answer ``question``, and why."""
    return {"type": "refusal", "name": question.name, "reason": reason}


def checked(
    question: Question, raw: dict[str, Any] | None, reason: str = "model_refused"
) -> dict[str, Any]:
    """The answer to ``question`` in its Decisions shape, or a refusal: for
    ``reason`` when ``raw`` is missing, and ``not_a_choice`` when it names a value
    the question does not list."""
    if not raw:
        return refusal(question, reason)
    if question.type == "predicate":
        probability = raw.get("probability")
        if not isinstance(probability, int | float) or not 0 <= probability <= 1:
            return refusal(question, "not_a_choice")
        return {"type": "predicate", "name": question.name, "probability": float(probability)}
    value = raw.get(question.type)
    if value not in question.values:
        return refusal(question, "not_a_choice")
    answer = {"type": question.type, "name": question.name, question.type: value}
    confidence = raw.get("confidence")
    if isinstance(confidence, int | float):
        answer["confidence"] = float(confidence)
    return answer


def metered(answers: list[dict[str, Any]], usage: Any = None, cost: Any = None) -> dict[str, Any]:
    """A decider's result: the answers, plus the token counts in ``usage`` and the
    ``cost`` when the model reports them."""
    result: dict[str, Any] = {"answers": answers}
    tokens = {
        k: v
        for k, v in (usage if isinstance(usage, dict) else {}).items()
        if k in ("input_tokens", "output_tokens") and isinstance(v, int)
    }
    if tokens:
        result["usage"] = tokens
    if isinstance(cost, int | float):
        result["cost"] = float(cost)
    return result


class ChatDecider:
    """Answers with a chat model through litellm, asking for JSON and checking it.
    Needs the ``llm`` extra and the model's key in the environment."""

    async def answer(
        self, agent: Agent, context: dict[str, Any], image: str | None
    ) -> dict[str, Any]:
        # Optional dependencies: the package imports without a model library.
        import litellm  # noqa: PLC0415
        import openai  # noqa: PLC0415

        spec = [
            {
                "type": q.type,
                "name": q.name,
                "instructions": q.instructions,
                "values": list(q.values),
            }
            for q in agent.questions
        ]
        text = (
            f"{agent.instructions}\n\nAnswer each question. For a choice or a score, use one of "
            "its values exactly; for a predicate, give the probability it is true. Reply with "
            'JSON only: {"answers": [{"name": ..., "choice" or "score": ..., "probability": ..., '
            f'"confidence": ...}}]}}.\n\nQuestions: {json.dumps(spec)}\n\nWhat you know: '
            f"{json.dumps(context, default=str)}"
        )
        content: list[dict[str, Any]] = [{"type": "text", "text": text}]
        if image:
            content.append({"type": "image_url", "image_url": {"url": image}})
        try:
            reply = await litellm.acompletion(
                model=agent.model, messages=[{"role": "user", "content": content}], **agent.options
            )
        except openai.APIError as exc:
            _log.warning("Chat request failed for %s (%s)", agent.name, type(exc).__name__)
            policy = isinstance(exc, litellm.ContentPolicyViolationError)
            reason = "model_refused" if policy else "request_failed"
            return metered([checked(q, None, reason) for q in agent.questions])
        body = reply.choices[0].message.content or ""
        start, end = body.find("{"), body.rfind("}")
        try:
            raw = json.loads(body[start : end + 1]) if start >= 0 else {}
        except json.JSONDecodeError:
            raw = {}
        by_name = {a.get("name"): a for a in raw.get("answers", []) if isinstance(a, dict)}
        usage = getattr(reply, "usage", None)
        return metered(
            [checked(q, by_name.get(q.name)) for q in agent.questions],
            {
                "input_tokens": getattr(usage, "prompt_tokens", None),
                "output_tokens": getattr(usage, "completion_tokens", None),
            },
            getattr(usage, "cost", None),
        )


class DecisionsDecider:
    """Answers through OpenRouter's Decisions API, checking every returned value."""

    async def answer(
        self, agent: Agent, context: dict[str, Any], image: str | None
    ) -> dict[str, Any]:
        refused = metered([checked(q, None, "request_failed") for q in agent.questions])
        if not agent.questions:
            return refused
        key = os.environ.get(agent.options.get("api_key_env", "OPENROUTER_API_KEY"))
        if not key:
            _log.warning("Decisions API key is missing for %s", agent.name)
            return refused
        questions: dict[str, Any] = {}
        for q in agent.questions:
            question: dict[str, Any] = {
                "type": "noul" if q.type == "predicate" else q.type,
                "instructions": f"{agent.instructions}\n\n{q.instructions}",
            }
            if q.type == "choice":
                question["criteria"] = {value: value for value in q.values}
            elif q.type == "score":
                question["criteria"] = list(q.values)
            questions[q.name] = question
        state: Any = {"context": json.loads(json.dumps(context, default=str))}
        if image:
            # The model sees an image only as a chat style image_url part.
            state = [
                {"type": "text", "text": json.dumps(state)},
                {"type": "image_url", "image_url": {"url": image}},
            ]
        url = agent.options.get("api_base", "https://openrouter.ai/api/alpha/decisions")
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                reply = await client.post(
                    url,
                    headers={"Authorization": f"Bearer {key}"},
                    json={
                        "model": agent.model.removeprefix("decisions/"),
                        "state": state,
                        "questions": questions,
                    },
                )
                reply.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            _log.warning("Decisions request failed for %s (%s)", agent.name, type(exc).__name__)
            # The API reports a model's refusal to answer as a 502 that says so.
            if (
                isinstance(exc, httpx.HTTPStatusError)
                and exc.response.status_code == 502
                and "refused" in exc.response.text
            ):
                return metered([checked(q, None) for q in agent.questions])
            return refused
        try:
            body = reply.json()
        except ValueError:
            _log.warning("Decisions response is not JSON for %s", agent.name)
            return refused
        answers = body.get("answers") if isinstance(body, dict) else None
        if not isinstance(answers, dict):
            _log.warning("Decisions response has no answer map for %s", agent.name)
            return refused
        result = []
        for q in agent.questions:
            raw = answers.get(q.name)
            expected = "noul" if q.type == "predicate" else q.type
            if not isinstance(raw, dict) or raw.get("type") != expected:
                silent = raw is None or (isinstance(raw, dict) and raw.get("type") == "refusal")
                result.append(refusal(q, "model_refused" if silent else "not_a_choice"))
                continue
            if q.type == "predicate":
                raw = {"probability": raw.get("noul")}
            elif q.type == "score":
                # A score is an index into the values, fractional when the model is unsure.
                score = raw.get("score")
                label = None
                if isinstance(score, int | float) and math.isfinite(score):
                    label = dict(enumerate(q.values)).get(round(score))
                raw = {**raw, "score": label}
            result.append(checked(q, raw))
        usage = body.get("usage")
        return metered(result, usage, usage.get("cost") if isinstance(usage, dict) else None)
