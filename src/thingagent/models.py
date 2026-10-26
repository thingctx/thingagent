# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Calls to an agent's model through litellm, with the agent's own settings.

An agent's ``options`` go to litellm with every call (a local model's context size
or temperature, a hosted one's timeout). An empty reply (no text and no tool call),
which a small model gives when it runs out of room, is retried once. Tool results
reach the model compacted (:func:`compact`); the call record keeps the full result.
"""

from __future__ import annotations

from typing import Any

from thingctx.contrib.llm import ChatFn

_NUDGE = {
    "role": "user",
    "content": "Your reply was empty. Make the next tool call, or answer if you are done.",
}
# Past these, a value costs the model context without telling it more.
_ITEMS = 16
_CHARACTERS = 2000


def chat_fn(model: str, options: dict[str, Any]) -> ChatFn:
    """A chat function for ``thingctx.contrib.llm.LLMHost`` that calls ``model``
    through litellm with ``options``. Needs the ``llm`` extra."""

    async def chat(messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        # Optional dependency: the package imports without a model library.
        import litellm  # noqa: PLC0415

        calls: list[Any]
        content, calls = "", []
        for sent in (messages, [*messages, _NUDGE]):
            reply = await litellm.acompletion(
                model=model,
                messages=sent,
                tools=tools or None,
                tool_choice="auto" if tools else None,
                **options,
            )
            message = reply.choices[0].message
            content, calls = message.content or "", message.tool_calls or []
            if calls or content.strip():
                break
        answer: dict[str, Any] = {"role": "assistant", "content": content or None}
        if calls:
            answer["tool_calls"] = [
                {
                    "id": call.id,
                    "type": "function",
                    "function": {"name": call.function.name, "arguments": call.function.arguments},
                }
                for call in calls
            ]
        else:
            answer["content"] = content
        return answer

    return chat


def compact(value: Any) -> Any:
    """``value`` as a model should read it: numbers to four decimals, long lists and
    strings cut short with a note of how much was left out, and empty fields
    dropped."""
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, str) and len(value) > _CHARACTERS:
        return f"{value[:_CHARACTERS]}... ({len(value)} characters in all)"
    if isinstance(value, list):
        kept = [compact(item) for item in value[:_ITEMS]]
        return kept + ([f"... {len(value) - _ITEMS} more"] if len(value) > _ITEMS else [])
    if isinstance(value, dict):
        out = {key: compact(item) for key, item in value.items()}
        return {key: item for key, item in out.items() if item not in (None, [], {})}
    return value
