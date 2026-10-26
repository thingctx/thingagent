# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""What reaches an agent's model: its own settings with every call, one more try
after an empty reply, and tool results it can afford to read."""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

from thingagent.models import chat_fn, compact


def _reply(content: str | None, calls: list[Any] | None = None) -> Any:
    message = SimpleNamespace(content=content, tool_calls=calls)
    return SimpleNamespace(choices=[SimpleNamespace(message=message)])


class _Litellm:
    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.sent: list[dict[str, Any]] = []

    async def acompletion(self, **kwargs: Any) -> Any:
        self.sent.append(kwargs)
        return self.replies.pop(0)


async def test_an_agents_options_go_with_every_call(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _Litellm(_reply("Done."))
    monkeypatch.setitem(sys.modules, "litellm", fake)
    chat = chat_fn("ollama_chat/qwen3", {"temperature": 0, "think": False})
    answer = await chat([{"role": "user", "content": "go"}], [])
    assert answer == {"role": "assistant", "content": "Done."}
    assert fake.sent[0]["temperature"] == 0
    assert fake.sent[0]["think"] is False
    assert fake.sent[0]["model"] == "ollama_chat/qwen3"


async def test_an_empty_reply_is_asked_for_once_more(monkeypatch: pytest.MonkeyPatch) -> None:
    call = SimpleNamespace(id="c1", function=SimpleNamespace(name="door__lock", arguments="{}"))
    fake = _Litellm(_reply(""), _reply(None, [call]))
    monkeypatch.setitem(sys.modules, "litellm", fake)
    answer = await chat_fn("m", {})(
        [{"role": "user", "content": "lock it"}], [{"type": "function"}]
    )
    assert answer["tool_calls"][0]["function"] == {"name": "door__lock", "arguments": "{}"}
    assert answer["content"] is None
    assert fake.sent[1]["messages"][-1]["content"].startswith("Your reply was empty.")


def test_a_result_is_compacted_for_the_model() -> None:
    result = {
        "pose": {"x": 0.123456789, "y": None, "z": 1.0},
        "points": list(range(40)),
        "image": "x" * 5000,
        "empty": {},
        "ok": True,
    }
    assert compact(result) == {
        "pose": {"x": 0.1235, "z": 1.0},
        "points": [*range(16), "... 24 more"],
        "image": "x" * 2000 + "... (5000 characters in all)",
        "ok": True,
    }
