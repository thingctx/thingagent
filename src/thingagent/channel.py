# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""A chat channel, served as a Thing.

A chat service adapter calls ``receive`` for each message a person writes and
delivers each ``message`` event that ``send`` emits. When a person has a question
pending, their next message is read as the answer, not as a new request.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from thingctx.bindings import LocalBinding

_log = logging.getLogger("thingagent")
YES = frozenset({"yes", "y", "ok", "approve", "approved"})


class ChatChannel:
    def __init__(self, name: str = "chat") -> None:
        self.name = name
        self.outbox: list[dict[str, str]] = []
        self.binding = LocalBinding({"send": self.send, "receive": self.receive})
        self._waiting: dict[str, asyncio.Future[str]] = {}
        # The action each open question asks about, by person.
        self._about: dict[str, str | None] = {}
        # One open question per person, so a reply cannot answer the wrong one.
        self._turns: dict[str, asyncio.Lock] = {}
        self._inbox: asyncio.Queue[dict[str, str]] = asyncio.Queue()

    def td(self) -> dict[str, Any]:
        text = {"person": {"type": "string"}, "text": {"type": "string"}}
        message = {"type": "object", "properties": text, "required": ["person", "text"]}
        done = {"type": "object"}
        return {
            "@context": "https://www.w3.org/2022/wot/td/v1.1",
            "id": f"urn:thingagent:channel:{self.name}",
            "title": f"Chat channel {self.name}",
            "securityDefinitions": {"nosec_sc": {"scheme": "nosec"}},
            "security": ["nosec_sc"],
            "actions": {
                "send": {"input": message, "output": done, "forms": [{"href": "local://send"}]},
                "receive": {
                    "input": message,
                    "output": done,
                    "forms": [{"href": "local://receive"}],
                },
            },
            "events": {
                "message": {
                    "description": "A message for a person, for an adapter to deliver.",
                    "data": message,
                    "forms": [{"href": "local://message"}],
                }
            },
        }

    async def send(self, person: str, text: str) -> dict[str, Any]:
        message = {"person": person, "text": text}
        self.outbox.append(message)
        self.binding.emit("message", message)
        return {"sent": True}

    async def receive(self, person: str, text: str) -> dict[str, Any]:
        waiting = self._waiting.pop(person, None)
        if waiting is not None and not waiting.done():
            waiting.set_result(text)
            return {"answered": True}
        self._inbox.put_nowait({"person": person, "text": text})
        return {"received": True}

    async def answer(self, person: str, yes: bool, action: str) -> dict[str, Any]:
        """Answer the question ``person`` has open, only when it asks about
        ``action``. Unlike ``receive``, it drops an answer with no open question, or
        one about another action, instead of reading it as a request; the question
        stays open."""
        waiting = self._waiting.get(person)
        if waiting is None or waiting.done():
            return {"answered": False}
        if self._about.get(person) != action:
            _log.warning(
                "thingagent: dropped an answer for %s about %s, not its question", person, action
            )
            return {"answered": False, "refused": f"{person}'s open question is not about {action}"}
        del self._waiting[person]
        waiting.set_result("yes" if yes else "no")
        return {"answered": True}

    async def messages(self) -> AsyncIterator[dict[str, str]]:
        """Every message that is not an answer, for the harness that routes them."""
        while True:
            yield await self._inbox.get()

    async def confirm(
        self, person: str, question: str, timeout: float, action: str | None = None
    ) -> bool:
        """Put ``question`` to ``person`` and wait for the reply. Only a yes within
        ``timeout`` seconds confirms; silence is a no. Questions to one person
        queue: a later one is asked once the earlier one is answered or times out,
        and then gets its own ``timeout``. Only ``answer`` naming ``action``, or the
        person's own reply, answers it."""
        async with self._turns.setdefault(person, asyncio.Lock()):
            future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
            self._waiting[person] = future
            self._about[person] = action
            await self.send(person, question)
            try:
                reply = await asyncio.wait_for(future, timeout)
            except asyncio.TimeoutError:
                _log.warning(
                    "thingagent: %s did not answer in %g s, so the answer is no", person, timeout
                )
                return False
            finally:
                if self._waiting.get(person) is future:
                    del self._waiting[person]
            return reply.strip().lower() in YES
