# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Five minutes with thingagent: one agent, a door and a thermostat in this
process, and no broker. The agent may lock the door, and must ask alice before it
unlocks it. thingagent's gate checks each call the agent makes and records it.

    python run.py          # a scripted model, no key needed
    python run.py MODEL    # a real model, with thingagent[llm] installed

Answer the question it asks with yes or no. Each record also goes to trace.jsonl.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from thingctx.bindings import LocalBinding

from thingagent import Agent, Harness

HERE = Path(__file__).parent
TASK = "Lock the front door. Then unlock it for the cleaner."


class House:
    """The devices the TDs in things/ describe, in this process."""

    def __init__(self) -> None:
        self.locked = False

    def lock(self) -> dict[str, bool]:
        self.locked = True
        return {"locked": True}

    def unlock(self) -> dict[str, bool]:
        self.locked = False
        return {"locked": False}

    def binding(self) -> LocalBinding:
        local = LocalBinding()
        local.register_thing(
            "door", {"locked": lambda: self.locked, "lock": self.lock, "unlock": self.unlock}
        )
        local.register_thing("thermostat", {"temperature": lambda: 21.5})
        return local


def _call(name: str) -> dict[str, Any]:
    call = {"id": f"call_{name}", "type": "function", "function": {"name": name, "arguments": "{}"}}
    return {"role": "assistant", "content": None, "tool_calls": [call]}


def scripted() -> Any:
    """A model that reads the door, locks it, then tries to unlock it."""
    turns = [
        _call("door__locked__get"),
        _call("door__lock"),
        _call("door__unlock"),
        {"role": "assistant", "content": "I locked the door and asked alice about unlocking it."},
    ]

    async def chat(_messages: list[dict[str, Any]], _tools: list[dict[str, Any]]) -> Any:
        return turns.pop(0)

    return chat


async def answer(harness: Harness, person: str) -> None:
    """Put each question the agent asks to the person at this terminal."""
    shown = 0
    while True:
        for message in harness.channel.outbox[shown:]:
            shown += 1
            try:
                reply = await asyncio.to_thread(input, f"{message['text']} ")
            except EOFError:
                reply = "no"
            await harness.channel.receive(person, reply)
        await asyncio.sleep(0.1)


async def main(model: str | None) -> None:
    raw = json.loads((HERE / "agents" / "keeper.json").read_text())
    agent = Agent.from_dict({**raw, "model": model} if model else raw)
    fleet = [json.loads(p.read_text()) for p in sorted((HERE / "things").glob("*.td.json"))]
    house = House()
    harness = Harness(
        [agent],
        fleet=fleet,
        bindings=[house.binding()],
        chat_fn=None if model else scripted(),
        trace=HERE / "trace.jsonl",
    )
    await harness.start()
    person = asyncio.ensure_future(answer(harness, "alice"))
    try:
        did = await harness.do(agent, TASK, person="alice")
    finally:
        person.cancel()
        await harness.aclose()
    print(did["answer"])
    for record in did["acted"]:
        kinds = record.get("@type", [])
        kind = " ".join(k.partition(":")[2] for k in ([kinds] if isinstance(kinds, str) else kinds))
        status = str(record.get("schema:actionStatus", "")).removeprefix("schema:")
        print(f"  {kind} {record.get('sosa:actsOnProperty', '')} {status}")
    print(f"The door is {'locked' if house.locked else 'unlocked'}.")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else None))
