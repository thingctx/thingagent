# thingagent

thingagent is a distributed runtime for AI agents in the physical world,
governed by policy. Every call an agent makes, to a robot arm, a lock or a
lamp, is decided by which agent is calling, which device, which operation, and
within which limits. Calls the policy refuses never reach the device.

Agents and the gateways in front of devices run on different hosts, joined by
an MQTT broker, or all in one process. Each agent is defined in a JSON file:
what it reads, what it may do, and within which limits. Every decision can be
recorded.

## Install

Python 3.10 or later.

    pip install thingagent

| Extra | Adds |
| --- | --- |
| `llm` | A model behind the agents, through litellm. |
| `ros` | ROS 2 robots behind rosbridge. |
| `mcp` | The `thingagent mcp` command. |

## First run

One heater and one agent, in this process, with a scripted model. It needs no
key, no broker and no network:

```python
import asyncio

from thingctx.bindings import LocalBinding

from thingagent import Agent, Harness

heater = {
    "@context": "https://www.w3.org/2022/wot/td/v1.1",
    "id": "urn:example:heater",
    "title": "Heater",
    "securityDefinitions": {"nosec_sc": {"scheme": "nosec"}},
    "security": ["nosec_sc"],
    "actions": {
        "set_target": {
            "input": {"type": "object", "properties": {"target": {"type": "number"}}},
            "output": {"type": "object"},
            "forms": [{"href": "local://set_target"}],
        }
    },
}

keeper = Agent.from_dict(
    {
        "name": "keeper",
        "instructions": "You look after the house.",
        "may": ["heater__set_target"],
        "limits": {
            "heater__set_target": {"properties": {"target": {"type": "number", "maximum": 24}}}
        },
    }
)

state = {"target": 18}
device = LocalBinding()
device.register_thing(
    "heater",
    {
        "set_target": lambda target: state.update(target=target) or {"target": target},
    },
)


def call(n, name, arguments):
    tool = {
        "id": f"call_{n}",
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }
    return {"role": "assistant", "content": None, "tool_calls": [tool]}


turns = [
    call(1, "heater__set_target", '{"target": 22}'),
    call(2, "heater__set_target", '{"target": 30}'),
    {"role": "assistant", "content": "Set the target to 22. The gate refused 30."},
]


async def scripted(messages, tools):
    return turns.pop(0)


async def main():
    harness = Harness([keeper], fleet=[heater], bindings=[device], chat_fn=scripted)
    await harness.start()
    try:
        did = await harness.do(keeper, "Warm the house up.")
    finally:
        await harness.aclose()
    print(did["answer"])
    for record in did["acted"]:
        status = record["schema:actionStatus"]
        print(
            f"  {record['sosa:actsOnProperty']} {status} {record.get('dct:description', '')}".rstrip()
        )
    print("target:", state["target"])


asyncio.run(main())
```

```
Set the target to 22. The gate refused 30.
  set_target schema:CompletedActionStatus
  set_target schema:FailedActionStatus arguments outside the grant's limits for ('urn:example:heater', 'set_target', 'invokeaction'): target: 30 is greater than the maximum of 24
target: 22
```

The first call ran. The second broke the agent's limit, so the gate refused it
and the heater never saw it. Each record in `acted` says what happened, in W3C
SOSA, PROV and schema.org terms. The `heater` dict is a W3C Thing Description;
thingagent reaches devices through
[thingctx](https://pypi.org/project/thingctx/), over HTTP, MQTT, ROS 2 or in
process as here. Give the agent a `"model"` litellm accepts, install the `llm`
extra, set the key, and drop `chat_fn` to put a real model behind it.

## An agent

Each agent is a JSON file. The keys: `name`; `model`; `instructions`; `reads`,
the properties it may read; `may`, the actions it may take on a task; `limits`,
a JSON Schema per action that its arguments must satisfy; `approve`, the
question its `person` must answer yes to before an action runs; `approval`, the
`timeout` and the `signal` and `clear` steps that show a question is waiting.
A numeric bound in `limits` needs a `type` beside it, or the agent does not load.

```json
{
  "name": "keeper",
  "instructions": "You look after the house. Check the door before you act.",
  "reads": ["door__locked", "thermostat__temperature"],
  "may": ["door__lock", "door__unlock"],
  "approve": {"door__unlock": "Unlock the front door?"},
  "person": "alice"
}
```

An agent runs in one of three modes. `do` runs a task: the model picks each
call and its arguments, only among `may`, inside `limits`, and waits for its
person on `approve`. `ask` answers a person from `reads` alone and cannot act.
`decide` answers fixed `questions` about a trigger, and each answer maps to
fixed steps in the `act` table; an answer outside the listed values is refused,
so the model cannot name an action of its own:

```json
{
  "name": "doorwatch",
  "model": "openrouter/google/gemma-4-31b-it",
  "questions": [{"type": "choice", "name": "who",
                 "values": ["resident", "delivery", "stranger", "unclear"]}],
  "act": {"who": {"stranger": [{"invoke": "door__lock"}]}},
  "reads": ["door__locked"],
  "image": "camera__snapshot",
  "person": "alice"
}
```

## Rules on device events

A rule names one device event (or an observable property), the values its data
must hold, and the agent action it calls: `<agent>__decide` with the event as
the trigger, `<agent>__do` with a `task` in `args`, or `<agent>__approve` with
a person's answer and the action it approves.

```json
{"name": "person_at_door", "event": "camera__person",
 "when": {"label": "person"}, "calls": "keeper__do",
 "args": {"task": "Someone is at the door. Lock it."}, "max_age": 30}
```

With `max_age`, an event whose time (at `time`, `at` by default) is missing or
older than that many seconds starts nothing, so an event the broker held while
thingagent was down does not move a device.

## The configuration file

Only `fleet` and `agents` are required. Paths are relative to the file. A
secret never goes in it: every `*_env` key names an environment variable.

```json
{"fleet": ["things"], "agents": ["agents/keeper.json"], "rules": ["rules/door.json"],
 "allow": {"alice": ["keeper"]}, "trace": "trace.jsonl"}
```

`fleet` holds TD files, folders of them or TD URLs, or `"bus"` with a `broker`
(`host:port`, or `mqtts://host:port` for TLS) to read the TDs the fleet's
gateways retain. `allow` maps a person to the agents they may chat with.
`trace` is a JSON Lines file of every task, answer, decision and record. The
logins, tokens, guard, policy and TLS keys for a broker are in the full README.

## Commands

    thingagent check CONFIG
    thingagent generate CONFIG --out DIR [--gateway GATEWAY]
    thingagent serve CONFIG
    thingagent chat CONFIG --as PERSON
    thingagent do CONFIG AGENT TASK [--as PERSON]
    thingagent ask CONFIG AGENT PROMPT
    thingagent gateway GATEWAY
    thingagent mcp CONFIG CLIENT

`check` reports an affordance the fleet lacks, a name used twice, a rule that
calls no agent action, and a broker setup that would let an agent act on an
open bus; `serve`, `chat`, `do`, `ask` and `mcp` refuse to start while it
reports a problem. `generate` writes `DIR/grants.json` for the fleet gateway
and `DIR/acl` for a Mosquitto broker. `serve` runs the agents and their rules
until stopped; `chat` adds a chat in this terminal, where a person in `allow`
writes `@keeper lock the door` to give the agent a task. `do` and `ask` run one
task or one prompt. With a broker, `gateway` serves the devices from its own
configuration file (`broker`, `things`, `login`, `guard`, `grants`, `trace`)
and `mcp` serves the fleet to an MCP client over stdio as one of the `clients`
in the configuration. An agent with a `model` needs the model's key in the
environment, and a litellm model needs the `llm` extra; `gateway` and `mcp`
need a broker.

## Python API

`thingagent` exports `Agent`, `Harness`, `Rule`, `check`, `grants`, the
deciders and `ChatChannel`.

- `Agent.from_dict(raw)` or `Agent.load(path)` reads an agent file; a limit or
  approval on an action the agent may not take raises `ValueError`. `agent.td()`
  is the Thing Description it is served under.
- `Rule.from_dict(raw)` or `Rule.load(path)`.
- `check(agents, rules, fleet)` returns every problem as a sentence; an empty
  list means none. `grants(agents, rules, fleet)` returns each name's least
  privilege grants, in the form thingctx's policy decision point takes.
- `Harness(agents, fleet=tds, bindings=..., rules=..., chat_fn=..., decider=...,
  trace=...)`, then `await harness.start()`. `await harness.do(agent, task,
  person=None)` returns the answer and the records in `acted`;
  `await harness.ask(agent, prompt)` returns `{"answer": ...}`;
  `await harness.decide(agent, trigger)` returns the answers and what ran.
  `await harness.channel.receive(person, text)` delivers a person's message or
  their yes or no. Close with `await harness.aclose()`. With `broker=`, the
  harness serves the agents on the bus and calls the fleet through its gateway.
- `Decider` is the protocol a `decide` agent answers through,
  `await answer(agent, context, image)`. `ChatDecider` asks a chat model through
  litellm; `DecisionsDecider` asks OpenRouter's Decisions API for a
  `decisions/...` model. `checked(question, raw)` turns an answer outside the
  question's values into a refusal.

## Docs and source

Full README, the fleet gateway, approvals, limits and `thingagent mcp`:
https://github.com/thingctx/thingagent

Apache-2.0
