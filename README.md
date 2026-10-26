# thingagent

thingagent is a distributed runtime for AI agents in the physical world, governed
by policy. Every call an agent makes, to a robot arm, a lock or a lamp, is decided
by which agent is calling, which device, which operation, and within which limits.
Calls the policy refuses never reach the device.

Agents and the gateways in front of devices run on different hosts, joined by an
MQTT broker, or all in one process. Each agent has its own identity and is defined
in a JSON file: what it reads, what it may do, and within which limits. Every
decision can be recorded.

Devices are described with W3C Web of Things Thing Descriptions (TDs) and reached
over HTTP, MQTT or ROS 2. [thingctx](https://github.com/thingctx/thingctx) projects
those descriptions into an agent's tools, makes each decision, and records it as a
W3C SOSA record.

thingagent is alpha. Commands and file formats may change before 1.0.

## Install

Python 3.10 or later.

```
pip install thingagent
```

| Extra | Adds |
| --- | --- |
| `llm` | A model behind the agents, through litellm. |
| `ros` | ROS 2 robots behind rosbridge. |
| `mcp` | The `thingagent mcp` command. |
| `dev` | Tests and linters. |

## A five minute example

`examples/quickstart` holds a door, a thermostat and one agent, in one process with
no broker and no model key.

```
git clone https://github.com/thingctx/thingagent
cd thingagent/examples/quickstart
pip install thingagent
thingagent check thingagent.json
python run.py
```

The agent may lock and unlock the door, and must ask alice before it unlocks:

```json
{
  "name": "keeper",
  "title": "House keeper",
  "instructions": "You look after the house. Check the door before you act. Make one tool call per turn.",
  "reads": ["door__locked", "thermostat__temperature"],
  "may": ["door__lock", "door__unlock"],
  "approve": {"door__unlock": "Unlock the front door?"},
  "person": "alice"
}
```

A scripted model reads the door, locks it, then tries to unlock it. Answer no:

```
Unlock the front door? (door__unlock {}) yes or no? no
I locked the door and asked alice about unlocking it.
  Actuation Action lock CompletedActionStatus
  Action Activity unlock FailedActionStatus
The door is locked.
```

The lock ran; the unlock was refused. `trace.jsonl` holds the read, both calls and
their records. With the `llm` extra and the model's key in the environment,
`python run.py MODEL` uses a real model, MODEL being any model name litellm accepts.

## Three modes

An agent is a Thing. It runs in one of three modes; how it is started (below) picks
which:

| Mode | What the agent does |
| --- | --- |
| `decide` | Answers fixed questions. Each answer maps to fixed steps in the agent's `act` table. The model cannot name an action of its own. |
| `ask` | Answers a person from the properties it reads. It cannot act. |
| `do` | Carries out a task. The model picks each call and its arguments, only among the actions in `may`. |

A `decide` agent:

```json
{
  "name": "doorwatch",
  "questions": [{"type": "choice", "name": "who",
                 "values": ["resident", "delivery", "stranger", "unclear"]}],
  "act": {"who": {"stranger": [{"invoke": "door__lock"}, {"invoke": "alarm__sound"}],
                  "delivery": [{"invoke": "door__lock"}]}},
  "reads": ["door__locked"],
  "image": "camera__snapshot",
  "person": "alice"
}
```

An agent that uses a model names it in `model`. An answer outside the listed values
is refused, with a reason. A table step sends only the arguments it names, so a step
that opens valve 3 cannot open valve 9.

`limits` bounds an action's arguments with a JSON Schema, such as
`{"plant__open_valve": {"properties": {"valve": {"type": "integer", "maximum": 4}}}}`.
A numeric bound needs a `type` beside it, or the agent does not load, because a
string would pass it. A call outside it is refused, naming the value that broke it. thingctx checks the bound in process
without a broker and at the fleet gateway with one. A broker's topic rules cannot
enforce it.

An agent starts in one of three ways. Each runs the agent's `model` and needs its
key in the environment; a litellm model also needs the `llm` extra. The agents
named below live in `examples/` and `examples/ur5e`, not in the quickstart.

1. A person writes to it: `thingagent chat thingagent.json --as alice`, then
   `@advisor is the house warm?`. An agent with `may` actions takes the message as
   a task.
2. A device event matches a rule, which calls the agent:

   ```json
   {"name": "person_at_door", "event": "camera__person",
    "when": {"label": "person"}, "calls": "doorwatch__decide", "max_age": 30}
   ```

   With `max_age`, an event whose time is missing, unreadable or more than that
   many seconds old is dropped and logged. The time is at `time`, a dotted name
   (`at` by default), as ISO 8601 with a zone or Unix seconds. Set it on every rule
   whose agent may act, so an event the broker held while thingagent was down does
   not move a device.

3. A command runs one task or one question:
   `thingagent do thingagent.json picker "Put the part in the tray." --as alice` or
   `thingagent ask thingagent.json advisor "Is the house warm?"`.

`thingagent serve thingagent.json` runs the agents and their rules until stopped.
Every `decide` and `do` carries a correlation id, which its `decided` or `did` event
reports and a rule passes on; a repeat of one id within 10 minutes runs nothing, so
one agent's event delivered twice starts the next agent once. A device event has an
id only if the device sends one in `correlation`; otherwise each delivery is a new
trigger.

## Approvals

`approve` maps an action to the question its person must answer first. The call
waits; only a yes within the timeout lets it run, and silence is a no. In `do`, a
refused call is recorded like any other. In `decide`, the decision reports the step
as declined.

`approval` sets the timeout and the steps that show a question is waiting and
answered, such as a lamp:

```json
"approval": {"timeout": 20,
             "signal": [{"invoke": "lamp__light", "args": {"color": "amber"}}],
             "clear": [{"invoke": "lamp__light", "args": {"color": "off"}}]}
```

A rule can carry an answer from a device, such as a knob a person presses:

```json
{"name": "knob_says_yes", "event": "watcher__knob", "when": {"pressed": true},
 "calls": "keeper__approve",
 "args": {"person": "alice", "yes": true, "action": "door__unlock"}}
```

The rule must name the action it approves, and answers only a question about that
action. A person who replies in chat answers their own open question.

## The fleet gateway

With no broker, thingagent runs the policy check in its own process. With a broker,
the devices sit behind a fleet gateway:

```
thingagent gateway gateway.json
```

```json
{"broker": "127.0.0.1:1885", "things": ["things"],
 "login": {"user": "fleet_gw", "password_env": "FLEET_GW_PASSWORD"},
 "guard": {"issuers": ["https://issuer.example.com"], "audience": "api://fleet",
           "jwks": "keys/jwks.json"},
 "grants": "policy/grants.json", "trace": "record/gateway.jsonl"}
```

`FLEET_GW_PASSWORD` is the name of an environment variable. Every `*_env` key
names the variable that holds a secret, never the secret; any other value is
refused. `broker`, `tls`, `insecure_bus` and `verify_broker` work as in the table
below. Each agent logs in to the broker under its own user and sends its own token. The gateway checks the
token against its grants, runs or refuses the call, and writes the SOSA record to
`trace`, with the caller's name.

`examples/ur5e` shows this for a UR5e arm: a picker that may move it, and a
visitor the owner grants no motion.

## Generate and check

```
thingagent check thingagent.json
thingagent generate thingagent.json --gateway gateway.json --out policy
```

`check` reports an affordance the fleet does not have, a name two agents share, and
a configuration that would let an agent act on an open bus. `serve`, `chat`, `do`,
`ask` and `mcp` will not start while it reports a problem.

`generate` writes two files from the same agents, rules and fleet:
`policy/grants.json` for the gateway and `policy/acl` for a Mosquitto broker. Each
agent may publish only the requests its grants allow, so the broker refuses any
other before the gateway sees it. Reload the broker after writing the file.

`policy/acl` is in Mosquitto's format. To check another broker, set
`"verify_broker": true`. Before any agent acts, each login sends a request on every
request topic the fleet has under the next login's name, and thingagent starts only
if the broker refuses every one. That catches an open broker, a login that may write
any topic, and a rule that leaves the name open or names the next login. It does
not test a name further down the list. A broker that lets a probe through hands
the gateway a request with no token, which it refuses before running anything. `serve`, `chat`, `do` and `ask` run the check; `thingagent gateway`
checks its own login the same way, under a name no login has; `thingagent mcp`
does not run it.

A configuration needs only `fleet` and `agents`. The other keys:

| Key | What it adds |
| --- | --- |
| `rules` | Rules that call agents on device events. |
| `broker`, `prefix` | Reach the fleet through its gateways on the bus. `"fleet": "bus"` reads the TDs they retain. `mqtts://host:port` uses TLS. |
| `tls` | `{"ca", "cert", "key"}`, each optional: TLS to the broker, verified with this CA file or the system's, with this client certificate and its key, which must not have a passphrase. |
| `insecure_bus` | Use a broker off this machine without TLS, accepting that secrets cross the network in the clear. |
| `verify_broker` | Start only if the broker refuses each login every request topic under the next login's name. |
| `name` | The harness's name, in its chat channel's id. |
| `token_env`, `logins`, `login` | Each agent and rule calls and logs in as itself; thingagent logs in as `login`. Each login is `{"user", "password_env"}`. |
| `guard`, `policy`, `broker_binds_identity` | Protect the agents' own Things. An agent that can act needs all three on a broker. |
| `open_bus` | Run on a bus without them, accepting that anyone on it can act as an agent. |
| `gateways`, `callers` | For `generate`: the fleet gateways' broker users, and other users that may send requests under their own name. |
| `clients` | Identities that call the fleet through `thingagent mcp`. |
| `allow` | Who may write to which agent. |
| `timeout`, `trace` | How long a call may take, and a JSON Lines file of every task, answer and record. |
| `grants` | With no broker, the owner's `grants.json`, used in place of the grants the agents give. |

## Clients and thingagent mcp (optional)

```
pip install 'thingagent[mcp]'
thingagent mcp thingagent.json assistant
```

serves the fleet over stdio to an MCP client, as the client named `assistant`:

```json
"clients": [{"name": "assistant", "reads": ["thermostat__temperature"], "may": ["door__lock"]}],
"token_env": {"assistant": "ASSISTANT_TOKEN"},
"logins": {"assistant": {"user": "assistant", "password_env": "ASSISTANT_PASSWORD"}}
```

The client sees what it reads as resources and the actions it may take as tools.
Each call goes to the fleet gateway with the client's token and login (again, names
of environment variables). An action the TD marks as needing approval asks the
person through the MCP client, or returns a token for the `approve` tool.
`thingagent mcp` needs a broker. Run `thingagent generate` again after adding a
client.

## Limits

Read these before trusting thingagent with a device.
[docs/LIMITS.md](https://github.com/thingctx/thingagent/blob/main/docs/LIMITS.md)
has the rest.

- With no broker, the policy check runs in the same process as the model that
  reads untrusted input, and cannot survive a compromise of that process.
- In `do`, text the model reads can steer it, but only among its `may` actions and
  inside their `limits`; put an action in `approve` when its worst use is not
  acceptable.
- On a broker, the generated `policy/acl` must stop each login publishing under
  another's name, or anyone on the bus can replay a token read there.
  `broker_binds_identity` only attests this; `verify_broker` tests it at start.

Report a security problem as
[SECURITY.md](https://github.com/thingctx/thingagent/blob/main/SECURITY.md) says.
To run the tests, see
[CONTRIBUTING.md](https://github.com/thingctx/thingagent/blob/main/CONTRIBUTING.md).

## License

Apache-2.0. See [LICENSE](https://github.com/thingctx/thingagent/blob/main/LICENSE).
