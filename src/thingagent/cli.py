# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""The ``thingagent`` command.

``thingagent check CONFIG`` checks the agents and rules against the fleet's TDs.
``thingagent serve CONFIG`` runs the agents and their rules until stopped.
``thingagent chat CONFIG --as PERSON`` does the same with a chat in this terminal.
``thingagent do CONFIG AGENT TASK`` runs until one agent has done one task.
``thingagent ask CONFIG AGENT PROMPT`` runs until one agent has answered from what
it can read. ``thingagent generate CONFIG --out DIR`` writes the fleet gateway's
grants and the broker's topic rules from the same agents, rules and fleet (or, with
``--gateway``, the Things a fleet gateway serves). ``thingagent gateway GATEWAY``
serves the fleet's devices on the bus with those grants (see :func:`_serve_fleet`).
``thingagent mcp CONFIG CLIENT`` serves the fleet to an MCP client over stdio, as
that client (see :func:`serving`).

CONFIG is a JSON file. Paths in it are relative to it. It never holds a secret:
each ``*_env`` key names the environment variable that holds one, in letters,
digits and _, not starting with a digit. Only ``fleet`` and ``agents`` are
required::

    {"fleet": "tds", "agents": ["agents/agent.json"], "rules": ["rules/rule.json"]}

Without a broker, ``fleet`` holds the devices' own TDs, reached over HTTP, MQTT or
rosbridge, or in process through handlers an installed package registers under
thingctx's ``thingctx.local_handlers`` entry points, and the gate runs here. Each
optional entry adds one thing:

``broker`` (``host:port``, or ``mqtts://host:port`` for TLS)
    The fleet is reached through its gateways on the bus and the agents are served
    there; ``fleet`` may then be ``"bus"``, the TDs the gateways retain at start,
    apart from any harness's agents and chat channels.
``tls``
    ``{"ca", "cert", "key"}``, each optional: every connection to the broker uses
    TLS, verified with the CA file ``ca`` or else the system's, presenting the
    client certificate ``cert`` and its key ``key`` when given.
``insecure_bus``
    Use a broker that is not on this machine (127.0.0.0/8, ::1, localhost) without
    TLS. Every password and token then crosses the network in the clear.
``name``
    The harness's name, in its chat channel's id, so harnesses on one bus do not
    collide. It is thingagent's login user when not given.
``token_env``
    Each agent and rule calls as itself: a map from its name to the environment
    variable that holds its token, read once at start.
``logins``, ``login``
    Each agent and rule logs in to the broker under its own user, and thingagent
    under its own. ``logins`` maps an agent or rule name to ``{"user",
    "password_env"}`` and ``login`` is thingagent's. A user must be its token's
    ``sub``. A guarded gateway needs them. In the broker's rules, thingagent's user
    serves the agents and the chat channel, and reads the TDs.
``guard``, ``broker_binds_identity``, ``policy``
    The agents' Things are protected like any Thing a thingctx gateway serves:
    ``guard`` holds ``issuers``, ``audience`` and ``jwks_url``, or ``jwks``, a JWKS
    file, and ``policy`` maps a role to grants as ``grants.json`` holds them. An agent
    that can act runs on a broker only when ``broker_binds_identity`` is true, it
    has a login in ``logins``, and ``guard`` and ``policy`` are set.
``grants``
    Without a broker, the grants file ``generate`` writes, keyed by each name, used
    in place of the grants derived from the agents and rules.
``open_bus``
    Run agents that can act on a broker that does not bind each login to its
    identity, or with no guard or policy. Anyone on that bus can then replay a token
    read there and act as an agent, or call the agents.
``verify_broker``
    Before any agent acts, each login sends a request on every request topic of the
    fleet and the agents' own Things under the next login's name, and the broker
    must refuse each; otherwise nothing starts (see :func:`binds_logins`).
``gateways``, ``callers``
    For ``generate``. ``gateways`` names the broker users of the fleet's gateways:
    a list of users that each serve the whole fleet, or a map from each user to the
    ids of the Things it serves. ``callers`` names other broker users, such as a
    chat adapter, that may send any request, but only under their own name.
``clients``
    Identities that call the fleet from outside, through ``thingagent mcp``: each is
    ``{"name", "reads", "may"}``, like an agent with no model. ``generate`` grants
    them what they read and may take; ``serve`` does not run them.
``allow``
    Who may ask which agent.
``timeout``, ``trace``
    How many seconds a call over the bus may take (30 by default), and a JSON Lines
    file that receives every task, answer and SOSA record.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib.util
import json
import os
import re
import signal
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Collection
from functools import partial
from pathlib import Path
from typing import Any, cast

from thingctx import TransportError
from thingctx.authz import GrantSet
from thingctx.bindings import (
    BindingRegistry,
    LocalBinding,
    MqttBinding,
    MqttTls,
    discover_local_handlers,
)
from thingctx.gateways.mosquitto import acl_file
from thingctx.models.sosa import recorder
from thingctx.registry import from_args
from thingctx.thing import thing_slug

from thingagent.agent import AGENT_ID, Agent
from thingagent.check import check, plain_bus_risk
from thingagent.grants import grants, loaded, saved
from thingagent.harness import Harness, do_view, guarded_gateway, only_decides, write_trace
from thingagent.rules import (
    Rule,
    binds_logins,
    broker_address,
    bus_tls,
    client_as,
    not_in_fleet,
    probe_topics,
)


async def retained(
    broker: str,
    topic: str,
    *,
    login: tuple[str, str] | None = None,
    settle: float = 1.0,
    tls: MqttTls | None = None,
) -> dict[str, Any]:
    """Each JSON message retained on ``topic`` (a filter) at ``broker``, by topic,
    collected for ``settle`` seconds, logged in as ``login`` when given, over
    ``tls`` when given, or the system CA store's for an ``mqtts://`` broker."""
    # Optional dependency of thingctx, required by thingagent.
    import paho.mqtt.client as mqtt  # noqa: PLC0415

    found: dict[str, Any] = {}

    def on_message(_client: Any, _userdata: Any, message: Any) -> None:
        with contextlib.suppress(ValueError):
            found[message.topic] = json.loads(message.payload)

    host, port, _ = broker_address(broker)
    tls = bus_tls(broker, tls)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.on_message = on_message
    if login is not None:
        client.username_pw_set(*login)
    if tls is not None:
        tls.apply(client)
    client.connect(host, port)
    client.subscribe(topic, qos=1)
    client.loop_start()
    try:
        await asyncio.sleep(settle)
    finally:
        client.loop_stop()
        client.disconnect()
    return found


_OWN_IDS = "urn:thingagent:"


async def fleet_from_bus(
    broker: str,
    prefix: str,
    *,
    login: tuple[str, str] | None = None,
    settle: float = 1.0,
    tls: MqttTls | None = None,
) -> list[dict[str, Any]]:
    """The TDs retained under ``<prefix>/+/td``, apart from any harness's agents and
    chat channels, collected for ``settle`` seconds, logged in as ``login`` when
    given, over ``tls`` when given. A TD :func:`not_in_fleet` refuses is left out,
    and reported on standard error."""
    found = await retained(broker, f"{prefix}/+/td", login=login, settle=settle, tls=tls)
    fleet: list[dict[str, Any]] = []
    for topic, td in found.items():
        if isinstance(td, dict) and str(td.get("id")).startswith(_OWN_IDS):
            continue
        reason = not_in_fleet(topic, td, broker)
        if reason is None:
            fleet.append(td)
        else:
            print(f"thingagent: leaving out the TD retained at {topic}: {reason}", file=sys.stderr)
    return fleet


def device_bindings(fleet: list[dict[str, Any]], timeout: float | None = None) -> list[Any]:
    """The bindings that reach the fleet's devices directly: HTTP, MQTT, in
    process handlers installed packages register, and rosbridge when a form uses it.
    ``timeout`` bounds each MQTT and rosbridge call when given."""
    bindings = list(BindingRegistry.default(mqtt=True).bindings)
    if timeout is not None:
        bindings = [
            MqttBinding(timeout=timeout) if isinstance(b, MqttBinding) else b for b in bindings
        ]
    local = next(b for b in bindings if isinstance(b, LocalBinding))
    for slug, handlers in discover_local_handlers({thing_slug(td["id"]) for td in fleet}).items():
        local.register_thing(slug, handlers)
    hrefs = json.dumps(fleet)
    if '"rosbridge://' in hrefs:
        # Needs thingctx's ros extra, only when the fleet has rosbridge forms.
        from thingctx.bindings.builtin.rosbridge import RosbridgeBinding  # noqa: PLC0415

        bindings.append(RosbridgeBinding(timeout=timeout) if timeout else RosbridgeBinding())
    return bindings


def guard(settings: dict[str, Any] | None, base: Path) -> Any:
    """A JWT guard from ``issuers``, ``audience`` and ``jwks_url``, or ``jwks``, a
    JWKS file relative to ``base``; None without settings."""
    if not settings:
        return None
    # Needs thingctx's authz extra, which thingagent depends on.
    from thingctx.identity import JwtGatewayGuard  # noqa: PLC0415

    keys = {"jwks": json.loads((base / settings["jwks"]).read_text())} if "jwks" in settings else {}
    return JwtGatewayGuard(
        issuers=settings["issuers"],
        audience=settings["audience"],
        jwks_url=settings.get("jwks_url"),
        **keys,
    )


def _of(name: str, names: Collection[str] | None) -> bool:
    return names is None or name in names


_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _env_name(key: str, value: Any) -> str:
    """``value``, the config key ``key``'s, when it names an environment variable.
    The message names the key, never the value, which may be a secret."""
    if not (isinstance(value, str) and _ENV_NAME.fullmatch(value)):
        raise SystemExit(
            f"thingagent: {key} must name an environment variable, in letters, digits "
            "and _, not starting with a digit"
        )
    return value


def _env(key: str, name: str) -> str:
    """The value of the environment variable ``name``, which the config key ``key``
    holds."""
    if name not in os.environ:
        raise SystemExit(f"thingagent: set the environment variable that {key} names")
    return os.environ[name]


def _secret_names(raw: dict[str, Any]) -> None:
    """Refuse a configuration whose secret keys do not each name an environment
    variable: ``login.password_env``, each ``logins.<name>.password_env`` and each
    ``token_env.<name>``."""
    logins = raw.get("logins", {})
    tokens = raw.get("token_env", {})
    if not isinstance(logins, dict) or not isinstance(tokens, dict):
        raise SystemExit("thingagent: logins and token_env each map a name to its entry")
    entries = {f"logins.{name}": entry for name, entry in logins.items()}
    if "login" in raw:
        entries["login"] = raw["login"]
    for key, entry in entries.items():
        given = entry.get("password_env") if isinstance(entry, dict) else None
        _env_name(f"{key}.password_env", given)
    for name, env in tokens.items():
        _env_name(f"token_env.{name}", env)


def _tls(raw: dict[str, Any], base: Path) -> MqttTls | None:
    """The TLS every connection to ``raw``'s broker uses: its ``tls`` entry, files
    relative to ``base``, or the system CA store's for an ``mqtts://`` broker."""
    entry = raw.get("tls")
    given = None
    if entry is not None:
        if not isinstance(entry, dict) or set(entry) - {"ca", "cert", "key"}:
            raise SystemExit("thingagent: tls takes only ca, cert and key, each a file")
        files = {k: str(base / entry[k]) for k in ("ca", "cert", "key") if k in entry}
        given = MqttTls(
            ca_certs=files.get("ca"), certfile=files.get("cert"), keyfile=files.get("key")
        )
    return bus_tls(raw["broker"], given) if raw.get("broker") else given


def _refuse_plain(raw: dict[str, Any], tls: MqttTls | None) -> None:
    """Refuse ``raw``'s broker before anything connects when :func:`plain_bus_risk`
    finds it may not be used."""
    insecure = bool(raw.get("insecure_bus", False))
    risk = plain_bus_risk(raw["broker"], tls=tls is not None, insecure_bus=insecure)
    if risk:
        raise SystemExit(f"thingagent: {risk}")


async def _verify(
    broker: str, topics: list[str], logins: list[tuple[str, str]], tls: MqttTls | None
) -> None:
    """Refuse to start unless the broker refuses each of ``logins`` a request under
    another name (see :func:`binds_logins`)."""
    if not logins:
        raise SystemExit("thingagent: verify_broker has no login to check")
    try:
        published = await binds_logins(broker, topics, logins, tls=tls)
    except TransportError as exc:
        raise SystemExit(f"thingagent: verify_broker could not log in: {exc}") from None
    if published:
        raise SystemExit("\n".join(f"thingagent: {why}" for why in published))


class Config:
    def __init__(self, path: Path) -> None:
        self.raw: dict[str, Any] = json.loads(path.read_text())
        self.base = path.parent
        self.broker: str | None = self.raw.get("broker")
        self.prefix: str = self.raw.get("prefix", "tc")
        _secret_names(self.raw)

    def agents(self) -> list[Agent]:
        return [Agent.load(self.base / p) for p in self.raw.get("agents", [])]

    def rules(self) -> list[Rule]:
        return [Rule.load(self.base / p) for p in self.raw.get("rules", [])]

    def clients(self) -> list[Agent]:
        return [Agent.from_dict(raw) for raw in self.raw.get("clients", [])]

    async def fleet(self) -> list[dict[str, Any]]:
        source = self.raw["fleet"]
        if source == "bus":
            if not self.broker:
                raise SystemExit('thingagent: "fleet": "bus" needs a broker')
            _refuse_plain(self.raw, self.tls())
            return await fleet_from_bus(
                self.broker, self.prefix, login=self.login(), tls=self.tls()
            )
        sources = [source] if isinstance(source, str) else source
        return await _tds(sources, self.base)

    def name(self) -> str | None:
        """The harness's name: ``name``, or else thingagent's login user."""
        return self.raw.get("name") or (self.raw.get("login") or {}).get("user")

    def own(self) -> list[dict[str, Any]]:
        """The TDs this harness serves: its agents and its chat channel."""
        return Harness(self.agents(), fleet=[], name=self.name()).own_tds()

    def tokens(self, names: Collection[str] | None = None) -> dict[str, str]:
        """Each token, or only those of ``names``, from the environment. Raises
        ``SystemExit`` when one is not set."""
        return {
            name: _env(f"token_env.{name}", env)
            for name, env in self.raw.get("token_env", {}).items()
            if _of(name, names)
        }

    def logins(self, names: Collection[str] | None = None) -> dict[str, tuple[str, str]]:
        """Each login, or only those of ``names``, its password from the environment."""
        return {
            name: (entry["user"], _env(f"logins.{name}.password_env", entry["password_env"]))
            for name, entry in self.raw.get("logins", {}).items()
            if _of(name, names)
        }

    def login(self) -> tuple[str, str] | None:
        entry = self.raw.get("login")
        return (entry["user"], _env("login.password_env", entry["password_env"])) if entry else None

    def tls(self) -> MqttTls | None:
        return _tls(self.raw, self.base)

    def guard(self) -> Any:
        return guard(self.raw.get("guard"), self.base)

    def bus(self) -> dict[str, Any]:
        """What :func:`check` needs to judge whether agents that act may use the bus."""
        return {
            "broker": self.broker,
            "broker_binds_identity": bool(self.raw.get("broker_binds_identity", False)),
            "logins": set(self.raw.get("logins", {})),
            "guard": bool(self.raw.get("guard")),
            "policy": "policy" in self.raw,
            "open_bus": bool(self.raw.get("open_bus", False)),
            "tls": self.tls() is not None,
            "insecure_bus": bool(self.raw.get("insecure_bus", False)),
        }

    def gateways(self, fleet: list[dict[str, Any]]) -> dict[str, list[str]]:
        """Each fleet gateway's broker user, with the ids of the Things it serves."""
        entry = self.raw.get("gateways", [])
        if isinstance(entry, dict):
            return {user: list(things) for user, things in entry.items()}
        if isinstance(entry, list):
            return {user: [td["id"] for td in fleet] for user in entry}
        raise SystemExit("thingagent: gateways is a list of users or a map from user to Thing ids")

    def policy(self) -> dict[str, GrantSet] | None:
        return loaded(self.raw["policy"]) if "policy" in self.raw else None

    def grants(self) -> dict[str, GrantSet] | None:
        """The grants file ``grants`` names, for the in process gate."""
        if "grants" not in self.raw:
            return None
        return loaded(json.loads((self.base / self.raw["grants"]).read_text()))


def _check(
    config: Config, fleet: list[dict[str, Any]], rules: list[Rule] | None = None
) -> list[str]:
    return check(
        config.agents(),
        config.rules() if rules is None else rules,
        fleet,
        tokens=set(config.raw.get("token_env", {})),
        allow=config.raw.get("allow"),
        clients=config.clients(),
        **config.bus(),
    )


async def _converse(harness: Harness, person: str) -> None:
    """A chat in this terminal, as ``person``, until end of input."""
    shown = 0

    async def show() -> None:
        nonlocal shown
        while True:
            for message in harness.channel.outbox[shown:]:
                if message["person"] == person:
                    print(f"\n{message['text']}", flush=True)
            shown = len(harness.channel.outbox)
            await asyncio.sleep(0.2)

    printer = asyncio.ensure_future(show())
    try:
        while True:
            try:
                line = await asyncio.to_thread(input, f"{person}> ")
            except EOFError:
                return
            if line.strip():
                await harness.channel.receive(person, line)
    finally:
        printer.cancel()


@contextlib.asynccontextmanager
async def running(config: Config, only: str | None = None) -> AsyncIterator[Harness]:
    """The agents and their rules, started from ``config`` and stopped on exit;
    with ``only``, an agent's name, only the rules that call that agent, and only
    an agent that can chat. Raises ``SystemExit`` with every problem :func:`check`
    reports."""
    agents, rules = config.agents(), config.rules()
    if only is not None:
        rules = [rule for rule in rules if rule.calls.partition("__")[0] == only]
        try:
            for agent in (a for a in agents if a.name == only):
                only_decides(agent)
        except ValueError as exc:
            raise SystemExit(f"thingagent: {exc}") from None
    fleet = await config.fleet()
    problems = _check(config, fleet, rules)
    if problems:
        raise SystemExit("\n".join(problems))
    # Clients call from outside, so their secrets are not needed here.
    served = {*(a.name for a in agents), *(r.name for r in config.rules())}
    tokens, logins = config.tokens(served), config.logins(served)
    login = config.login()
    if config.broker and config.raw.get("verify_broker"):
        probed = [*logins.values(), *([login] if login else [])]
        await _verify(
            config.broker,
            probe_topics([*fleet, *config.own()], config.prefix),
            probed,
            config.tls(),
        )
    trace = config.raw.get("trace")
    timeout = float(config.raw.get("timeout", 30.0))
    harness = Harness(
        agents,
        fleet=fleet,
        broker=config.broker,
        bindings=None if config.broker else device_bindings(fleet, timeout),
        rules=rules,
        tokens=tokens,
        logins=logins,
        login=login,
        allow=config.raw.get("allow"),
        prefix=config.prefix,
        name=config.name(),
        guard=config.guard(),
        broker_binds_identity=config.raw.get("broker_binds_identity", False),
        open_bus=bool(config.raw.get("open_bus", False)),
        insecure_bus=bool(config.raw.get("insecure_bus", False)),
        policy=config.policy(),
        grants=config.grants(),
        tls=config.tls(),
        timeout=timeout,
        trace=config.base / trace if trace else None,
    )
    await harness.start()
    try:
        where = f"on {config.broker}" if config.broker else "in this process"
        print(f"thingagent: running {', '.join(harness.agents)} {where}", file=sys.stderr)
        yield harness
    finally:
        await harness.aclose()


async def _until_stopped(*work: Awaitable[Any]) -> None:
    """Wait for the first of ``work`` to finish, or for SIGINT or SIGTERM."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    tasks = [asyncio.ensure_future(w) for w in work] + [asyncio.ensure_future(stop.wait())]
    done, pending = await asyncio.wait(tasks, return_when="FIRST_COMPLETED")
    for task in pending:
        task.cancel()
    for task in done:
        task.result()


async def _run(config: Config, person: str | None) -> None:
    async with running(config) as harness:
        await _until_stopped(*([_converse(harness, person)] if person else []))


async def _ask(config: Config, name: str, prompt: str) -> str:
    """Run the agents until ``name`` has answered ``prompt`` from what it can read."""
    async with running(config, only=name) as harness:
        if name not in harness.agents:
            raise SystemExit(f"thingagent: no agent named {name}")
        answered = await harness.ask(harness.agents[name], prompt)
        return str(answered["answer"])


async def _do(config: Config, name: str, task: str, person: str | None) -> dict[str, Any]:
    """Run the agents until ``name`` has done ``task``; with ``person``, this
    terminal is that person's chat, so they can approve what the task waits on.
    Only the rules that call ``name`` run, such as one that approves for it."""
    async with running(config, only=name) as harness:
        if name not in harness.agents:
            raise SystemExit(f"thingagent: no agent named {name}")
        done: dict[str, Any] = {}

        async def work() -> None:
            done.update(await harness.do(harness.agents[name], task, person=person))

        await _until_stopped(work(), *([_converse(harness, person)] if person else []))
        return done


@contextlib.asynccontextmanager
async def serving(config: Config, name: str) -> AsyncIterator[Any]:
    """An MCP server of the fleet for the client ``name``, closed on exit. The client
    sees only what it reads and may take, each call goes to the fleet's gateway under
    its own token and login, and each is recorded in the trace. Broker only."""
    # The mcp extra, needed only here.
    from thingctx.integrations.mcp import build_mcp_server  # noqa: PLC0415

    client = next((c for c in config.clients() if c.name == name), None)
    if client is None:
        raise SystemExit(f"thingagent: no client named {name}")
    if not config.broker:
        raise SystemExit("thingagent: mcp needs a broker")
    tls = config.tls()
    _refuse_plain(config.raw, tls)
    token, login = config.tokens([name]).get(name), config.logins([name]).get(name)
    fleet = await fleet_from_bus(config.broker, config.prefix, login=login, tls=tls)
    problems = _check(config, fleet)
    if problems:
        raise SystemExit("\n".join(problems))
    trace = config.raw.get("trace")
    actor = f"urn:thingagent:client:{name}"
    record = recorder(partial(_traced, config.base / trace), actor=actor) if trace else None
    timeout = float(config.raw.get("timeout", 30.0))
    tds = [do_view(td, client) for td in fleet]
    calls = client_as(tds, token, login, record=record, timeout=timeout, tls=tls)
    try:
        yield build_mcp_server(calls, name="thingagent", approve_when="declared")
    finally:
        await calls.aclose()


async def _mcp(config: Config, name: str) -> None:
    # The mcp extra, needed only here.
    from mcp.server.stdio import stdio_server  # noqa: PLC0415

    async with serving(config, name) as server, stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def _load(base: Path, ref: str) -> Callable[[list[dict[str, Any]]], dict[str, Any]]:
    """The function ``ref`` (``file.py:name``, the file relative to ``base``) names."""
    file, _, name = ref.partition(":")
    spec = importlib.util.spec_from_file_location(Path(file).stem, base / file)
    if spec is None or spec.loader is None or not name:
        raise SystemExit(f"thingagent: {ref} names no function in a Python file")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast("Callable[[list[dict[str, Any]]], dict[str, Any]]", getattr(module, name))


async def _tds(entries: list[str], base: Path) -> list[dict[str, Any]]:
    """The TDs ``entries`` name: TD files and folders of them, relative to ``base``,
    and the URLs and ``tdd:URL`` directories thingctx's registry reads."""
    remote = ("tdd:", "http://", "https://")
    sources = [e if e.startswith(remote) else str(base / e) for e in entries]
    return await asyncio.to_thread(from_args(sources).fetch)


async def _things(entries: list[Any], base: Path) -> list[dict[str, Any]]:
    """The TDs ``entries`` name, as for :func:`_tds`, and ``{"broker", "topic"}``
    for the TDs devices retain on a broker of their own."""
    tds = await _tds([e for e in entries if isinstance(e, str)], base)
    for entry in entries:
        if isinstance(entry, dict):
            found = await retained(entry["broker"], entry["topic"], settle=2.0)
            tds += [td for td in found.values() if isinstance(td, dict) and "id" in td]
    return tds


def _traced(trace: Path, record: Any) -> None:
    write_trace(trace, {"record": record.as_jsonld()})


async def _serve_fleet(path: Path) -> None:
    """Serve the fleet's devices on the bus, as the fleet's gateway, until stopped.

    The configuration is a JSON file; paths in it are relative to it::

        {"broker": "127.0.0.1:1885", "things": ["things"],
         "login": {"user": "fleet_gw", "password_env": "FLEET_GW_PASSWORD"},
         "guard": {"issuers": ["..."], "audience": "...", "jwks": "keys/jwks.json"},
         "grants": "policy/grants.json", "trace": "record/gateway.jsonl"}

    ``things`` holds TD files, folders of them, TD URLs, ``tdd:URL`` directories and
    ``{"broker", "topic"}`` for the TDs devices retain on a broker of their own.
    ``login`` names the broker user and, in ``password_env``, the environment
    variable that holds its password. ``broker``, ``tls`` and ``insecure_bus`` are
    as for the agents; the refusal of a plain link covers ``broker``, not the
    brokers of devices under ``things``. With ``verify_broker`` the gateway starts
    only when the broker refuses its login every request topic of its Things under
    a name no login has. ``grants`` is the file
    ``thingagent generate`` writes, keyed by each caller's token ``sub``; a request
    whose arguments fall outside the caller's grant is refused. ``handlers`` maps a
    Thing's slug to ``file.py:name``, a function that takes the fleet's TDs and
    returns that Thing's in process handlers. ``prefix`` and ``timeout`` are as for
    the agents. ``trace`` names a JSON Lines file that receives the SOSA record of
    every call the gateway runs or refuses, naming its caller. With a guard, a
    request runs only when its token names the User Name it was sent under, so the
    broker must stop each login sending as another."""
    raw = json.loads(path.read_text())
    base = path.parent
    _secret_names(raw)
    tls = _tls(raw, base)
    _refuse_plain(raw, tls)
    things = await _things(raw["things"], base)
    if not things:
        raise SystemExit(f"thingagent: {path} names no Thing Descriptions")
    bindings = device_bindings(things, float(raw.get("timeout", 30.0)))
    local = next(b for b in bindings if isinstance(b, LocalBinding))
    for slug, ref in raw.get("handlers", {}).items():
        local.register_thing(slug, _load(base, ref)(things))
    given = loaded(json.loads((base / raw["grants"]).read_text())) if "grants" in raw else None
    checker = guard(raw.get("guard"), base)
    entry = raw.get("login")
    login = (entry["user"], _env("login.password_env", entry["password_env"])) if entry else None
    prefix = raw.get("prefix", "tc")
    if raw.get("verify_broker"):
        await _verify(raw["broker"], probe_topics(things, prefix), [login] if login else [], tls)
    trace = base / raw["trace"] if "trace" in raw else None
    record = recorder(partial(_traced, trace)) if trace else None
    gateway = guarded_gateway(
        things,
        bindings,
        raw["broker"],
        grants=given,
        claim="sub",
        guard=checker,
        record=record,
        login=login,
        prefix=prefix,
        broker_binds_identity=checker is not None,
        tls=tls,
    )
    await gateway.start()
    names = ", ".join(thing_slug(td["id"]) for td in things)
    print(f"thingagent: serving {names} on {raw['broker']}", file=sys.stderr)
    try:
        await _until_stopped()
    finally:
        await gateway.aclose()


async def _generate(config: Config, out: Path, gateway: Path | None = None) -> list[Path]:
    """Write ``grants.json`` and ``acl`` to ``out`` and return their paths.

    ``grants.json`` holds each agent's and rule's grants on the fleet, keyed by its
    broker user (its token's ``sub``), with the JSON Schema that bounds an action's
    arguments where an agent's steps fix them or its ``limits`` bound them (see
    :func:`thingagent.grants.saved`). ``acl`` holds the broker's rules, compiled from
    the same grants. A rule may also call its agent's ``decide`` or ``do``, which go
    over the bus. Each gateway user may serve only its own Things: a fleet gateway
    the fleet's, or those ``gateways`` lists for it; thingagent's own user the agents
    and the chat channel. Given ``gateway``, a fleet gateway's configuration, the
    fleet is the Things it serves, and its user serves them."""
    served: dict[str, Any] = json.loads(gateway.read_text()) if gateway else {}
    fleet = await (_things(served["things"], gateway.parent) if gateway else config.fleet())
    agents, rules = config.agents(), config.rules()
    logins = config.raw.get("logins", {})

    def user(name: str) -> str:
        return str(logins.get(name, {}).get("user", name))

    given_by = grants([*agents, *config.clients()], rules, fleet)
    by_user = {user(name): given for name, given in given_by.items()}
    allowed = {name: set(given) for name, given in by_user.items()}
    for rule in rules:
        agent, _, action = rule.calls.partition("__")
        if action != "approve":
            allowed[user(rule.name)].add((f"{AGENT_ID}{agent}", action, "invokeaction"))
    serves = config.gateways(fleet)
    if served.get("login"):
        serves[served["login"]["user"]] = [td["id"] for td in fleet]
    own = (config.raw.get("login") or {}).get("user")
    if own:
        serves[own] = [*serves.get(own, []), *(td["id"] for td in config.own())]
        allowed.setdefault(own, set())
    callers = list(dict.fromkeys([*allowed, *config.raw.get("callers", [])]))
    out.mkdir(parents=True, exist_ok=True)
    acl = acl_file(
        gateways=list(serves), callers=callers, prefix=config.prefix, grants=allowed, serves=serves
    )
    written = {
        "grants.json": json.dumps(saved(by_user), indent=1),
        "acl": acl,
    }
    for name, text in written.items():
        (out / name).write_text(text if text.endswith("\n") else text + "\n")
    return [out / name for name in written]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="thingagent")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="check agents and rules against the fleet").add_argument(
        "config", type=Path
    )
    commands.add_parser("serve", help="run the agents and their rules").add_argument(
        "config", type=Path
    )
    chat = commands.add_parser("chat", help="run the agents with a chat in this terminal")
    chat.add_argument("config", type=Path)
    chat.add_argument("--as", dest="person", required=True, help="who you are in the chat")
    do = commands.add_parser("do", help="run the agents until one has done one task")
    do.add_argument("config", type=Path)
    do.add_argument("agent", help="the agent that does the task")
    do.add_argument("task", help="the task, in plain words")
    do.add_argument("--as", dest="person", help="chat in this terminal as this person")
    generate = commands.add_parser("generate", help="write the gateway grants and broker rules")
    generate.add_argument("config", type=Path)
    generate.add_argument("--out", type=Path, required=True, help="the directory to write to")
    generate.add_argument("--gateway", type=Path, help="the fleet gateway's configuration")
    ask = commands.add_parser("ask", help="run the agents until one has answered one prompt")
    ask.add_argument("config", type=Path)
    ask.add_argument("agent", help="the agent that answers")
    ask.add_argument("prompt", help="the prompt, in plain words")
    commands.add_parser("gateway", help="serve the fleet's devices on the bus").add_argument(
        "config", type=Path
    )
    mcp = commands.add_parser("mcp", help="serve the fleet to an MCP client over stdio")
    mcp.add_argument("config", type=Path)
    mcp.add_argument("client", help="the client in the config that the MCP client calls as")
    args = parser.parse_args(argv)
    if args.command == "gateway":
        asyncio.run(_serve_fleet(args.config))
        return 0
    config = Config(args.config)
    if args.command == "do":
        did = asyncio.run(_do(config, args.agent, args.task, args.person))
        print(did.get("answer", ""))
        for record in did.get("acted", []):
            kinds = record.get("@type", [])
            kind = " ".join(
                k.partition(":")[2] for k in ([kinds] if isinstance(kinds, str) else kinds)
            )
            status = str(record.get("schema:actionStatus", "")).removeprefix("schema:")
            print(f"  {kind} {record.get('sosa:actsOnProperty', '')} {status}".rstrip())
        return 0 if did else 1
    if args.command == "ask":
        print(asyncio.run(_ask(config, args.agent, args.prompt)))
        return 0
    if args.command == "generate":
        for path in asyncio.run(_generate(config, args.out, args.gateway)):
            print(f"wrote {path}")
        return 0
    if args.command == "mcp":
        asyncio.run(_mcp(config, args.client))
        return 0
    if args.command == "check":
        problems = _check(config, asyncio.run(config.fleet()))
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    asyncio.run(_run(config, getattr(args, "person", None)))
    return 0
