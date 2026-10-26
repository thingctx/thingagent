# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""The harness: agents, a chat channel, and the rules that start agents.

Each agent acts on the fleet only through thingctx, as itself. With a broker, the
fleet is the TDs its gateways project: every request carries the agent's token, so
the fleet's gateway authorizes and records it, and the agents and the channel are
served as Things on the bus. Without one, the fleet is the devices' own TDs and the
gate runs here, granting each agent and rule exactly what its definition names.

``ask`` runs on a read view: only the properties the agent reads, read only, and no
actions, so a model reading untrusted text has nothing to actuate with. ``do`` runs
on a view that adds the actions the agent ``may`` take: the model chooses the calls
and arguments, the agent's grants bound them where they land, and an action that
needs approval waits for its person.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any

from thingctx import ThingClient, TransportError
from thingctx.authz import (
    GrantSet,
    LocalPolicyGrantSource,
    PolicyDecisionPoint,
    build_vocabulary,
)
from thingctx.bindings import BindingRegistry, LocalBinding, MqttBinding, MqttTls
from thingctx.contrib.llm import LLMHost
from thingctx.gateways import Gateway
from thingctx.gateways.builtin.mqtt import MqttGatewayBinding
from thingctx.models.sosa import recorder
from thingctx.operations import Recorder
from thingctx.thing import parse_thing, thing_slug
from thingctx.trust import ApprovalRequest

from thingagent.agent import Agent, Step
from thingagent.channel import ChatChannel
from thingagent.check import open_bus_risks, plain_bus_risk
from thingagent.decide import ChatDecider, Decider, DecisionsDecider
from thingagent.grants import grants
from thingagent.models import ChatFn, chat_fn, compact
from thingagent.rules import (
    Rule,
    broker_address,
    bus_tls,
    client_as,
    credentials,
    on_request,
    open_stream,
    other_brokers,
    stream_href,
    topic,
)

_log = logging.getLogger("thingagent")
# Context variables keep the records and approvals of concurrent tasks apart.
_acted: ContextVar[list[dict[str, Any]] | None] = ContextVar("thingagent_acted", default=None)
_asker: ContextVar[str | None] = ContextVar("thingagent_asker", default=None)
_CONTEXT = "https://www.w3.org/2022/wot/td/v1.1"
_NOSEC = {"securityDefinitions": {"nosec_sc": {"scheme": "nosec"}}, "security": ["nosec_sc"]}
# A guarded gateway's callers log in, since each request topic ends in the caller's
# MQTT User Name.
BUS_BASIC = {"bus_basic": {"scheme": "basic"}}
# How many triggers, and for how many seconds, a harness remembers to drop repeats.
REMEMBER = 1024
WINDOW = 600.0
# Seconds a rule waits to reopen a stream: doubled after each try, up to the cap.
REOPEN = 1.0
REOPEN_MAX = 30.0
# The keys thingagent adds to route a trigger. Records and events keep them; no model
# reads them, since an id with no meaning still moves a model's answer.
ROUTING = frozenset({"correlation"})


def _correlation(data: Any) -> str:
    """The id ``data`` carries in ``correlation``, or a new one."""
    found = data.get("correlation") if isinstance(data, dict) else None
    return found if isinstance(found, str) and found else uuid.uuid4().hex


def unrouted(value: Any) -> Any:
    """``value`` as a model reads it: without the ``ROUTING`` keys, at any depth."""
    if isinstance(value, dict):
        return {k: unrouted(v) for k, v in value.items() if k not in ROUTING}
    if isinstance(value, list):
        return [unrouted(v) for v in value]
    return value


def _td_topic(href: str) -> str:
    """The topic a gateway retains the TD on, for the stream form ``href``."""
    path = re.sub(r"/(?:events|props)/[^/]+/(?:subscribe|observe)(?:/[^/]+)?/stream$", "", href)
    return f"{topic(path)}/td"


class _Tolerant(ThingClient):
    """A model's client. Results come back compacted, a transport failure comes
    back as an error instead of ending the turn, and ``on_call`` sees each call."""

    def __init__(
        self, *args: Any, on_call: Callable[[dict[str, Any]], None] | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._on_call = on_call

    async def call_tool(self, name: str, args: dict[str, Any] | None = None) -> Any:
        started = datetime.now(timezone.utc).isoformat()
        try:
            result = compact(await super().call_tool(name, args))
        except TransportError as exc:
            _log.warning("thingagent: %s got no answer: %s", name, exc)
            result = {"error": str(exc), "failed": True}
        if self._on_call is not None:
            self._on_call({"call": name, "args": args or {}, "result": result, "started": started})
        return result


def _view(td: dict[str, Any], agent: Agent, actions: tuple[str, ...]) -> dict[str, Any]:
    slug = thing_slug(td["id"])
    view = copy.deepcopy(td)
    view.pop("events", None)
    reads = {*agent.reads, *([agent.image] if agent.image else [])}
    view["properties"] = {
        name: {**prop, "readOnly": True}
        for name, prop in view.get("properties", {}).items()
        if f"{slug}__{name}" in reads
    }
    view["actions"] = {
        name: action
        for name, action in view.get("actions", {}).items()
        if f"{slug}__{name}" in actions
    }
    return view


def read_view(td: dict[str, Any], agent: Agent) -> dict[str, Any]:
    """``td`` with only the properties ``agent`` reads, read only; no actions or
    events."""
    return _view(td, agent, ())


def do_view(td: dict[str, Any], agent: Agent) -> dict[str, Any]:
    """``td`` with only the properties ``agent`` reads, read only, and the actions it
    may take; no events."""
    return _view(td, agent, agent.may)


class LoggedIn(MqttGatewayBinding):
    """An MQTT gateway that logs in to the broker as ``login``, when one is given.
    ``options`` go to :class:`MqttGatewayBinding`, ``tls`` among them."""

    def __init__(self, broker: str, *, login: tuple[str, str] | None, **options: Any) -> None:
        super().__init__(broker, **options)
        self._login = login

    def configure(self, paho: Any) -> None:
        if self._login is not None:
            paho.username_pw_set(*self._login)


def guarded_gateway(
    tds: list[dict[str, Any]],
    bindings: list[Any],
    broker: str,
    *,
    grants: dict[str, GrantSet] | None = None,
    claim: str = "roles",
    guard: Any = None,
    record: Recorder | None = None,
    **options: Any,
) -> Gateway:
    """A gateway that serves ``tds`` on ``broker`` through ``bindings``, logged in
    as ``options["login"]``. With ``grants``, keyed by the token claim ``claim``,
    each request runs only as its caller's grants allow, with only the arguments
    they permit. With ``guard``, only a request with a valid token runs. ``record``
    receives every call it runs or refuses."""
    served = ThingClient(tds=tds, bindings=bindings, record=record)
    if grants is not None:
        source = LocalPolicyGrantSource(grants, claim=claim)
        vocabulary = build_vocabulary(served.things)
        served = served.guarded(PolicyDecisionPoint(vocabulary=vocabulary, grant_source=source))
    bus_security = BUS_BASIC if guard is not None else None
    return Gateway(served, LoggedIn(broker, guard=guard, bus_security=bus_security, **options))


def _gate(fleet: list[dict[str, Any]], given: dict[str, GrantSet]) -> PolicyDecisionPoint:
    """A decision point on ``fleet`` that grants each token ``sub`` what ``given``
    holds for it."""
    return PolicyDecisionPoint(
        vocabulary=build_vocabulary([parse_thing(td) for td in fleet]),
        grant_source=LocalPolicyGrantSource(given, claim="sub"),
    )


def write_trace(trace: Path, entry: dict[str, Any]) -> None:
    """Append ``entry``, stamped with the time, to the JSON Lines file ``trace``."""
    entry = {"at": datetime.now(timezone.utc).isoformat(), **entry}
    with trace.open("a") as lines:
        lines.write(json.dumps(entry, default=str) + "\n")


def only_decides(agent: Agent) -> None:
    """Raise ``ValueError`` when ``agent`` has a decisions model, which only decides."""
    if agent.decides_only:
        raise ValueError(f"{agent.name} has a decisions model, so it can only decide")


class Harness:
    """Run ``agents`` against ``fleet``.

    With ``broker``, ``fleet`` is the TDs the fleet's gateways project, and their
    MQTT forms must all name ``broker``. Each agent and rule calls with its token
    from ``tokens`` and logs in with its user and password from ``logins``; the user
    must be the token's ``sub``. thingagent itself logs in as ``login``. Every
    connection to the broker uses ``tls``, or the system CA store's TLS for an
    ``mqtts://`` broker; a broker not on this machine with neither is refused unless
    ``insecure_bus``. Without a broker, ``fleet`` is the devices' own TDs, reached
    through ``bindings`` (HTTP, MQTT and in process by default), and ``grants``,
    keyed by each token's ``sub``, replaces the grants derived from the definitions
    when given.

    ``rules`` run here. With a broker, a rule calls this harness's agents' ``decide``
    and ``do`` over the bus as itself, so the gateway's ``policy`` sees the call.
    ``allow`` maps a person to the agents they may ask. With a broker, ``guard`` and
    ``policy`` (role to grants) protect the agents' own Things as a thingctx gateway
    protects any Thing, and an agent that can act starts only with
    ``broker_binds_identity``, a login, ``guard`` and ``policy``, or with
    ``open_bus``. ``timeout`` bounds each call over the bus. ``trace`` names a JSON
    Lines file that receives every task, answer, decision and SOSA record. A rule's
    stream that ends or fails, or whose guarded gateway restarts, is opened again
    with a growing wait between tries, with the rule's token as ``tokens`` holds it
    then, so a token replaced there takes effect.

    Every ``decide`` and ``do`` carries a correlation id, which its event reports and
    a rule passes on to the agent it calls. No model reads it. The harness runs each
    agent's action once per id, so copies of an upstream agent start the next one
    once.

    ``name`` goes into the chat channel's id, so harnesses on one prefix do not
    collide.
    """

    def __init__(
        self,
        agents: list[Agent],
        *,
        fleet: list[dict[str, Any]],
        broker: str | None = None,
        bindings: list[Any] | None = None,
        rules: list[Rule] | None = None,
        tokens: Mapping[str, str] | None = None,
        logins: dict[str, tuple[str, str]] | None = None,
        login: tuple[str, str] | None = None,
        allow: dict[str, list[str]] | None = None,
        prefix: str = "tc",
        name: str | None = None,
        guard: Any = None,
        broker_binds_identity: bool = False,
        open_bus: bool = False,
        insecure_bus: bool = False,
        policy: dict[str, GrantSet] | None = None,
        grants: dict[str, GrantSet] | None = None,
        tls: MqttTls | None = None,
        decider: Decider | None = None,
        chat_fn: ChatFn | None = None,
        channel: ChatChannel | None = None,
        confirm_timeout: float = 120.0,
        timeout: float = 30.0,
        trace: Path | None = None,
    ) -> None:
        self.agents = {agent.name: agent for agent in agents}
        own = f"_{name}" if name else ""
        self.channel = channel or ChatChannel(f"chat{own}")
        self._fleet = list(fleet)
        self._broker = broker
        self._devices = bindings
        self._rules = rules or []
        self._tokens = tokens or {}
        self._logins = logins or {}
        self._login = login
        self._allow = {person: set(names) for person, names in (allow or {}).items()}
        self._prefix = prefix
        self._guard = guard
        self._binds = broker_binds_identity
        self._open_bus = open_bus
        self._insecure_bus = insecure_bus
        self._policy = policy
        self._grants = grants
        self._tls = bus_tls(broker, tls) if broker else tls
        self._decider = decider
        self._chat_fn = chat_fn
        self._confirm_timeout = confirm_timeout
        self._timeout = timeout
        self._trace = trace
        self._bindings: dict[str, LocalBinding] = {}
        self._actors: dict[str, ThingClient] = {}
        self._doers: dict[str, ThingClient] = {}
        self._readers: dict[str, ThingClient] = {}
        self._sources: dict[str, ThingClient] = {}
        # The token each rule's clients were made with, by rule name.
        self._used: dict[str, str | None] = {}
        self._tasks: list[asyncio.Task[None]] = []
        # Each rule's task that follows its event, by rule name.
        self._follows: dict[str, asyncio.Task[None]] = {}
        self._watchers: list[Any] = []
        self._running: set[asyncio.Task[None]] = set()
        self._handled: dict[str, tuple[float, asyncio.Future[dict[str, Any]]]] = {}
        self.gateway: Gateway | None = None

    async def start(self) -> None:
        local = LocalBinding()
        for agent in self.agents.values():
            binding = LocalBinding(self._handlers(agent))
            local.register_thing(agent.name, binding)
            self._bindings[agent.name] = binding
        local.register_thing(thing_slug(self.channel.td()["id"]), self.channel.binding)
        if self._broker is None:
            self._reach_in_process()
        else:
            self._reach_over_bus(self._broker)
            await self._serve(local, self._broker)
        # Every rule is listening before start returns, so no event is missed and
        # a stream the gateway refuses fails here.
        streams = [(rule, await self._events(rule)) for rule in self._rules]
        self._tasks = [asyncio.ensure_future(self._each(self.channel.messages(), self._on_message))]
        for rule, stream in streams:
            self._follows[rule.name] = asyncio.ensure_future(self._follow(rule, stream))
        restarts = self._restarts()
        if restarts and self._broker is not None:
            await self._watch(self._broker, list(restarts), partial(self._restarted, restarts))

    def own_tds(self) -> list[dict[str, Any]]:
        """The TDs of the Things this harness serves: its agents and its chat
        channel."""
        return [a.td() for a in self.agents.values()] + [self.channel.td()]

    def served_tds(self) -> list[dict[str, Any]]:
        """The TDs the agents and the channel are served under on the bus; none
        without a broker."""
        return list(self.gateway.projected_tds.values()) if self.gateway else []

    async def aclose(self) -> None:
        for watcher in self._watchers:
            # DISCONNECT ends the network thread at once; the join runs off the loop.
            watcher.disconnect()
            await asyncio.to_thread(watcher.loop_stop)
        for task in [*self._tasks, *self._follows.values(), *self._running]:
            task.cancel()
        if self.gateway is not None:
            await self.gateway.aclose()
        clients = [*self._actors.values(), *self._readers.values(), *self._doers.values()]
        for client in [*clients, *self._sources.values()]:
            await client.aclose()

    # -- what an agent does ------------------------------------------------ #

    async def decide(self, agent: Agent, trigger: dict[str, Any]) -> dict[str, Any]:
        """Read what the agent reads, answer its questions, run the steps the
        answers call for, and emit the decision. A trigger keeps the
        ``correlation`` it carries or gets a new one; a repeat of one this harness
        already handled for the agent runs nothing and gets the first decision."""
        correlation = _correlation(trigger)
        return await self._once(
            f"{agent.name}__decide {correlation}",
            partial(self._decide, agent, trigger, correlation),
        )

    async def _decide(
        self, agent: Agent, trigger: dict[str, Any], correlation: str
    ) -> dict[str, Any]:
        actor = self._actors[agent.name]
        context: dict[str, Any] = {"trigger": unrouted(trigger)}
        for name in agent.reads:
            context[name] = await actor.read_property(name)
        image = await actor.read_property(agent.image) if agent.image else None
        decider = self._decider
        if decider is None:
            decider = DecisionsDecider() if agent.decides_only else ChatDecider()
        result = await decider.answer(agent, context, image if isinstance(image, str) else None)
        answers = result["answers"]
        acted: list[dict[str, Any]] = []
        for answer in answers:
            value = answer.get(answer["type"])
            if isinstance(value, str):
                acted += await self._run(agent, agent.act.get(answer["name"], {}).get(value, ()))
        decision = {
            "agent": agent.name,
            "correlation": correlation,
            "trigger": trigger,
            "answers": answers,
            "acted": acted,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        decision |= {k: result[k] for k in ("usage", "cost") if k in result}
        self._write({"agent": agent.name, "decided": decision})
        self._bindings[agent.name].emit("decided", decision)
        return decision

    async def ask(self, agent: Agent, prompt: str) -> dict[str, Any]:
        """Answer from what the agent can read. Nothing it reads can make it act."""
        only_decides(agent)
        image = await self._actors[agent.name].read_property(agent.image) if agent.image else None
        host = LLMHost(
            self._readers[agent.name],
            model=agent.model or None,
            system=agent.instructions,
            chat_fn=self._chat(agent),
        )
        answer = await (host.see(image, prompt) if isinstance(image, str) else host.chat(prompt))
        return {"answer": answer}

    async def do(
        self,
        agent: Agent,
        task: str,
        person: str | None = None,
        trigger: dict[str, Any] | None = None,
        correlation: str | None = None,
    ) -> dict[str, Any]:
        """Carry out ``task`` with the actions the agent may take, and emit what was
        done. The model chooses each call and its arguments. An action that needs
        approval waits for ``person``, or else the agent's own person. The task
        keeps ``correlation``, or the trigger's, as ``decide`` does."""
        only_decides(agent)
        doer = self._doers.get(agent.name)
        if doer is None:
            raise ValueError(f"{agent.name} may take no action, so it cannot do a task")
        correlation = correlation or _correlation(trigger)
        return await self._once(
            f"{agent.name}__do {correlation}",
            partial(self._do, agent, doer, task, person, trigger, correlation),
        )

    async def _do(
        self,
        agent: Agent,
        doer: ThingClient,
        task: str,
        person: str | None,
        trigger: dict[str, Any] | None,
        correlation: str,
    ) -> dict[str, Any]:
        prompt = task
        if trigger is not None:
            prompt += f"\n\nWhat started this: {json.dumps(unrouted(trigger), default=str)}"
        acted: list[dict[str, Any]] = []
        reset = (_acted.set(acted), _asker.set(person))
        try:
            self._note(agent, "task", prompt)
            image = await doer.read_property(agent.image) if agent.image else None
            host = LLMHost(
                doer,
                model=agent.model or None,
                system=agent.instructions,
                max_rounds=agent.rounds,
                chat_fn=self._chat(agent),
            )
            answer = await (
                host.see(image, prompt) if isinstance(image, str) else host.chat(prompt)
            )
            self._note(agent, "answer", answer)
        finally:
            _acted.reset(reset[0])
            _asker.reset(reset[1])
        did = {
            "agent": agent.name,
            "correlation": correlation,
            "task": task,
            "answer": answer,
            "acted": acted,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        self._write({"agent": agent.name, "did": did})
        self._bindings[agent.name].emit("did", did)
        return did

    # -- reaching the fleet ------------------------------------------------ #

    def _reach_over_bus(self, broker: str) -> None:
        elsewhere = other_brokers(self._fleet, broker)
        if elsewhere:
            raise ValueError(
                f"thingagent logs in only to {broker}, and the fleet's TDs name another "
                f"broker: {', '.join(elsewhere)}"
            )
        for rule in self._rules:
            _, href = stream_href(self._fleet, rule.event)
            if on_request(href) and not self._tokens.get(rule.name):
                raise ValueError(
                    f"{rule.event} streams only on an authorized request; give {rule.name} a token"
                )
        risk = plain_bus_risk(broker, tls=self._tls is not None, insecure_bus=self._insecure_bus)
        if risk:
            raise ValueError(risk)
        if not self._open_bus:
            risks = open_bus_risks(
                list(self.agents.values()),
                broker_binds_identity=self._binds,
                logins=self._logins,
                guard=self._guard is not None,
                policy=self._policy is not None,
            )
            if risks:
                raise ValueError("\n".join(risks))
        # The fleet's gateway checks each call again; this refuses one before it is sent.
        pdp = _gate(self._fleet, grants(list(self.agents.values()), self._rules, self._fleet))
        for agent in self.agents.values():
            secrets = credentials(
                self._fleet, self._tokens.get(agent.name), self._logins.get(agent.name)
            )
            bus = partial(MqttBinding, credentials=secrets, timeout=self._timeout, tls=self._tls)
            self._actors[agent.name] = ThingClient(
                tds=self._fleet,
                bindings=[bus()],
                record=self._recorder(agent),
                pdp=pdp,
                identity={"sub": agent.name},
                authz_raise=False,
            )
            self._readers[agent.name] = _Tolerant(
                tds=[read_view(td, agent) for td in self._fleet],
                bindings=[bus()],
                record=self._recorder(agent),
                on_call=self._caller(agent),
            )
            if agent.may:
                self._doers[agent.name] = _Tolerant(
                    tds=[do_view(td, agent) for td in self._fleet],
                    bindings=[bus()],
                    approve=self._approver(agent),
                    approve_when="all",
                    record=self._recorder(agent),
                    on_call=self._caller(agent),
                )

    def _reach_in_process(self) -> None:
        devices = list(self._devices or BindingRegistry.default(mqtt=True).bindings)
        local = next((b for b in devices if isinstance(b, LocalBinding)), None)
        if local is None:
            local = LocalBinding()
            devices.append(local)
        allowed = self._grants
        if allowed is None:
            allowed = grants(list(self.agents.values()), self._rules, self._fleet)
        pdp = _gate(self._fleet, allowed)

        def client(
            tds: list[dict[str, Any]],
            name: str,
            kind: type[ThingClient] = ThingClient,
            **options: Any,
        ) -> ThingClient:
            identity = {"sub": name}
            return kind(
                tds=tds, bindings=devices, pdp=pdp, identity=identity, authz_raise=False, **options
            )

        for agent in self.agents.values():
            record = self._recorder(agent)
            self._actors[agent.name] = client(self._fleet, agent.name, record=record)
            self._readers[agent.name] = client(
                [read_view(t, agent) for t in self._fleet],
                agent.name,
                _Tolerant,
                record=record,
                on_call=self._caller(agent),
            )
            if agent.may:
                self._doers[agent.name] = client(
                    [do_view(t, agent) for t in self._fleet],
                    agent.name,
                    _Tolerant,
                    approve=self._approver(agent),
                    approve_when="all",
                    record=record,
                    on_call=self._caller(agent),
                )
        for rule in self._rules:
            self._sources[rule.name] = client(self._fleet, rule.name)

    async def _serve(self, local: LocalBinding, broker: str) -> None:
        self.gateway = guarded_gateway(
            self.own_tds(),
            [local],
            broker,
            grants=self._policy,
            login=self._login,
            prefix=self._prefix,
            guard=self._guard,
            broker_binds_identity=self._binds,
            tls=self._tls,
        )
        await self.gateway.start()

    async def _events(self, rule: Rule) -> AsyncIterator[Any]:
        """The event the rule reads, read as the rule. Over a guarded bus the rule
        listens on its own stream before it asks the fleet's gateway to start it. Over a
        bus, a token that changed since the rule's clients were made replaces theirs."""
        token, login = self._tokens.get(rule.name), self._logins.get(rule.name)
        source = self._sources.get(rule.name)
        if self._broker is not None and self._used.get(rule.name, token) != token:
            if source is not None:
                await self._sources.pop(rule.name).aclose()
            self._retire(self._sources.pop(f"{rule.name}.calls", None))
            source = None
        if source is None:
            # The agents this harness serves are in reach too, so a rule calls them here.
            tds = [*self._fleet, *self.served_tds()]
            # A fixed id keeps the session (MQTT 3.1.1 only), so the broker queues the
            # rule's events while thingagent is down. Two copies share it and take turns.
            user = self._login[0] if self._login else self._prefix
            client_id = f"thingagent.{user}.{rule.calls.partition('__')[0]}.{rule.name}"
            source = self._sources[rule.name] = client_as(
                tds, token, login, timeout=self._timeout, client_id=client_id, tls=self._tls
            )
            self._used[rule.name] = token
            if f"{rule.name}.calls" not in self._sources:
                # The rule calls on a client of its own, as one id holds one connection.
                calls = client_as(tds, token, login, timeout=self._timeout, tls=self._tls)
                self._sources[f"{rule.name}.calls"] = calls
        stream = await source.subscribe(rule.event)
        if token and self._broker is not None:
            td, href = stream_href(self._fleet, rule.event)
            try:
                await open_stream(td, href, credentials([td], token, login), tls=self._tls)
            except BaseException:
                await self._sources.pop(rule.name).aclose()
                raise
        return stream

    def _retire(self, client: ThingClient | None) -> None:
        """Close ``client`` once a call already sent on it has had its timeout."""
        if client is None:
            return

        async def close() -> None:
            try:
                await asyncio.sleep(self._timeout)
            finally:
                await client.aclose()

        task = asyncio.ensure_future(close())
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    async def _follow(self, rule: Rule, stream: AsyncIterator[Any] | None = None) -> None:
        """Start the rule's call on each event of ``stream``. A stream that ends or
        fails, or none, is opened again after ``REOPEN`` seconds, doubled after each
        try up to ``REOPEN_MAX``, until the harness closes, with the rule's token as
        it is then (see :meth:`_events`)."""
        delay = REOPEN
        while True:
            if stream is not None:
                try:
                    async for data in stream:
                        delay = REOPEN
                        self._on_event(rule, data)
                except Exception as exc:
                    ended = f"failed: {exc}"
                else:
                    ended = "ended"
                _log.warning(
                    "thingagent: the stream of rule %s %s; reopening it in %g s",
                    rule.name,
                    ended,
                    delay,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, REOPEN_MAX)
            try:
                stream = await self._events(rule)
            except Exception as exc:
                stream = None
                _log.warning(
                    "thingagent: rule %s could not reopen %s: %s; trying again in %g s",
                    rule.name,
                    rule.event,
                    exc,
                    delay,
                )
            else:
                _log.warning("thingagent: rule %s reopened %s", rule.name, rule.event)

    def _on_event(self, rule: Rule, data: Any) -> None:
        if not rule.matches(data):
            return
        stale = rule.stale(data)
        if stale is not None:
            _log.warning(
                "thingagent: rule %s dropped an event of %s: %s", rule.name, rule.event, stale
            )
            return
        name, _, action = rule.calls.partition("__")
        agent = self.agents.get(name)
        # With a broker, decide and do go through the agent's gateway.
        handle = (
            partial(self._called, agent, action, rule.args)
            if agent is not None and (self._broker is None or action == "approve")
            else partial(self._call_peer, rule)
        )
        task = asyncio.ensure_future(self._guarded(handle, data))
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    def _restarts(self) -> dict[str, list[Rule]]:
        """The rules' streams a guarded gateway starts on request, by the topic it
        retains their Thing's TD on. A restarted gateway republishes the TD and has
        forgotten every stream it was asked for."""
        restarts: dict[str, list[Rule]] = {}
        if self._broker is None:
            return restarts
        for rule in self._rules:
            _, href = stream_href(self._fleet, rule.event)
            if on_request(href):
                restarts.setdefault(_td_topic(href), []).append(rule)
        return restarts

    def _restarted(self, restarts: dict[str, list[Rule]], message: Any) -> None:
        """Reopen the streams whose gateway published its TD again. The broker marks
        the copy it held when the watch began as retained."""
        if message.retain or not message.payload:
            return
        for rule in restarts.get(message.topic, []):
            _log.warning(
                "thingagent: %s was served again; reopening the stream of rule %s",
                rule.event,
                rule.name,
            )
            self._follows[rule.name].cancel()
            self._follows[rule.name] = asyncio.ensure_future(self._follow(rule))

    async def _called(
        self, agent: Agent, action: str, args: dict[str, Any], data: dict[str, Any]
    ) -> None:
        if action == "decide":
            await self.decide(agent, data)
        elif action == "do":
            await self.do(agent, args["task"], trigger=data)
        else:
            await self.channel.answer(args["person"], bool(args["yes"]), args["action"])

    async def _call_peer(self, rule: Rule, data: dict[str, Any]) -> None:
        """Call an agent over the bus as ``rule``: ``decide`` with ``data`` as the
        trigger, ``do`` and ``approve`` with the rule's args, and ``do`` with the
        trigger and its correlation too."""
        args = data if rule.calls.endswith("__decide") else rule.args
        if rule.calls.endswith("__do"):
            args = {**args, "correlation": _correlation(data), "trigger": data}
        caller = self._sources.get(f"{rule.name}.calls") or self._sources[rule.name]
        result = await caller.invoke(rule.calls, args, timeout=self._timeout)
        if isinstance(result, dict) and ("error" in result or result.get("denied")):
            _log.warning("thingagent: rule %s was refused %s: %s", rule.name, rule.calls, result)

    async def _watch(self, broker: str, topics: list[str], seen: Callable[[Any], None]) -> None:
        """Pass each message on ``topics`` to ``seen`` on the event loop, logged in as
        thingagent. Returns once the broker acknowledges the subscription: a TD
        published before then arrives marked retained, which ``_restarted`` skips."""
        # Optional dependency of thingctx, required by thingagent.
        import paho.mqtt.client as mqtt  # noqa: PLC0415

        loop = asyncio.get_running_loop()
        subscribed = asyncio.Event()

        def on_connect(client: Any, *_: Any) -> None:
            # One SUBSCRIBE for every topic, so one SUBACK covers them all.
            client.subscribe([(watched, 1) for watched in topics])

        def on_subscribe(*_: Any) -> None:
            loop.call_soon_threadsafe(subscribed.set)

        def on_message(_client: Any, _userdata: Any, message: Any) -> None:
            loop.call_soon_threadsafe(seen, message)

        host, port, _ = broker_address(broker)
        watcher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        watcher.on_connect = on_connect
        watcher.on_subscribe = on_subscribe
        watcher.on_message = on_message
        if self._login is not None:
            watcher.username_pw_set(*self._login)
        if self._tls is not None:
            self._tls.apply(watcher)
        watcher.connect(host, port)
        watcher.loop_start()
        self._watchers.append(watcher)
        await asyncio.wait_for(subscribed.wait(), self._timeout)

    # -- plumbing ---------------------------------------------------------- #

    async def _once(
        self, key: str, run: Callable[[], Coroutine[Any, Any, dict[str, Any]]]
    ) -> dict[str, Any]:
        """``run()``, unless ``key`` ran in the last ``WINDOW`` seconds and is among
        the last ``REMEMBER`` keys: then nothing runs and the first result returns."""
        now = time.monotonic()
        while self._handled and (
            len(self._handled) >= REMEMBER or now - next(iter(self._handled.values()))[0] > WINDOW
        ):
            del self._handled[next(iter(self._handled))]
        first = self._handled.get(key)
        if first is not None:
            _log.warning("thingagent: dropped a repeated trigger, %s", key)
            return await asyncio.shield(first[1])
        task = asyncio.ensure_future(run())
        self._handled[key] = (now, task)
        try:
            return await task
        except BaseException:
            # A run that failed may be tried again.
            self._handled.pop(key, None)
            raise

    def _handlers(self, agent: Agent) -> dict[str, Callable[..., Awaitable[Any]]]:
        async def decide(**trigger: Any) -> dict[str, Any]:
            return await self.decide(agent, trigger)

        async def ask(prompt: str) -> dict[str, Any]:
            return await self.ask(agent, prompt)

        async def approve(person: str, yes: bool, action: str) -> dict[str, Any]:
            return await self.channel.answer(person, yes, action)

        async def do(
            task: str, correlation: str | None = None, trigger: dict[str, Any] | None = None
        ) -> dict[str, Any]:
            return await self.do(agent, task, trigger=trigger, correlation=correlation)

        handlers: dict[str, Callable[..., Awaitable[Any]]] = {
            "decide": decide,
            "ask": ask,
            "approve": approve,
        }
        if agent.may:
            handlers["do"] = do
        return handlers

    def _chat(self, agent: Agent) -> ChatFn | None:
        if self._chat_fn is not None or not agent.model:
            return self._chat_fn
        return chat_fn(agent.model, agent.options)

    async def _run(
        self, agent: Agent, steps: tuple[Step, ...], confirmed: bool = False
    ) -> list[dict[str, Any]]:
        done: list[dict[str, Any]] = []
        for step in steps:
            if step.approve and not confirmed:
                person = agent.person
                if not person or not await self._confirm(agent, person, step.approve, step.invoke):
                    done.append({"invoke": step.invoke, "declined": True})
                    continue
            try:
                result = await self._actors[agent.name].invoke(step.invoke, dict(step.args))
            except TransportError as exc:
                # A broker that refuses the publish, or a bus that does not answer.
                _log.warning("thingagent: %s got no answer to %s: %s", agent.name, step.invoke, exc)
                done.append({"invoke": step.invoke, "failed": str(exc)})
                continue
            refused = isinstance(result, dict) and ("error" in result or result.get("denied"))
            done.append({"invoke": step.invoke, "refused" if refused else "result": result})
        return done

    async def _confirm(self, agent: Agent, person: str, question: str, action: str) -> bool:
        """Ask ``person`` ``question`` about ``action`` for ``agent``, showing it is
        waiting with the agent's ``signal`` steps and that it is answered with its
        ``clear`` steps."""
        timeout = agent.approval.timeout or self._confirm_timeout
        await self._run(agent, agent.approval.signal, confirmed=True)
        try:
            return await self.channel.confirm(person, question, timeout, action)
        finally:
            await self._run(agent, agent.approval.clear, confirmed=True)

    def _approver(self, agent: Agent) -> Callable[[ApprovalRequest], Awaitable[bool]]:
        """Approves each call a task makes: at once, or, for an action that needs
        approval, only when its person says yes."""

        async def approve(request: ApprovalRequest) -> bool:
            question = agent.approve.get(request.tool_name)
            if question is None:
                return True
            person = _asker.get() or agent.person
            ask = f"{question} ({request.tool_name} {json.dumps(request.arguments)}) yes or no?"
            return bool(person) and await self._confirm(agent, str(person), ask, request.tool_name)

        return approve

    def _recorder(self, agent: Agent) -> Callable[[Any], None]:
        """A ``record`` for the agent's clients: each SOSA record goes to the trace
        and, during a task, to what the task reports it did."""

        def write(record: Any) -> None:
            entry = record.as_jsonld()
            acted = _acted.get()
            if acted is not None and type(record).__name__ != "Observation":
                acted.append(entry)
            self._write({"agent": agent.name, "record": entry})

        return recorder(write, actor=f"urn:thingagent:agent:{agent.name}")

    def _caller(self, agent: Agent) -> Callable[[dict[str, Any]], None]:
        """Writes each call a model makes, with what it read back, to the trace."""

        def seen(call: dict[str, Any]) -> None:
            self._write({"agent": agent.name, **call})

        return seen

    def _note(self, agent: Agent, kind: str, text: str) -> None:
        self._write({"agent": agent.name, kind: text})

    def _write(self, entry: dict[str, Any]) -> None:
        if self._trace is not None:
            write_trace(self._trace, entry)

    async def _each(self, stream: Any, handle: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        # Each item runs on its own task: an ask waits for a person's reply, which
        # arrives as a later item on the same stream.
        async for item in stream:
            task = asyncio.ensure_future(self._guarded(handle, item))
            self._running.add(task)
            task.add_done_callback(self._running.discard)

    async def _guarded(
        self, handle: Callable[[dict[str, Any]], Awaitable[None]], item: dict[str, Any]
    ) -> None:
        try:
            await handle(item)
        except Exception:
            _log.exception("thingagent: handling %s failed", item)

    async def _on_message(self, message: dict[str, Any]) -> None:
        person, text = message["person"], message["text"].strip()
        name, _, prompt = text.partition(" ") if text.startswith("@") else ("", "", text)
        agent = self.agents.get(name.lstrip("@")) if name else None
        if agent is None or agent.name not in self._allow.get(person, set()):
            await self.channel.send(person, "Name an agent you may ask, like @doorwatch.")
            return
        if agent.may:
            did = await self.do(agent, prompt, person=person)
            await self.channel.send(person, did["answer"])
            return
        result = await self.ask(agent, prompt)
        await self.channel.send(person, result["answer"])
