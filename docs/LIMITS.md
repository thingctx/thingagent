# Limits

The README lists the limits to read before trusting thingagent with a device. These
are the rest: how it behaves at the edges.

- A rule's event stream ends when the gateway restarts or the rule's token expires.
  thingagent reopens it after 1 s, doubling the wait each time up to 30 s, with the
  rule's token as it is then; events sent in between are lost. The `thingagent`
  command reads tokens from its own environment, which nothing outside the process
  can change, so there an expired token fails until thingagent restarts with a new
  one. A gateway that stops sending goes unseen.
- Each rule keeps a broker session. The broker queues its events while thingagent
  is down and delivers them, however old, when it returns. A rule with `max_age`
  drops one older than that; a rule without it lets an old event start an agent
  that moves a device. `max_age` trusts the device's clock: it and thingagent's
  must agree to within `max_age`. Two copies of one rule share a session.
- A rule that approves from a device names the action, and answers only its
  person's open question about that action; anyone who can press the knob
  approves that action.
- Two copies of an agent that acts both act. They share no memory.
- The chat channel runs in the thingagent process. A chat service needs an adapter
  that calls `receive` and delivers each `message` event.
- `thingagent mcp` needs a broker, serves no events, and asks approval only for the
  actions the TDs mark as needing it.
