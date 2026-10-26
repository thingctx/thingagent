# A UR5e pick cell

One plain task, "Pick up the part from the bench and put it in the tray, then go
home.", carried out by a thingagent agent on a UR5e arm. The agent never reaches
ROS: the fleet gateway runs beside rosbridge, which must listen on loopback only, so
the gateway is the one path to the robot. The agent calls as itself over a locked
broker, and the gateway checks each call against the grants the cell's owner wrote.

The cell itself is not included: no Gazebo world, no MoveIt setup, no UR driver
and no rosbridge. You need a cell that exposes the interfaces in
`things/ur5e.td.json`.

| File | What it is |
| --- | --- |
| `things/ur5e.td.json` | The arm's TD: joint states, a wrist camera with marker detections, inverse kinematics, motion, a suction gripper, and the controller manager, all over rosbridge. |
| `agents/picker.json` | The picker. It may look, move, grip and release. |
| `agents/visitor_granted.json` | What the owner grants a visitor: reads and planning only, no motion. |
| `operator.json` | The owner's policy: the agents, their broker logins, and the guard on the agents' own Things. |
| `gateway.json` | The fleet gateway that serves the arm on the broker. |

Every `password_env` in these files, and each `token_env` you add, is the name
of an environment variable, never the secret itself.

## See what the owner grants

This step needs no cell and no broker:

```
pip install thingagent
thingagent generate operator.json --gateway gateway.json --out policy
```

`policy/grants.json` gives `picker` the motion actions and `visitor` only
`compute_ik` and `list_controllers`. A visitor run from a definition that lists the
motion actions has its first move refused on the broker and at the gateway; the arm
never moves. The picker's `limits` keep every `move_to` target over the bench and the
tray.

## Run it on a cell

1. Create `keys/jwks.json` with your token issuer's public keys, and a token for
   each agent whose `sub` is its broker user.
2. Run a Mosquitto broker on 127.0.0.1:1887 with a password file for `fleet_gw`,
   `agents_gw`, `picker` and `visitor`, and `acl_file policy/acl`. For a broker
   on another host, which the cell's host can reach, set `broker` to
   `mqtts://host:port` in both files, and `tls` if its CA is not in the system's
   store; thingagent refuses one without TLS.
3. On the cell's host, with `FLEET_GW_PASSWORD` (the environment variable
   `gateway.json` names) set: `pip install 'thingagent[ros]'`, then
   `thingagent gateway gateway.json`.
4. Copy `operator.json` to CONFIG, add `"token_env"` and `"allow"`, and run
   `thingagent do CONFIG picker "Pick up the part ..." --as alice` with the `llm`
   extra.

Every call the gateway runs or refuses goes to `record/gateway.jsonl`.
