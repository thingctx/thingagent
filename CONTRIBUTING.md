# Contributing to thingagent

Thank you for helping. Keep each pull request to one change, with a test.

## Run the checks

```
pip install -e '.[dev]'
ruff check .
ruff format --check .
mypy
vulture
pytest
```

The live tests need an MQTT 5 broker and skip without one. `compose.yaml` starts
one on loopback:

```
docker compose up -d
pytest
```

`THINGAGENT_TEST_BROKER` points the tests at another broker. To run them on a
locked broker:

1. Write its rules with `python tests/locked_acl.py ACL_FILE`.
2. Add the users the rules name (`fleet_gw`, `agents_gw`, `doorwatch`, `advisor`,
   `stranger`, `person_at_door`, `chat`, `app`, `mallory`, `executor`) to its
   password file. Set `THINGAGENT_TEST_PASSWORDS` to a JSON file of user to
   password.
3. Set `THINGAGENT_TEST_BROKER` to the broker.

The tests then also check that the broker refuses an anonymous client, a wrong
password, and a request published at another login's level.

## Sign your commits (DCO)

This project uses the [Developer Certificate of Origin](https://developercertificate.org/),
not a CLA. Signing off certifies that you wrote the contribution or have the right
to submit it under Apache-2.0. `git commit -s` appends the line:

```
Signed-off-by: Your Name <you@example.com>
```

If you forget it, run `git commit -s --amend`. A real person must sign off every
commit, whatever tools helped write it.

## Security

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md).
