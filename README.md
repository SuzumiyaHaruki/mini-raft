# Mini Raft

Mini Raft is a deliberately small Go consensus protocol used as the first
development benchmark for ConsensusSeam. It is not intended for production.

The implementation provides:

- follower, candidate, and leader roles;
- vote request/response and append request/response messages;
- explicit logical `Tick()` progression;
- a normal protocol input boundary through `Node.Step()`;
- proposals and read-only status observation;
- an in-memory transport whose `Send()` immediately delivers to the target;
- internal randomized election timeouts;
- pause/resume semantics that explicitly do not define crash recovery.

The baseline intentionally has no controlled transport, Pending Store, stable
message IDs, or `Inject(id)` operation. Those are the message-control seams that a
ConsensusSeam transformation is expected to add without changing production
behavior.

## Baseline tests

```bash
go test ./...
```

## ConsensusSeam acceptance tests

`acceptance/message_control_test.go` is protected by the `consensusseam` build
tag. Before transformation it must fail to compile because the expected seam does
not exist:

```bash
go test -tags consensusseam ./acceptance
```

After a successful transformation, the tagged tests validate:

- MC1: outbound messages are captured;
- MC2: captured messages do not continue through the original transport;
- MC3: injecting one selected ID consumes only that message and delivers it to
  its recorded target through the normal protocol handler; a failed delivery
  keeps the selected message pending.

The included `consensus-seam.project.yaml` can be passed directly to the
ConsensusSeam CLI once that package is installed. Its first smoke experiment is
intentionally scoped to `message_capture` and `message_injection`; Agent 1 still
reports all other capabilities, but Agent 2 does not transform randomness yet.
