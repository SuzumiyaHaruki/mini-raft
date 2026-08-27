# Mini Raft

Mini Raft is a deliberately small Go consensus protocol intended for education
and testing. It is not a production consensus implementation.

The implementation provides:

- follower, candidate, and leader roles;
- vote request/response and append request/response messages;
- explicit logical `Tick()` progression;
- a normal protocol input boundary through `Node.Step()`;
- proposals and read-only status observation;
- an in-memory transport whose `Send()` immediately delivers to the target;
- internal randomized election timeouts;
- pause/resume semantics that explicitly do not define crash recovery.

## Tests

```bash
go test ./...
```
