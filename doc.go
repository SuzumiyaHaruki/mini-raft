// Package miniraft implements a deliberately small Raft-like protocol core.
//
// It is a development benchmark for studying test-control seams. It is not a
// production consensus implementation and intentionally omits log conflict
// repair, snapshots, membership changes, persistence, and real networking.
package miniraft
