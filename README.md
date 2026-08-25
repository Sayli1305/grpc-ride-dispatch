# Lab 11: gRPC Ride Dispatch System

> Status: In Progress
> Started: 2026-08-09 · Published: —
> Not part of the original 10-lab backlog - added by request as a dedicated gRPC lab.

## Problem Statement

gRPC's pitch is strong typing, low overhead, and native support for streaming shapes HTTP/REST handles awkwardly at best - but a lab that only implements one unary "hello world" RPC doesn't actually test that pitch. Can one coherent, production-shaped domain exercise all four gRPC call types (unary, server-streaming, client-streaming, bidirectional-streaming) naturally, with the auth, observability, and deadline-handling concerns a real gRPC service needs, not just the happy-path RPC calls?

## Hypothesis

A ride-hailing dispatch domain maps onto all four RPC shapes without forcing any of them: matching a ride is naturally one request/one response (unary), watching a ride's status is naturally a server push over time (server-streaming), a driver's GPS trail is naturally a client push with one summary at the end (client-streaming), and in-ride chat is naturally bidirectional. If that mapping holds, the resulting service should read as one coherent system, not four disconnected RPC demos bolted together.

## Architecture

See `docs/architecture.md`. Two Spring Boot modules sharing one `.proto` file:

- **dispatch-server** (gRPC port 9090, HTTP/actuator port 8400) - implements `RideDispatchService`'s four RPCs, backed by an in-memory `DriverRegistry` (nearest-driver matching via haversine distance), `RideStore` (ride state machine that pushes status updates to subscribers), and `ChatRelay` (bidi message relay).
- **client-simulator** - a `CommandLineRunner` that drives all four RPCs against `dispatch-server` in sequence, acting as both rider and driver where needed (e.g. joining two chat streams to the same ride to make the relay demo meaningful).

Cross-cutting concerns, because a lab that skips them isn't really testing gRPC in a production shape:

- **Auth**: `AuthInterceptor`/`AuthClientInterceptor` - a bearer-token check via gRPC metadata, applied globally to every RPC (`@GrpcGlobalServerInterceptor`), not just the ones that seemed to need it.
- **Observability**: `LoggingInterceptor` records a Micrometer timer per method + final status code, exposed via `/actuator/prometheus` - the same instrument-every-request pattern used in Labs 01 and 02, applied to gRPC instead of HTTP.
- **Deadlines**: the client simulator deliberately calls `RequestRide` with a 500ms deadline against a rider ID (`SLOW_TEST`) that the server is hardcoded to delay 3 seconds for, so `DEADLINE_EXCEEDED` is demonstrated on purpose rather than hoped for.
- **Status codes**: `INVALID_ARGUMENT`, `NOT_FOUND`, `FAILED_PRECONDITION`, `UNAUTHENTICATED`, `DEADLINE_EXCEEDED`, and `INTERNAL` are all used deliberately for their actual failure modes, not defaulted to `UNKNOWN`.
- **Production debugging**: reflection is enabled, so `scripts/grpcurl-examples.sh` can call the service with no compiled client at all - the same way an on-call engineer would poke at a live gRPC service.

## Implementation

```
lab-11-grpc-ride-dispatch/
├── proto/ride_dispatch.proto        # shared by both modules - one source of truth for the contract
├── dispatch-server/
│   └── src/main/java/.../server/
│       ├── RideDispatchServiceImpl.java   # all 4 RPCs
│       ├── DriverRegistry.java              # nearest-match via haversine distance
│       ├── RideStore.java                    # ride state machine + subscriber push
│       ├── ChatRelay.java                     # bidi message relay
│       ├── AuthInterceptor.java                # global server interceptor
│       └── LoggingInterceptor.java              # Micrometer timer per method+status
├── client-simulator/
│   └── src/main/java/.../client/
│       ├── RideDispatchDemoRunner.java     # drives all 4 RPCs + the deadline demo
│       └── AuthClientInterceptor.java       # global client interceptor
├── scripts/grpcurl-examples.sh
├── docker-compose.yml                 # optional - see Experiments
└── docs/architecture.md
```

No database, no message broker, no external infra at all - this is deliberately the first "complex" lab in this series that needs nothing beyond a JVM and Maven to run for real.

## Experiments

Quick start (no Docker needed):

```bash
# terminal 1
cd dispatch-server && mvn spring-boot:run

# terminal 2, once dispatch-server is up
cd client-simulator && mvn spring-boot:run
```

`client-simulator` runs the full demo automatically on startup and prints each RPC's result, including the expected `DEADLINE_EXCEEDED` at the end. Total runtime is roughly 20-25 seconds, mostly spent waiting on `WatchRideStatus`'s simulated ride progression (4 steps, 3 seconds apart).

Manual production-style debugging via `grpcurl` (reflection-enabled, no compiled client needed):

```bash
./scripts/grpcurl-examples.sh
```

Optional Docker packaging, for parity with how the other labs ship:

```bash
docker compose up -d --build
```

**Verification status**: unlike every other lab in this series, this one has not even received the usual structural pass (valid XML, balanced braces, valid YAML) - the sandboxed environment's shell access was unavailable for the entire time this lab was built (VM service down). The code was written carefully and reviewed by re-reading each file, but nothing here has been mechanically checked yet, let alone run. Treat this as a first draft that needs the standard verification pass - and an actual `mvn spring-boot:run` - before trusting any of it. That verification is expected to happen in a follow-up pass once the sandbox is back.

## Metrics

_Populated once run for real - paste `client-simulator`'s console output here (all 4 RPC results + the deadline demo), plus a snapshot of `curl localhost:8400/actuator/prometheus | grep grpc_server_calls` showing per-method call counts and status codes._

## Failures

Nothing yet from a live run - see the note on verification status at the end of Experiments for what has and hasn't been checked in this environment. But manual review (the only verification available while the sandbox was down) did catch two real bugs before they'd have surfaced at compile time or in the demo output:

- **A genuine compile error**: `RideDispatchServiceImpl.watchRideStatus` originally did `if (responseObserver instanceof ServerCallStreamObserver<RideStatusUpdate> serverObserver)` - Java disallows `instanceof` against a parameterized generic type like that (type erasure means the JVM can't check the type argument at runtime), so this would have failed to compile, not just misbehaved. Fixed to the unbounded wildcard form, `instanceof ServerCallStreamObserver<?> serverObserver`.
- **A real logic bug in the chat demo**: `ChatRelay` only registers a stream as a ride's chat participant on that stream's *first* `onNext` (see `RideDispatchServiceImpl.chat()`), so a message sent before the other party has joined has nobody to relay to and is silently dropped. `RideDispatchDemoRunner`'s original ordering had the rider send first, before the driver had joined anything - so the rider's message would vanish and the wrong `CountDownLatch` would end up waiting on it. Fixed by reordering: driver joins first (a message nobody's listening for yet), then rider joins (delivered to the now-present driver), then driver replies (delivered to the now-present rider).

Neither of these would have been caught by a brace-balance or YAML-validity check - the first needed actual Java semantics knowledge, the second needed reasoning through the relay's join-on-first-message timing. That's worth remembering next time "no bash access" tempts skipping straight to "looks fine on read-through."

One thing still worth testing deliberately once this runs for real: `RideStore`'s cancellation handling (`ServerCallStreamObserver.setOnCancelHandler`) - kill `client-simulator` mid-`WatchRideStatus` and confirm `dispatch-server` doesn't keep trying to push to a dead stream or leak the subscriber reference. That's the kind of bug that's invisible in a happy-path demo and only shows up under real client churn.

## Conclusions

_To be written after a real run - state plainly whether all 4 RPC types worked as designed, whether the deadline demo actually produced DEADLINE_EXCEEDED, and whether the ride-hailing domain mapping held up as cleanly as the hypothesis predicted or needed adjustment._

## Future Work

- Replace the hardcoded bearer-token check with real JWT validation (e.g. Spring Security's OAuth2 resource server support, which has direct gRPC interceptor integration patterns).
- Back `DriverRegistry` with a geo-indexed store (Redis GEO commands, or PostGIS) once fleet size would make the linear scan in `findNearestAvailable` actually matter - directly parallel to Lab 07's "this works until it doesn't" milestone structure.
- Add gRPC's built-in retry policy (via service config) on the client side and test it against Lab 02-style broker/service chaos - does automatic retry on `UNAVAILABLE` actually help, or does it just move the problem?
- TLS between client and server instead of plaintext - this lab's `negotiation-type: plaintext` is explicitly a demo shortcut, called out in `client-simulator`'s `application.yml`.

## Reusable Components

`AuthInterceptor`/`AuthClientInterceptor` and `LoggingInterceptor` are fully generic - nothing about them is specific to ride dispatch - and are strong Athena candidates as a "gRPC interceptor starter pack" for any future lab that adds a gRPC service.
