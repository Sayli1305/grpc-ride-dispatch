# Architecture

```mermaid
flowchart LR
    subgraph client-simulator
        BS[BlockingStub] -->|RequestRide, unary| DS
        AS[AsyncStub] -->|WatchRideStatus, server-stream| DS
        AS -->|StreamDriverLocation, client-stream| DS
        AS -->|Chat, bidi-stream| DS
    end

    subgraph dispatch-server
        DS[RideDispatchServiceImpl]
        DS --> DR[DriverRegistry]
        DS --> RS[RideStore]
        DS --> CR[ChatRelay]
        AUTH[AuthInterceptor] -.every call.-> DS
        LOG[LoggingInterceptor] -.every call.-> DS
    end

    LOG -->|Micrometer Timer| PROM[/actuator/prometheus]
    RS -->|status progression, timed| RS
```

Both modules share one `.proto` file (`proto/ride_dispatch.proto`) rather than each maintaining their own copy - `protobuf-maven-plugin` in each module's `pom.xml` points `protoSourceRoot` at the same shared file, so the generated Java client and server stubs are guaranteed to agree on the wire contract by construction, not by convention.

`AuthInterceptor` and `LoggingInterceptor` are registered as `@GrpcGlobalServerInterceptor` beans, which the `net.devh` starter applies to every RPC on every service automatically - unary and all three streaming shapes alike, since interception happens once per call setup regardless of how many messages flow through it afterward. `AuthClientInterceptor` is the client-side mirror, attaching the same bearer token to every outgoing call from `client-simulator`.

`RideStore` holds each ride's state and a list of `StreamObserver`s currently watching it via `WatchRideStatus` - when the ride's simulated status advances (see `RideStore.PROGRESSION`), every subscriber gets pushed an update, and the list is cleaned up via a cancellation handler if a client disconnects mid-stream.
