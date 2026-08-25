package dev.buildwithsayli.ridedispatch.server;

import io.grpc.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Records a Micrometer timer per RPC, tagged by method name and final
 * status code, exposed via /actuator/prometheus - the same
 * "instrument every request, tag by outcome" pattern used across the
 * other labs (Lab 01's request/downstream timers, Lab 02's retry/DLQ
 * counters), applied here to gRPC instead of HTTP.
 *
 * The trick is that gRPC doesn't know the final status until the call
 * closes - possibly long after headers were sent for a streaming RPC -
 * so timing has to hook ServerCall.close(), not just the call start.
 */
@Component
@GrpcGlobalServerInterceptor
public class LoggingInterceptor implements ServerInterceptor {

    private final MeterRegistry registry;

    public LoggingInterceptor(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        long startNanos = System.nanoTime();
        String method = call.getMethodDescriptor().getFullMethodName();

        ServerCall<ReqT, RespT> timedCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                Timer.builder("grpc.server.calls")
                        .description("gRPC server call duration by method and status")
                        .tag("method", method)
                        .tag("status", status.getCode().name())
                        .register(registry)
                        .record(Duration.ofNanos(System.nanoTime() - startNanos));
                super.close(status, trailers);
            }
        };

        return next.startCall(timedCall, headers);
    }
}
