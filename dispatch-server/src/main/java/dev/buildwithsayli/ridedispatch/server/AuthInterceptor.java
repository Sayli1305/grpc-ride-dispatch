package dev.buildwithsayli.ridedispatch.server;

import io.grpc.*;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;
import org.springframework.stereotype.Component;

/**
 * Stand-in for real auth (a production service would validate a JWT or
 * call an auth service here, not compare a hardcoded string) - but the
 * mechanics are the real part: reading metadata before the call reaches
 * any service method, and rejecting with UNAUTHENTICATED rather than
 * letting a missing-credential request fail deep inside business logic.
 *
 * Applies to every RPC on every service automatically (that's what
 * @GrpcGlobalServerInterceptor means to the net.devh starter) -
 * including the streaming ones, since interception happens once per
 * call regardless of how many messages flow afterward.
 */
@Component
@GrpcGlobalServerInterceptor
public class AuthInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String EXPECTED = "Bearer demo-token";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String token = headers.get(AUTHORIZATION);
        if (token == null || !token.equals(EXPECTED)) {
            call.close(Status.UNAUTHENTICATED.withDescription("missing or invalid bearer token"), new Metadata());
            return new ServerCall.Listener<>() {
                // No-op listener: the call is already closed, so this just
                // satisfies the return type without processing any messages.
            };
        }
        return next.startCall(call, headers);
    }
}
