package dev.buildwithsayli.ridedispatch.client;

import io.grpc.*;
import net.devh.boot.grpc.client.interceptor.GrpcGlobalClientInterceptor;
import org.springframework.stereotype.Component;

/**
 * Attaches the demo bearer token to every outgoing call, on every
 * @GrpcClient stub, automatically - mirrors AuthInterceptor on the
 * server side. In production this would pull a real token from a
 * credential provider (and probably refresh it), not hardcode one.
 */
@Component
@GrpcGlobalClientInterceptor
public class AuthClientInterceptor implements ClientInterceptor {

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(AUTHORIZATION, "Bearer demo-token");
                super.start(responseListener, headers);
            }
        };
    }
}
