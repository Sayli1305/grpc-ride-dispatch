#!/usr/bin/env bash
# Manual production-debugging examples using grpcurl
# (https://github.com/fullstorydev/grpcurl) - this is what an on-call
# engineer actually does against a running gRPC service: no compiled
# client needed, because reflection-service-enabled=true in
# dispatch-server's application.yml lets grpcurl discover the schema
# at runtime.
#
# Install grpcurl: https://github.com/fullstorydev/grpcurl#installation
# Prerequisite: dispatch-server running locally (mvn spring-boot:run),
# gRPC port 9090.

set -euo pipefail
HOST="localhost:9090"
AUTH_HEADER="authorization: Bearer demo-token"

echo "=== List services (via reflection) ==="
grpcurl -plaintext "$HOST" list

echo ""
echo "=== Describe RideDispatchService ==="
grpcurl -plaintext "$HOST" describe dev.buildwithsayli.ridedispatch.RideDispatchService

echo ""
echo "=== Health check ==="
grpcurl -plaintext "$HOST" grpc.health.v1.Health/Check

echo ""
echo "=== Unary call: RequestRide (with auth header) ==="
grpcurl -plaintext -H "$AUTH_HEADER" -d '{
  "rider_id": "rider-99",
  "pickup": {"latitude": 37.7749, "longitude": -122.4194},
  "dropoff": {"latitude": 37.8044, "longitude": -122.2712}
}' "$HOST" dev.buildwithsayli.ridedispatch.RideDispatchService/RequestRide

echo ""
echo "=== Same call WITHOUT the auth header - expect UNAUTHENTICATED ==="
grpcurl -plaintext -d '{"rider_id": "rider-99"}' \
  "$HOST" dev.buildwithsayli.ridedispatch.RideDispatchService/RequestRide || true

echo ""
echo "=== Server streaming: WatchRideStatus (paste a real ride_id from above) ==="
echo 'grpcurl -plaintext -H "'"$AUTH_HEADER"'" -d '"'"'{"ride_id": "<paste-ride-id>"}'"'"' \'
echo "  $HOST dev.buildwithsayli.ridedispatch.RideDispatchService/WatchRideStatus"
