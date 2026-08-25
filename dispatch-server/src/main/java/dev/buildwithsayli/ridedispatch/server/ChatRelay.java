package dev.buildwithsayli.ridedispatch.server;

import dev.buildwithsayli.ridedispatch.grpc.ChatMessage;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Relays chat messages between whoever is connected to the same ride's
 * bidirectional Chat stream (rider and driver), excluding the sender.
 * A real system would persist chat history and support reconnects with
 * backfill - this is deliberately just the in-memory relay, since that's
 * the part that actually exercises bidi streaming.
 */
@Component
public class ChatRelay {

    private final Map<String, List<StreamObserver<ChatMessage>>> participantsByRide = new ConcurrentHashMap<>();

    public void join(String rideId, StreamObserver<ChatMessage> participant) {
        participantsByRide.computeIfAbsent(rideId, id -> new CopyOnWriteArrayList<>()).add(participant);
    }

    public void leave(String rideId, StreamObserver<ChatMessage> participant) {
        if (rideId == null) {
            return;
        }
        List<StreamObserver<ChatMessage>> participants = participantsByRide.get(rideId);
        if (participants != null) {
            participants.remove(participant);
        }
    }

    public void broadcast(String rideId, ChatMessage message, StreamObserver<ChatMessage> sender) {
        List<StreamObserver<ChatMessage>> participants = participantsByRide.get(rideId);
        if (participants == null) {
            return;
        }
        for (StreamObserver<ChatMessage> participant : participants) {
            if (participant == sender) {
                continue;
            }
            try {
                participant.onNext(message);
            } catch (Exception e) {
                // The other party's stream is already gone - drop them
                // rather than let one dead peer break delivery to others.
                participants.remove(participant);
            }
        }
    }
}
