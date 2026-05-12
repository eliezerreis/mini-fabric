package com.myfabric.api.controller;

import com.myfabric.api.producer.ProfileEventProducer;
import com.myfabric.events.EventType;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * Endpoints for triggering Kafka failure scenarios during demos/testing.
 *
 * POST /simulate/poison          - inject a message the consumer cannot process
 * POST /simulate/burst?count=N   - flood the topic to build consumer lag
 * POST /simulate/merge           - send a MERGED event (v2-only schema field)
 */
@RestController
@RequestMapping("/simulate")
public class SimulationController {

    private final ProfileEventProducer producer;

    public SimulationController(ProfileEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/poison")
    public Map<String, String> injectPoison(@RequestParam(defaultValue = "poison-profile-1") String profileId) {
        producer.sendPoisonMessage(profileId);
        return Map.of("status", "poison message sent", "profileId", profileId,
                "observe", "watch dlq-consumer logs and profile.events.dlq topic");
    }

    @PostMapping("/burst")
    public Map<String, Object> burst(@RequestParam(defaultValue = "100") int count) {
        for (int i = 0; i < count; i++) {
            String id = UUID.randomUUID().toString();
            producer.send(id, EventType.CREATED,
                    producer.buildEvent(id, EventType.CREATED,
                            "Burst", "User-" + i, "burst" + i + "@example.com", null, null));
        }
        return Map.of("status", "burst sent", "count", count,
                "observe", "watch consumer lag in Kafka UI at http://localhost:8090");
    }

    @PostMapping("/merge")
    public Map<String, String> simulateMerge(
            @RequestParam String targetProfileId,
            @RequestParam String sourceProfileId) {
        var event = com.myfabric.events.ProfileEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setProfileId(targetProfileId)
                .setEventType(EventType.MERGED)
                .setTimestamp(System.currentTimeMillis())
                .setFirstName("Merged")
                .setLastName("Profile")
                .setEmail("merged@example.com")
                .setSchemaVersion(2)
                .setPhoneNumber(null)
                .setDepartment(null)
                .setMergedFromId(sourceProfileId)
                .setMetadata(Map.of("reason", "duplicate-detected"))
                .build();
        producer.send(targetProfileId, EventType.MERGED, event);
        return Map.of("status", "merge event sent",
                "targetProfileId", targetProfileId,
                "sourceProfileId", sourceProfileId,
                "observe", "v1 consumers will read this as UPDATED (enum default), v2 consumers handle MERGED");
    }
}
