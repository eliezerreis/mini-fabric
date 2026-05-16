package com.myfabric.api.producer;

import com.myfabric.events.EventType;
import com.myfabric.events.ProfileEvent;
import org.apache.avro.specific.SpecificRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class ProfileEventProducer {

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    @Value("${fabric.kafka.topic.profile-events:profile.events}")
    private String topic;

    public ProfileEventProducer(KafkaTemplate<String, SpecificRecord> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String profileId, EventType eventType, ProfileEvent event) {
        // key = profileId ensures all events for the same profile land on the same partition (ordering guarantee)
        CompletableFuture<SendResult<String, SpecificRecord>> future =
                kafkaTemplate.send(topic, profileId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send event profileId={} type={}", profileId, eventType, ex);
            } else {
                log.info("Sent profileId={} type={} partition={} offset={}",
                        profileId, eventType,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /** Injects a raw byte payload that cannot be deserialized — demonstrates poison message handling. */
    public void sendPoisonMessage(String profileId) {
        log.warn("Injecting poison message for profileId={}", profileId);
        // Send a string instead of an Avro record — Schema Registry validation will reject it at consumer side
        kafkaTemplate.send(topic, profileId, buildMinimalEvent(profileId, EventType.UPDATED));
    }

    public ProfileEvent buildEvent(String profileId, EventType type,
                                   String firstName, String lastName, String email,
                                   String phoneNumber, String department,
                                   Map<String, String> metadata) {
        return ProfileEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setProfileId(profileId)
                .setEventType(type)
                .setTimestamp(Instant.now().toEpochMilli())
                .setFirstName(firstName)
                .setLastName(lastName)
                .setEmail(email)
                .setSchemaVersion(2)
                .setPhoneNumber(phoneNumber)
                .setDepartment(department)
                .setMergedFromId(null)
                .setMetadata(metadata)
                .build();
    }

    private ProfileEvent buildMinimalEvent(String profileId, EventType type) {
        return ProfileEvent.newBuilder()
                .setEventId("POISON-" + UUID.randomUUID())
                .setProfileId(profileId)
                .setEventType(type)
                .setTimestamp(Instant.now().toEpochMilli())
                .setFirstName("\uDEAD") // invalid UTF surrogate — will fail JSON/string processing
                .setLastName("POISON")
                .setEmail("poison@invalid")
                .setSchemaVersion(-1)
                .setPhoneNumber(null)
                .setDepartment(null)
                .setMergedFromId(null)
                .setMetadata(null)
                .build();
    }
}
