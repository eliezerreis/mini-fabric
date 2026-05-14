package com.myfabric.consumer.processor;

import com.myfabric.consumer.document.ProfileDocument;
import com.myfabric.consumer.repository.ProfileRepository;
import com.myfabric.events.ProfileEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Component
public class ProfileEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(ProfileEventProcessor.class);

    private final ProfileRepository repository;

    public ProfileEventProcessor(ProfileRepository repository) {
        this.repository = repository;
    }

    public void process(ProfileEvent event) {
        if (event.getSchemaVersion() == -1) {
            throw new RuntimeException(
                "Poison message detected: profileId=" + event.getProfileId()
                + " — schemaVersion=-1 is a poison indicator. Will retry then route to DLQ.");
        }

        switch (event.getEventType()) {
            case CREATED, UPDATED, MERGED -> upsert(event);
            case DELETED -> {
                repository.deleteById(event.getProfileId().toString());
                log.info("Deleted profile profileId={}", event.getProfileId());
            }
        }
    }

    private void upsert(ProfileEvent event) {
        String profileId = event.getProfileId().toString();

        ProfileDocument doc = repository.findById(profileId)
                .orElseGet(ProfileDocument::new);

        doc.setId(profileId);
        doc.setFirstName(event.getFirstName().toString());
        doc.setLastName(event.getLastName().toString());
        doc.setEmail(event.getEmail().toString());
        doc.setSchemaVersion(event.getSchemaVersion());
        doc.setLastEventType(event.getEventType().name());
        doc.setProcessedAt(Instant.now());

        // v2-only fields — safe to access because ErrorHandlingDeserializer always gives us a v2 object
        if (event.getPhoneNumber() != null) {
            doc.setPhoneNumber(event.getPhoneNumber().toString());
        }
        if (event.getDepartment() != null) {
            doc.setDepartment(event.getDepartment().toString());
        }
        if (event.getMergedFromId() != null) {
            doc.setMergedFromId(event.getMergedFromId().toString());
        }
        if (event.getMetadata() != null) {
            Map<String, String> meta = new HashMap<>();
            event.getMetadata().forEach((k, v) -> meta.put(k.toString(), v.toString()));
            doc.setMetadata(meta);
        }

        repository.save(doc);
        log.info("Upserted profileId={} type={}", profileId, event.getEventType());
    }
}
