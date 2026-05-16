package com.myfabric.consumer.service;

import com.myfabric.consumer.model.Profile;
import com.myfabric.consumer.repository.ProfileRepository;
import com.myfabric.events.ProfileEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ProfileService {

    private final ProfileRepository repository;

    public ProfileService(ProfileRepository repository) {
        this.repository = repository;
    }

    public void upsert(ProfileEvent event) {
        String profileId = event.getProfileId();

        Map<String, String> meta = event.getMetadata() == null ? null :
                event.getMetadata().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        var profile = new Profile(
                profileId,
                event.getFirstName(),
                event.getLastName(),
                event.getEmail(),
                event.getPhoneNumber(),
                event.getDepartment(),
                event.getMergedFromId(),
                meta,
                Instant.now(),
                event.getSchemaVersion(),
                event.getEventType().name()
        );

        repository.save(profile);
        log.info("Upserted profileId={} type={}", profileId, event.getEventType());
    }

    public void delete(String profileId) {
        repository.deleteById(profileId);
        log.info("Deleted profile profileId={}", profileId);
    }
}
