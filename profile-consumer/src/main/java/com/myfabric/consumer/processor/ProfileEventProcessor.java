package com.myfabric.consumer.processor;

import com.myfabric.consumer.service.ProfileService;
import com.myfabric.events.ProfileEvent;
import org.springframework.stereotype.Component;

@Component
public class ProfileEventProcessor {

    private final ProfileService profileService;

    public ProfileEventProcessor(ProfileService profileService) {
        this.profileService = profileService;
    }

    public void process(ProfileEvent event) {
        if (event.getSchemaVersion() == -1) {
            throw new RuntimeException(
                "Poison message detected: profileId=" + event.getProfileId()
                + " — schemaVersion=-1 is a poison indicator. Will retry then route to DLQ.");
        }

        switch (event.getEventType()) {
            case CREATED, UPDATED, MERGED -> profileService.upsert(event);
            case DELETED -> profileService.delete(event.getProfileId().toString());
        }
    }
}
