package com.myfabric.api.model;

import com.myfabric.api.dto.ProfileDTO;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document(collection = "profiles")
public record Profile(
        @Id String id,
        String firstName,
        String lastName,
        String email,
        String phoneNumber,
        String department,
        Instant createdAt,
        Instant updatedAt,
        int schemaVersion
) {
    public static Profile from(ProfileDTO dto) {
        return new Profile(null,
                dto.firstName(), dto.lastName(), dto.email(),
                dto.phoneNumber(), dto.department(),
                Instant.now(), Instant.now(), 2);
    }

    public Profile withUpdates(ProfileDTO dto) {
        return new Profile(id,
                dto.firstName(), dto.lastName(), dto.email(),
                dto.phoneNumber(), dto.department(),
                createdAt, Instant.now(), schemaVersion);
    }
}
