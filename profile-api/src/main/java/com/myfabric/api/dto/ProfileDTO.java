package com.myfabric.api.dto;

import com.myfabric.api.model.Profile;

import java.util.Map;

public record ProfileDTO(
        String firstName,
        String lastName,
        String email,
        String phoneNumber,
        String department,
        Map<String, String> metadata
) {
    public Profile toProfile() {
        return Profile.from(this);
    }
}
