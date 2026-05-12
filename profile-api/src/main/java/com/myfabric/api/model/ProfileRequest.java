package com.myfabric.api.model;

public record ProfileRequest(
        String firstName,
        String lastName,
        String email,
        String phoneNumber,
        String department
) {}
