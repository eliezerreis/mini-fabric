package com.myfabric.api.controller;

import com.myfabric.api.model.Profile;
import com.myfabric.api.dto.ProfileDTO;
import com.myfabric.api.service.ProfileService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/profiles")
public class ProfileController {

    private final ProfileService profileService;

    public ProfileController(ProfileService profileService) {
        this.profileService = profileService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Profile create(@RequestBody ProfileDTO dto) {
        return profileService.create(dto);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Profile> update(@PathVariable String id, @RequestBody ProfileDTO dto) {
        return profileService.update(id, dto)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id) {
        return profileService.delete(id)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Profile> get(@PathVariable String id) {
        return profileService.get(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
