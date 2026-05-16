package com.myfabric.api.service;

import com.myfabric.api.model.Profile;
import com.myfabric.api.dto.ProfileDTO;
import com.myfabric.api.producer.ProfileEventProducer;
import com.myfabric.api.repository.ProfileRepository;
import com.myfabric.events.EventType;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class ProfileService {

    private final ProfileRepository repository;
    private final ProfileEventProducer producer;

    public ProfileService(ProfileRepository repository, ProfileEventProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    public Profile create(ProfileDTO dto) {
        var saved = repository.save(dto.toProfile());
        producer.send(saved.id(), EventType.CREATED,
                producer.buildEvent(saved.id(), EventType.CREATED,
                        dto.firstName(), dto.lastName(), dto.email(),
                        dto.phoneNumber(), dto.department(), dto.metadata()));
        return saved;
    }

    public Optional<Profile> update(String id, ProfileDTO dto) {
        return repository.findById(id).map(existing -> {
            var saved = repository.save(existing.withUpdates(dto));
            producer.send(id, EventType.UPDATED,
                    producer.buildEvent(id, EventType.UPDATED,
                            dto.firstName(), dto.lastName(), dto.email(),
                            dto.phoneNumber(), dto.department(), dto.metadata()));
            return saved;
        });
    }

    public boolean delete(String id) {
        return repository.findById(id).map(profile -> {
            repository.deleteById(id);
            producer.send(id, EventType.DELETED,
                    producer.buildEvent(id, EventType.DELETED,
                            profile.firstName(), profile.lastName(), profile.email(),
                            profile.phoneNumber(), profile.department(), null));
            return true;
        }).orElse(false);
    }

    public Optional<Profile> get(String id) {
        return repository.findById(id);
    }
}
