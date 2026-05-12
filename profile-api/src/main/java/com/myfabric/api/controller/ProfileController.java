package com.myfabric.api.controller;

import com.myfabric.api.document.ProfileDocument;
import com.myfabric.api.model.ProfileRequest;
import com.myfabric.api.producer.ProfileEventProducer;
import com.myfabric.api.repository.ProfileRepository;
import com.myfabric.events.EventType;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/profiles")
public class ProfileController {

    private final ProfileRepository repository;
    private final ProfileEventProducer producer;

    public ProfileController(ProfileRepository repository, ProfileEventProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ProfileDocument create(@RequestBody ProfileRequest req) {
        var doc = new ProfileDocument();
        doc.setId(UUID.randomUUID().toString());
        doc.setFirstName(req.firstName());
        doc.setLastName(req.lastName());
        doc.setEmail(req.email());
        doc.setPhoneNumber(req.phoneNumber());
        doc.setDepartment(req.department());
        doc.setCreatedAt(Instant.now());
        doc.setUpdatedAt(Instant.now());
        doc.setSchemaVersion(2);
        repository.save(doc);

        producer.send(doc.getId(), EventType.CREATED,
                producer.buildEvent(doc.getId(), EventType.CREATED,
                        req.firstName(), req.lastName(), req.email(),
                        req.phoneNumber(), req.department()));
        return doc;
    }

    @PutMapping("/{id}")
    public ResponseEntity<ProfileDocument> update(@PathVariable String id, @RequestBody ProfileRequest req) {
        return repository.findById(id).map(doc -> {
            doc.setFirstName(req.firstName());
            doc.setLastName(req.lastName());
            doc.setEmail(req.email());
            doc.setPhoneNumber(req.phoneNumber());
            doc.setDepartment(req.department());
            doc.setUpdatedAt(Instant.now());
            repository.save(doc);
            producer.send(id, EventType.UPDATED,
                    producer.buildEvent(id, EventType.UPDATED,
                            req.firstName(), req.lastName(), req.email(),
                            req.phoneNumber(), req.department()));
            return ResponseEntity.ok(doc);
        }).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id) {
        return repository.findById(id).map(doc -> {
            repository.deleteById(id);
            producer.send(id, EventType.DELETED,
                    producer.buildEvent(id, EventType.DELETED,
                            doc.getFirstName(), doc.getLastName(), doc.getEmail(),
                            doc.getPhoneNumber(), doc.getDepartment()));
            return ResponseEntity.noContent().<Void>build();
        }).orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}")
    public ResponseEntity<ProfileDocument> get(@PathVariable String id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
