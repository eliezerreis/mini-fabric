package com.myfabric.dlq.service;

import com.myfabric.dlq.model.FailedEvent;
import com.myfabric.dlq.repository.FailedEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FailedEventService {

    private final FailedEventRepository repository;

    public FailedEventService(FailedEventRepository repository) {
        this.repository = repository;
    }

    public void persist(String profileId, String topic, int partition, long offset,
                        String exceptionClass, String exceptionMessage, String rawPayload) {
        var event = FailedEvent.from(profileId, topic, partition, offset,
                exceptionClass, exceptionMessage, rawPayload);
        repository.save(event);
        log.info("Persisted failed event to MongoDB id={}", event.id());
    }
}
