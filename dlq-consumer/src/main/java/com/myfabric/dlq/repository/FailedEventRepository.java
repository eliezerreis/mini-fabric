package com.myfabric.dlq.repository;

import com.myfabric.dlq.model.FailedEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface FailedEventRepository extends MongoRepository<FailedEvent, String> {
    List<FailedEvent> findByStatus(String status);
    List<FailedEvent> findByProfileId(String profileId);
}
