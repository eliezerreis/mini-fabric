package com.myfabric.dlq.repository;

import com.myfabric.dlq.document.FailedEventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface FailedEventRepository extends MongoRepository<FailedEventDocument, String> {
    List<FailedEventDocument> findByStatus(String status);
    List<FailedEventDocument> findByProfileId(String profileId);
}
