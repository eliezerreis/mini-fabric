package com.myfabric.consumer.repository;

import com.myfabric.consumer.document.ProfileDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProfileRepository extends MongoRepository<ProfileDocument, String> {}
