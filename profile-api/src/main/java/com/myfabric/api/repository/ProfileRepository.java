package com.myfabric.api.repository;

import com.myfabric.api.document.ProfileDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProfileRepository extends MongoRepository<ProfileDocument, String> {}
