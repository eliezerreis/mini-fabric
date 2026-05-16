package com.myfabric.consumer.repository;

import com.myfabric.consumer.model.Profile;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProfileRepository extends MongoRepository<Profile, String> {}
