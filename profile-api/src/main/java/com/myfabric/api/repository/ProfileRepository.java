package com.myfabric.api.repository;

import com.myfabric.api.model.Profile;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProfileRepository extends MongoRepository<Profile, String> {}
