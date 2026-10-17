package ru.datacollector.persistance;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import ru.datacollector.persistance.entity.UserEntity;

@Repository
public interface UserRepository extends CrudRepository<UserEntity, Long> {}