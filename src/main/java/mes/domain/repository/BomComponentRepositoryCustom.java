package mes.domain.repository;

import mes.domain.entity.BomComponent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface BomComponentRepositoryCustom {

    void insertIfNotExists(Long bomId, Integer materialId, int amount);
}