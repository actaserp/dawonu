package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Factory;

import java.util.List;

@Repository
public interface FactoryRepository extends JpaRepository<Factory, Integer>{

    Factory getFactoryById(Integer id);

}
