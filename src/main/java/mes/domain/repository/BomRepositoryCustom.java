package mes.domain.repository;

import mes.domain.entity.Bom;
import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

public interface BomRepositoryCustom {
    Long ensureBom(Integer materialId, boolean dryRun);
}