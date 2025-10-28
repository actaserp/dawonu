package mes.domain.repository;

import mes.domain.entity.Yearamt;
import mes.domain.entity.YearamtId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface YearamtRepository extends JpaRepository<Yearamt, YearamtId> {

  Optional<Yearamt> findByIdCltcdAndIdIoflagAndIdYyyymm(Integer cltcd, String ioflag, String yyyymm);

  void deleteByIdIoflagAndIdYyyymmAndIdCltcdAndSpjangcd(String ioflag, String yyyymm, Integer cltcd, String spjangcd);
}
