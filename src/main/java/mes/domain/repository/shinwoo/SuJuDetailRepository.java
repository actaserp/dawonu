package mes.domain.repository.shinwoo;

import mes.domain.entity.shinwoo.suju_detail;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface SuJuDetailRepository extends JpaRepository<suju_detail, Integer> {

  void deleteBySujuId(Integer sujuId);

}
