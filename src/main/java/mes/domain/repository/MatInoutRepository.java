package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MaterialInout;

@Repository
public interface MatInoutRepository extends JpaRepository<MaterialInout, Integer> {
	
	MaterialInout getMatInoutById(Integer id);

	List<MaterialInout> findBySourceTableNameAndSourceDataPkAndInOutAndInputType(String string, int id, String string2,
			String string3);

	List<MaterialInout> findBySourceTableNameAndSourceDataPkAndInOutAndOutputType(String string, int id, String string2,
			String string3);

	MaterialInout findBySourceTableNameAndSourceDataPkAndInOutAndInputTypeAndMaterialId(String string, int id,
			String string2, String string3, Integer materialId);

	MaterialInout findBySourceTableNameAndSourceDataPkAndInOutAndOutputTypeAndMaterialId(String string, int id,
			String string2, String string3, Integer consumeMatPk);

	void deleteBySourceTableNameAndSourceDataPkAndInOutAndInputType(String string, int id, String string2,
			String string3);

	void deleteBySourceTableNameAndSourceDataPkAndInOutAndOutputType(String string, int id, String string2,
			String string3);

    @Modifying
    @Query("""
    delete from MaterialInout m
    where m.sourceTableName = :sourceTableName
      and m.sourceDataPk IN (:mpid)
      and m.inOut = :inOut
      and m.inputType = :inputType
""")
    void deleteBySourceTableNameAndSourceDataPksAndInOutAndInputType(
            @Param("sourceTableName") String sourceTableName,
            @Param("mpid") List<Integer> mpid,
            @Param("inOut") String inOut,
            @Param("inputType") String inputType
    );
}
