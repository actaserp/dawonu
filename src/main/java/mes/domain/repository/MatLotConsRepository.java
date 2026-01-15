package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MatLotCons;

@Repository
public interface MatLotConsRepository extends JpaRepository<MatLotCons, Integer> {
	
	MatLotCons getMatLotConsById(Integer id);

	List<MatLotCons> findByMaterialLotId(int id);

	List<MatLotCons> findBySourceTableNameAndSourceDataPk(String string, int id);

	void deleteBySourceTableNameAndSourceDataPk(String string, int id);

	@Query("SELECT m FROM MatLotCons m WHERE m.sourceTableName = 'shipment' AND m.sourceDataPk IN :shipmentIds")
	List<MatLotCons> findByShipmentIds(@Param("shipmentIds") List<Integer> shipmentIds);

    List<MatLotCons> findByMaterialLotIdIn(List<Integer> list);

    @Modifying
    @Query("""
    delete from MatLotCons m
    where m.sourceTableName = :tableName
      and m.sourceDataPk IN (:lotIds)
""")
    void deleteBySourceTableNameAndSourceDataPkIn(
            @Param("tableName") String tableName,
            @Param("lotIds") List<Integer> lotIds
    );
}
