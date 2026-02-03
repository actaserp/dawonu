package mes.domain.repository;


import java.util.List;
import java.util.Optional;

import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import mes.domain.entity.JobRes;

@Repository
public interface JobResRepository extends JpaRepository<JobRes, Integer> {
	
	JobRes getJobResById(Integer id);

	List<JobRes> getJobResByParentId(Integer parentId);

	List<JobRes> findBySourceDataPkAndSourceTableName(Integer id, String string);

	List<JobRes> findBySourceDataPkAndSourceTableNameAndMaterialIdAndIdNotIn(Integer id, String string,
			Integer material_id, List<Integer> id2);

	@Modifying
	@Query("UPDATE JobRes j SET j.state = :state WHERE j.id = :jrPk")
	void updateStateById(@Param("jrPk") Integer jrPk, @Param("state") String state);

	@Query("""
		SELECT jr.id
		FROM JobRes jr
		JOIN Workcenter wc ON wc.id = jr.workCenter_id
		WHERE jr.workOrderNumber = :orderNum
		  AND wc.processId = :processId
		  AND jr.materialId = :prodMatId
		  AND jr.state <> 'canceled'
		ORDER BY jr.id DESC
	""")
	Integer findIdByOrderProcessAndMaterial(String orderNum, Integer processId, Integer prodMatId);

    Optional<JobRes> findTopByWorkOrderNumberAndParentIdIsNotNullOrderByWorkIndexDesc(
            String workOrderNumber
    );

    @Query("""
    SELECT j
    FROM JobRes j
    JOIN Material m ON m.id = j.materialId
    WHERE j.workOrderNumber = :workOrderNumber
      AND j.workIndex > :workIndex
      AND j.state <> 'ordered'
      AND NOT (
            (m.class1 IS NULL OR m.class1 = '')
        AND (m.class2 IS NULL OR m.class2 = '')
        AND  m.class3 IS NOT NULL
        AND  m.class3 <> ''
      )
    ORDER BY j.workIndex ASC
""")
    List<JobRes> findNextProcess(
            @Param("workOrderNumber") String workOrderNumber,
            @Param("workIndex") Integer workIndex
    );

    List<JobRes> findAllByParentId(Integer deleteTargetId);

    boolean existsByParentId(int id);


    @Query("""
            SELECT j
            FROM JobRes j
            WHERE j.parentId = :parent_id
            order by j.workIndex desc
            """)
    List<JobRes> previousByParentId(Integer parent_id);


    List<JobRes> findByWorkOrderNumberAndWorkIndex(String workOrderNumber, int workIndex);

    Optional<JobRes> findByWorkOrderNumberAndParentId(String workOrderNumber, Object o);
}
