package mes.domain.repository;

import mes.domain.entity.JobPlanHead;
import mes.domain.entity.JobResProcessTree;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface JobResProcessTreeRepository extends JpaRepository<JobResProcessTree, Integer> {

    JobResProcessTree findByWorkOrderNo(String workOrderNo);

    @Query(
            value = """
    select *
    from job_res_process_tree
    where "WorkOrderNo" = :workOrderNo
    """,
            nativeQuery = true
    )
    JobResProcessTree findNative(@Param("workOrderNo") String workOrderNo);

    @Modifying
    @Query(value = "UPDATE job_res_process_tree SET \"ProcessTree\" = CAST(:processTree AS json) WHERE \"WorkOrderNo\" = :woNo", nativeQuery = true)
    int updateProcessTreeOnly(@Param("woNo") String wono, @Param("processTree") String processTree);

    void deleteByWorkOrderNo(String workOrderNumber);
}
