package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.ColumnTransformer;

import javax.persistence.*;

@Entity
@Table(name="job_res_process_tree")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class JobResProcessTree {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    int id;

    @Column(name = "\"WorkOrderNo\"")
    String workOrderNo;

    @Column(name = "\"ProcessTree\"", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::json")
    private String processTree;

}
