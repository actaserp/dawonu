package mes.domain.entity;

import lombok.*;
import org.hibernate.annotations.ColumnTransformer;

import javax.persistence.*;

@Entity
@Table(name="job_res_process_tree")
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(callSuper=false)
public class JobResProcessTree {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    int id;

    @Column(name = "\"WorkOrderNo\"")
    private String workOrderNo;

    @Column(name = "\"ProcessTree\"", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::json")
    private String processTree;

}
