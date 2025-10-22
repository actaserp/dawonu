package mes.domain.entity;

import java.sql.Timestamp;

import javax.persistence.*;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import mes.app.util.FloatToDoubleConverter;

@Entity
@Table(name="mat_lot_cons")
@Setter
@Getter
@NoArgsConstructor
public class MatLotCons extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"OutputDateTime\"")
	Timestamp outputDateTime;

	@Convert(converter = FloatToDoubleConverter.class)
	@Column(name = "\"OutputQty\"")
	Float outputQty;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"MaterialLot_id\"")
	Integer materialLotId;
	
	@Column(name = "\"PrevStock\"")
	Float prevStock;
	
	@Column(name = "\"CurrentStock\"")
	Float currentStock;

	@Column(name = "\"shipment_id\"")
	int shipmentId;

	String spjangcd;
}
