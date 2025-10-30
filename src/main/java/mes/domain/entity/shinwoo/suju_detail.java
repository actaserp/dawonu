package mes.domain.entity.shinwoo;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name="suju_detail")
@NoArgsConstructor
@Data
public class suju_detail {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  Integer id;

  @Column(name="suju_id")
  Integer sujuId;

  @Column(name="\"Standard\"")
  String Standard;

  @Column(name="\"Qty\"")
  Double Qty;

  @Column(name="\"UnitPrice\"")
  Double UnitPrice;

  @Column(name="\"UnitName\"")
  String UnitName;

  @Column(name="\"Price\"")
  Double Price;

  @Column(name="\"Vat\"")
  Double Vat;

  @Column(name="\"TotalAmount\"")
  Double TotalAmount;
}
