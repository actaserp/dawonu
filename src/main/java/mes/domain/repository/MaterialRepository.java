package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Material;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Repository
public interface MaterialRepository extends JpaRepository<Material, Integer>{

	Material getMaterialById(Integer matPk);
	
	Integer countByIdAndStoreHouseIdIsNull(Integer id);


	Material findByCode(String matCode);

    List<Material> findByIdIn(Collection<Integer> matIds);

	List<Material> findBySpjangcd(String spjangcd);

    Integer findIdByName(String materialName);

	Material findByName(String matName);

	@Query(value = "SELECT MAX(CAST(\"Code\" AS INTEGER)) FROM material WHERE LENGTH(\"Code\") = 4 AND \"Code\" ~ '^[0-9]{4}$'", nativeQuery = true)
	String findMaxCodeBy4000Prefix();

	@Query("SELECT m FROM Material m WHERE TRIM(m.name) = TRIM(:materialName)")
	Material findByNameTrimmed(@Param("materialName") String materialName);

	@Query("""
        SELECT CASE WHEN COUNT(m) > 0 THEN true ELSE false END
        FROM Material m
        WHERE m.code = :code
    """)
	boolean existsByCode(@Param("code") String code);

	@Query("SELECT m.id FROM Material m WHERE m.code = :code")
	Integer findIdByCode(@Param("code") String code);

	Material findTopByCodeStartingWithOrderByCodeDesc(String prefix);
	@Query("""
        SELECT m.code
        FROM Material m
        ORDER BY LENGTH(m.code) DESC
    """)
	List<String> findAllCodesOrderByLengthDesc();
}
