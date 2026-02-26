package mes.app.production.production_package.ProductionStrategy.Bomprocess;

import lombok.RequiredArgsConstructor;
import mes.Exception.CustomException;
import mes.app.production.Enum.ProcessType;
import mes.app.production.dto.BomProcessContext;
import mes.app.production.dto.productionResult.NextContext;
import mes.app.production.production_package.BomNode;
import mes.app.production.production_package.BomTreeService;
import mes.app.production.production_package.ProcessFlow;
import mes.app.production.production_package.ProcessTreeCalculate.ProcessTreeCalculateService;
import mes.app.production.production_package.ProductionStrategy.productionStart.ProcessStartStrategy;
import mes.app.util.JsonUtil;
import mes.domain.entity.*;
import mes.domain.repository.*;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManager;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class ActiveBomProcessor implements BomProcessor{

    private final JobResRepository jobResRepository;
    private final EntityManager entityManager;
    private final JobResProcessTreeRepository jobResProcessTreeRepository;
    private final MaterialRepository materialRepository;
    private final SqlRunner sqlRunner;
    private final MatConsuRepository matConsuRepository;
    private final MatInoutRepository matInoutRepository;
    private final ProcessTreeCalculateService processTreeCalculateService;


    @Override
    public void createJobRes(BomProcessContext ctx) {
        // Context에서 재료 꺼내기
        List<Map<String, Object>> bomList = ctx.bomList();
        ProcessFlow flow = ctx.flow();
        JobRes header = ctx.jobRes();
        ProcessStartStrategy strategy = ctx.strategy();
        User user = ctx.user();

        BomTreeService bomTreeService = new BomTreeService();

        // 1. BOM 트리 생성
        Map<String, BomNode> bomTree = bomTreeService.buildTree(bomList);

        // 2. 처음 작업에 대한 품목 및 현재 공정 판별
        bomTreeService.markFirstCurrentProcess(flow, bomTree, header.getOrderQty());

        // 3. 부모(마스터 작지) 저장 - 트리거 번호 생성을 위해 flush/refresh 실행
        JobRes savedHeaderEntity = jobResRepository.save(header);
        jobResRepository.flush();
        entityManager.refresh(savedHeaderEntity);

        // 4. job_res_process_tree 저장 (JSON 변환)
        saveJobResProcessTree(savedHeaderEntity, JsonUtil.mapToJson(bomTree));

        // 5. 전략 패턴 실행 (상세 코드는 몰라도 동적으로 처리)
        Map<ProcessType, BomNode> bomNodes = resolveStartBomNodes(bomTree);
        strategy.start(savedHeaderEntity, flow, bomNodes, user);
    }

    @Override
    public void consumeMaterials(BomProcessContext ctx) {

        // Context에서 재료 꺼내기
        List<Map<String, Object>> bomMatItems = ctx.bomList();
        JobRes jr = ctx.jobRes();
        Float goodQty = ctx.goodQty();
        List<MaterialProduce> mpEntityList = ctx.mpEntityList();
        User user = ctx.user();
        String spjangcd = ctx.spjangcd();

        Timestamp now = DateUtil.getNowTimeStamp();

        LocalDate date = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        LocalTime time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss");

        for (int i = 0; i < bomMatItems.size(); i++) {
            Map<String, Object> bomMap = bomMatItems.get(i);

            float chasuBomQty = Float.parseFloat(bomMap.get("chasu_bom_qty").toString());
            Float bom_ratio   = bomMap.get("bom_ratio") == null ? null : Float.parseFloat(bomMap.get("bom_ratio").toString());
            if(bom_ratio == null) throw new CustomException("bom 수량이 존재하지 않습니다.");

            int consumeMatPk = (int) bomMap.get("mat_pk");
            String matName = bomMap.get("mat_name").toString();
            Material consMat = this.materialRepository.getMaterialById(consumeMatPk);
            String lotUseYn = bomMap.get("lotUseYn").toString();
            float totalQty = 0f;


            if ("Y".equals(lotUseYn)) { //lot 관리를 할 경우
                // 수정시작
                // 1. mat_proc_input 에서 해당 품목의 로트리스트를 가져온다.

                List<Map<String, Object>> mpiList = this.getMaterialProcessInputList(jr.getId(), consumeMatPk);
                // 투입요청에서 해당 품목이 로트 투입인지 조회한다

                float remainQty = chasuBomQty; //남은 투입량

                /*for (int j = 0; j < mpiList.size(); j++) {
                    Map<String, Object> mpiMap = mpiList.get(j);

                    float reqQty = Float.parseFloat(mpiMap.get("req_qty").toString());
                    totalQty += reqQty;

                    int matLotId = (int) mpiMap.get("ml_id");
                    float currentStock = Float.parseFloat(mpiMap.get("curr_qty").toString());
                    if (currentStock == 0) { //TODO: 수량이 부족한데 예외 안터지고 그냥 반복문만 빠져나감... 뭐지?
                        continue;
                    }

                    MatLotCons mlc = new MatLotCons();
                    mlc.setMaterialLotId(matLotId);
                    mlc.setOutputDateTime(now);
                    mlc.setSourceDataPk(mp.getId());
                    mlc.setSourceTableName("mat_produce");
                    mlc.set_audit(user);
                    mlc.setCurrentStock(ml.getCurrentStock()); // 당시 재고량
                    mlc.setSpjangcd(spjangcd);
                    if (currentStock >= remainQty) {
                        // 해당로트의현재수량 가능
                        mlc.setOutputQty(reqQty);
                        remainQty = (float) 0;
                        mlc = this.matLotConsRepository.save(mlc);

                        break;
                    } else {
                        mlc.setOutputQty(reqQty);
                        mlc = this.matLotConsRepository.save(mlc);
                        remainQty = remainQty - reqQty;
                    }

                }*/

//                if (remainQty > 0) {
//                    result.message = "로트 수량이 부족합니다.(" + matName + ")";
//                    result.success = false;
//                    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
//                    return result;
//                }
            } else {
                if ("1".equals(consMat.getUseyn())) {
                    throw new CustomException("사용 불가능한 품목이 BOM에 등록되어 있습니다.(" + matName + ")");
                }
                // mtyn이 0일 때는 재고 체크하지 않음
                if ("0".equals(consMat.getMtyn())) {
                    // 아무 동작 안함.
                } else {
                    Float currentStock = consMat.getCurrentStock();
                    if (currentStock == null || currentStock == 0f) {
                        throw new CustomException("가용한 품목 재고가 없습니다.(" + matName + ")");
                    } else if (currentStock < goodQty) {
                        throw new CustomException("가용한 품목 재고가 부족합니다. \n(" +
                                matName + ", 필요 수량: " + goodQty + ", 가용 수량: " + currentStock + ")");
                    }
                }
                totalQty += chasuBomQty; //TODO 아래보니깐 그냥 chasuBomQty가 맞는듯함.
            }

            for(int j=0; j<mpEntityList.size(); j++){
                // mat_cons 생성
                MaterialConsume mc = new MaterialConsume();
                mc.setJobResponseId(jr.getId());
                mc.setMaterialId(consumeMatPk);
                mc.setProcessOrder(1);
                mc.setLotIndex(mpEntityList.get(j).getLotIndex());
                mc.setStartTime(now);
                mc.setEndTime(now);
                mc.setDescription("차수생산분");
                mc.setBomQty(chasuBomQty); //TODO

                Float goodQty1 = mpEntityList.get(j).getGoodQty();

                mc.setConsumedQty(goodQty1 * bom_ratio);        // 차수 생산분에 해당하는 BOM기준물량, lot 마다 투입 수량  //TODO
                mc.set_audit(user);
                mc.setState("finished");
                mc.set_status("a");
                mc.setStoreHouseId(consMat.getStoreHouseId());
                mc.setSpjangcd(spjangcd);
                mc = this.matConsuRepository.save(mc);

                //1. mat_inout 생성=> lot 투입이면 투입 수량만큼 lot 없으면 BOM 수량만큼 재고를 차감한다.
                MaterialInout mic = new MaterialInout();
                mic.setMaterialInoutHeadId(null);
                mic.setMaterialId(mc.getMaterialId());
                mic.setStoreHouseId(consMat.getStoreHouseId());
                mic.setLotNumber(mpEntityList.get(j).getLotNumber());
                mic.setInoutDate(LocalDate.parse(date.format(dateFormat)));
                mic.setInoutTime(LocalTime.parse(time.format(timeFormat)));
                mic.setInOut("out");
                mic.setOutputType("consumed_out");
                mic.setOutputQty(mc.getConsumedQty());
                mic.setSourceDataPk(mc.getId());
                mic.setSourceTableName("mat_consu");
                mic.setState("confirmed");
                mic.set_status("a");
                mic.setDescription("차수생산 투입재고 차감");
                mic.set_audit(user);
                mic.setSpjangcd(spjangcd);

                this.matInoutRepository.save(mic);
            }


        } // bom List 반목문 끝, for문 끝
    }

    @Override
    public void createNextProcess(BomProcessContext context) {

        JobRes jr = context.jobRes();
        User user = context.user();

        //자체재고 생산이면 createNextProcess 안함.
        if("suju".equals(jr.getSourceTableName())
                && jr.getSourceDataPk() != null){
            NextContext nextProcess = processTreeCalculateService.createNextProcess(jr, user);

            if(nextProcess != null && nextProcess.getNextNode().matPk != null){
                saveNextOfProcess(jr, nextProcess.getNextNode(), user);
            }
        }
    }

    private void saveNextOfProcess(JobRes previous, BomNode node, User user) {

        Optional<JobRes> jr = jobResRepository.findTopByWorkOrderNumberAndParentIdIsNotNullOrderByWorkIndexDesc(previous.getWorkOrderNumber());

        if(!jr.isPresent()) throw new CustomException("작업지시를 찾을 수 없습니다.");

        int workIndex = jr.get().getWorkIndex() + 1;

        List<JobRes> nextProcesses =
                jobResRepository.findByWorkOrderNumberAndWorkIndex(
                        jr.get().getWorkOrderNumber(),
                        workIndex
                );

        if (nextProcesses.size() > 1) {
            throw new CustomException("작업순서가 이상합니다.");
        }

        if (nextProcesses.size() == 1) {
            // 이미 다음 공정 존재 → 더 만들 필요 없음
            return;
        }

        JobRes next = new JobRes();
        next.set_audit(user); //공통
        next.setProductionPlanDate(previous.getProductionPlanDate());
        next.setProductionDate(previous.getProductionDate());
        next.setShiftCode(previous.getShiftCode());
        next.setWorkIndex(workIndex);
        next.setOrderQty(node.calculatedBomRatio.floatValue());
        next.setState("ordered");
        next.setParentId(previous.getParentId());
        next.setProcessCount(previous.getProcessCount()+1);
        next.setSourceDataPk(previous.getSourceDataPk());
        next.setSourceTableName(previous.getSourceTableName());
        next.setFirstWorkCenter_id(previous.getFirstWorkCenter_id());

        next.setMaterialId(node.matPk);

        next.setStoreHouse_id(node.storeHouseId);

        next.setWorkCenter_id(previous.getWorkCenter_id());
        next.setSpjangcd(previous.getSpjangcd());

        jobResRepository.save(next);

        //: 이전공정이 물고있는 parentId 수정해줘여함
        previous.setParentId(next.getId());
        jobResRepository.save(previous);

    }

    private List<Map<String, Object>> getMaterialProcessInputList(int jrPk, int matPk) {

        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("jrPk", jrPk);
        param.addValue("matPk", matPk);

        String sql = """
                select  mpi.id  as mpi_id
                	  ,	mpi."RequestQty" as req_qty
                	  , mpi."InputQty" as input_qty
                	  , mpi."Material_id" as mat_pk
                	  , ml."CurrentStock" as curr_qty
                	  , ml.id as ml_id
                	  , ml."LotNumber"
                	  , ml."EffectiveDate" as eff_date
                from job_res jr
                inner join mat_proc_input mpi on mpi."MaterialProcessInputRequest_id"  = jr."MaterialProcessInputRequest_id"
                inner join mat_lot ml on ml.id = mpi."MaterialLot_id"
                where jr.id = :jrPk
                and mpi."Material_id" = :matPk
                order by ml."EffectiveDate"
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

        return items;
    }


    private void saveJobResProcessTree(JobRes jr, String bomTree) {

        JobResProcessTree jpt = new JobResProcessTree();

        jpt.setWorkOrderNo(jr.getWorkOrderNumber());
        jpt.setProcessTree(bomTree);

        jobResProcessTreeRepository.save(jpt);
    }


    /**
     * 첫 작업지시 생성을 위한 시작 BomNode 목록 반환
     * - 1·2차 메인라인 → 1개
     * - 3차 공정 포함 시 → 병렬로 추가
     */
    private Map<ProcessType, BomNode> resolveStartBomNodes(
            Map<String, BomNode> bomTree
    ) {
        if (bomTree == null || bomTree.isEmpty()) {
            throw new IllegalArgumentException("BOM tree is empty");
        }

        Map<ProcessType, BomNode> result = new EnumMap<>(ProcessType.class);

        // 1️⃣ 메인 라인 (1·2차)
        BomNode main = bomTree.get(ProcessType.SIMPLE_FLOW.name());
        if (main != null) {
            result.put(ProcessType.SIMPLE_FLOW, main);
        }

        // 2️⃣ 3차 병렬 공정
        BomNode third = bomTree.get(ProcessType.FULL_FLOW.name());
        if (third != null) {
            result.put(ProcessType.FULL_FLOW, third);
        }

        // 3️⃣ fallback: 키가 달라도 하나뿐인 경우
        if (result.isEmpty() && bomTree.size() == 1) {
            BomNode only = bomTree.values().iterator().next();
            result.put(ProcessType.SIMPLE_FLOW, only); // 의미상 메인으로 간주
        }

        if (result.isEmpty()) {
            throw new IllegalStateException("Cannot resolve start BomNodes from bomTree");
        }

        return result;
    }

}
