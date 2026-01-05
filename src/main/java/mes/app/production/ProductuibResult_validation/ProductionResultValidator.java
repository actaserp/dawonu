package mes.app.production.ProductuibResult_validation;

import mes.Exception.CustomException;
import mes.domain.entity.JobRes;
import mes.domain.entity.MaterialLot;
import mes.domain.model.AjaxResult;
import mes.domain.repository.JobResRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class ProductionResultValidator {

    @Autowired
    JobResRepository jobResRepository;

    /**공용**/
    //작업지시 있는지 없는지 찾는 메서드
    public void validateJobResExists(JobRes jr, String message) {
        if (jr == null) {
            throw new CustomException(message);
        }
    }

    /**공용**/
    //리스트에 해당하는 요소가 있는지 체크
    public void assertNotEmpty(List<?> items, String message){
        if(items.isEmpty()){
            throw new CustomException(message);
        }
    }

    /**공용**/
    //어떤것에 대하여 양수값(이미존재 하여서 List size가 양수) 인 경우 익셉션
    public void throwIfExists(Integer size, String message){
        if(size > 0){
            throw new CustomException(message);
        }
    }
    /**공용**/
    //시간역전 검증
    public void reverseTimeValidator(LocalDateTime endDt, LocalDateTime startDt, String message){
        if (endDt.isBefore(startDt)) {
            throw new CustomException(message);
        }
    }
    /**공용**/
    //해당 객체가 null인지 체크
    public void throwIfNull(Object o, String errMsg){
        if(o == null) throw new CustomException(errMsg);
    }

    //시간관점에서 타당성 체크
    public void workFinish_Exists_endDate(String endDate, String endTime, String message){
        if(endDate == null || endDate.isBlank() || endTime == null || endTime.isBlank()){
            throw new CustomException(message);
        }
    }



    //해당 lot가 가용한 재고가 있는지 , 품목 기본창고가 존재하는지
    public void addLotInput_validateByMatLot(MaterialLot ml, String message, String message2){
        if(ml.getCurrentStock() <= 0){
            throw new CustomException(message);
        }

        if(ml.getStoreHouseId() == null){
            throw new CustomException(message2);
        }
    }



}
