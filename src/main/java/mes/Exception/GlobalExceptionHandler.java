package mes.Exception;

import mes.domain.model.AjaxResult;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CustomException.class)
    public AjaxResult CustomExceptionHandler(CustomException e){
        AjaxResult r= new AjaxResult();
        r.success = false;
        r.message = e.getMessage();
        return r;
    }
}
