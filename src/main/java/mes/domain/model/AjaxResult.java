package mes.domain.model;

public class AjaxResult {
	
	public AjaxResult() {
		
	}
    public boolean success = true;
	public String message = "";
	public Object data = null;	
	public String code="";
	public String StateName= "";

    public static AjaxResult success(String message, Object data){
        AjaxResult result = new AjaxResult();
        result.success = true;
        result.message = message;
        result.data = data;
        return result;
    }


}
