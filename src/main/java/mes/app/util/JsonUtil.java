package mes.app.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import mes.Exception.CustomException;
import mes.app.production.production_package.BomNode;
import mes.config.ObjectMapperHolder;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.AssociationOverride;
import java.util.Map;

@Slf4j
public class JsonUtil {


    private JsonUtil(){}

    /**
     * Map -> Json 결과물을 보기 좋게 print 찍기
     * **/
    public static String mapToJson(Map<String, ?> item){

        try{

            ObjectMapper mapper = ObjectMapperHolder.mapper;

            mapper.enable(SerializationFeature.INDENT_OUTPUT);

            String json = mapper.writeValueAsString(item);
            //log.info(json);
            return json;

        }catch(Exception e){
            log.error("json으로 변환하지 못함.");
            return null;
        }
    }

    /***
     *  String -> JsonNode
     * **/
    public static JsonNode StringToJson(String tree){

        try{
            ObjectMapper mapper = ObjectMapperHolder.mapper;

            return  mapper.readTree(tree);
        }catch(Exception e){
            throw new CustomException("문자열에서 Json 구조로 변환하지 못했습니다.");
        }
    }

    /***
     * String -> BomNode
     * 주의 : 문자열 자체가 평문이 아니고 Map 형태여야 파싱가능
     * **/
    public static Map<String, BomNode> parseProcessTree(String json){
        try{
            ObjectMapper mapper = ObjectMapperHolder.mapper;

            return mapper.readValue(json, new TypeReference<Map<String, BomNode>>() {

            });
        }catch(Exception e){
            throw new CustomException("파싱 실패");
        }
    }

    /***
     * BomNode -> String
     * **/
    public static String bomNodeToJson(BomNode node){
        ObjectMapper mapper = ObjectMapperHolder.mapper;

        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new CustomException("BomNode -> Json 변환 실패");
        }
    }
}
