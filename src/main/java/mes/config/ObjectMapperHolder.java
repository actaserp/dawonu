package mes.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ObjectMapperHolder {

    public static ObjectMapper mapper;

    @Autowired
    public ObjectMapperHolder(ObjectMapper objectMapper) {
        mapper = objectMapper;
    }

}
