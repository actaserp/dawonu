package mes.app.util;

import javax.persistence.AttributeConverter;

public class FloatToDoubleConverter implements AttributeConverter<Float, Double> {

    @Override
    public Double convertToDatabaseColumn(Float attribute) {
        if (attribute == null) return null;
        return Math.round(attribute * 1000) / 1000.0; // 반올림 후 double로 변환
    }

    @Override
    public Float convertToEntityAttribute(Double dbData) {
        return dbData == null ? null : dbData.floatValue();
    }
}
