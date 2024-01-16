package org.hswebframework.ezorm.rdb.supports.clickhouse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hswebframework.web.dict.EnumDict;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;

/**
 * @author dengpengyu
 * @date 2023/10/12 15:16
 */
@AllArgsConstructor
@Getter
public enum ClickhouseDataType implements EnumDict<Class<?>> {

    INT8(Byte.class),
    INT16(Short.class),
    INT32(Integer.class),
    INT64(Long.class),
    INT128(BigInteger.class),
    INT256(BigInteger.class),
    UINT8(Short.class),
    UINT16(Integer.class),
    UINT32(Long.class),
    UINT64(BigInteger.class),
    NULLABLE_UINT64(BigInteger.class),
    FLOAT32(Float.class),
    FLOAT64(Double.class),
    STRING(String.class),
    UUID(String.class),
    BOOLEAN(Boolean.class),
    DATE(java.sql.Date.class),
    DATETIME(java.sql.Timestamp.class),
    DATETIME64(java.time.LocalDateTime.class),
    ARRAY(Array.class), // This would be a placeholder, actual implementation depends on the array type
    ENUM(String.class), // Enums in Clickhouse are typically represented as strings in Java
    DECIMAL(BigDecimal.class),
    IP(String.class); // IP address types can be represented as strings

    private final Class<?> javaType;

    @Override
    public Class<?> getValue() {
        return javaType;
    }

    @Override
    public String getText() {
        return name();
    }
}
