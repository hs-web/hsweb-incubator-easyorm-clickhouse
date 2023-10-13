package org.hswebframework.ezorm.rdb.supports.clickhouse;

import org.hswebframework.ezorm.core.utils.StringUtils;
import org.hswebframework.ezorm.rdb.metadata.DataType;
import org.hswebframework.ezorm.rdb.metadata.JdbcDataType;
import org.hswebframework.ezorm.rdb.metadata.dialect.DefaultDialect;

import java.sql.Date;
import java.sql.JDBCType;
import java.time.LocalDate;
import java.util.UUID;

/**
 * @className ClickhouseDire
 * @Description TODO
 * @Author dengpengyu
 * @Date 2023/9/4 14:53
 * @Vesion 1.0
 */
public class ClickhouseDialect extends DefaultDialect {

    public ClickhouseDialect() {
        addDataTypeBuilder(JDBCType.TINYINT, (meta) -> ClickhouseDataType.INT8);
        addDataTypeBuilder(JDBCType.BOOLEAN, (meta) -> ClickhouseDataType.INT8);
        addDataTypeBuilder(JDBCType.SMALLINT, (meta) -> ClickhouseDataType.INT16);
        addDataTypeBuilder(JDBCType.INTEGER, (meta) -> ClickhouseDataType.INT32);
        addDataTypeBuilder(JDBCType.BIGINT, (meta) -> ClickhouseDataType.INT64);
        addDataTypeBuilder(JDBCType.VARCHAR, (meta) -> ClickhouseDataType.STRING);
        registerDataType("uuid", JdbcDataType.of(JDBCType.VARCHAR, String.class));
        registerDataType("timestamp", JdbcDataType.of(JDBCType.BIGINT, Long.class));
    }

    @Override
    public String getQuoteStart() {
        return "`";
    }

    @Override
    public String getQuoteEnd() {
        return "`";
    }

    @Override
    public boolean isColumnToUpperCase() {
        return false;
    }

    @Override
    public String getId() {
        return "clickhouse";
    }

    @Override
    public String getName() {
        return "Clickhouse";
    }
}
