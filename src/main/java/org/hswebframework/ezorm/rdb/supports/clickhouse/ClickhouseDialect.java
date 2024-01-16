package org.hswebframework.ezorm.rdb.supports.clickhouse;

import org.hswebframework.ezorm.rdb.metadata.JdbcDataType;
import org.hswebframework.ezorm.rdb.metadata.dialect.DefaultDialect;

import java.sql.JDBCType;

/**
 * @className ClickhouseDire
 * @Description TODO
 * @Author dengpengyu
 * @Date 2023/9/4 14:53
 * @Vesion 1.0
 */
public class ClickhouseDialect extends DefaultDialect {

    public ClickhouseDialect() {
        addDataTypeBuilder(JDBCType.TINYINT, (meta) -> ClickhouseDataType.INT8.getText());
        addDataTypeBuilder(JDBCType.BOOLEAN, (meta) -> ClickhouseDataType.INT8.getText());
        addDataTypeBuilder(JDBCType.SMALLINT, (meta) -> ClickhouseDataType.INT16.getText());
        addDataTypeBuilder(JDBCType.INTEGER, (meta) -> ClickhouseDataType.INT32.getText());
        addDataTypeBuilder(JDBCType.BIGINT, (meta) -> ClickhouseDataType.INT64.getText());
        addDataTypeBuilder(JDBCType.VARCHAR, (meta) -> ClickhouseDataType.STRING.getText());
        registerDataType("uuid", JdbcDataType.of(JDBCType.VARCHAR, String.class));
        registerDataType("timestamp", JdbcDataType.of(JDBCType.BIGINT, Long.class));
        registerDataType("date", JdbcDataType.of(JDBCType.VARCHAR, String.class));
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
