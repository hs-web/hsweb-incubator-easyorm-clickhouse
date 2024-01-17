package org.hswebframework.ezorm.rdb.supports.clickhouse;

import lombok.SneakyThrows;
import org.hswebframework.ezorm.rdb.executor.reactive.ReactiveSqlExecutor;
import org.hswebframework.ezorm.rdb.executor.reactive.ReactiveSyncSqlExecutor;
import org.hswebframework.ezorm.rdb.mapping.EntityColumnMapping;
import org.hswebframework.ezorm.rdb.mapping.MappingFeatureType;
import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.ezorm.rdb.mapping.defaults.DefaultReactiveRepository;
import org.hswebframework.ezorm.rdb.mapping.jpa.JpaEntityTableMetadataParser;
import org.hswebframework.ezorm.rdb.mapping.wrapper.EntityResultWrapper;
import org.hswebframework.ezorm.rdb.metadata.RDBDatabaseMetadata;
import org.hswebframework.ezorm.rdb.metadata.RDBSchemaMetadata;
import org.hswebframework.ezorm.rdb.metadata.RDBTableMetadata;
import org.hswebframework.ezorm.rdb.metadata.dialect.Dialect;
import org.hswebframework.ezorm.rdb.operator.DatabaseOperator;
import org.hswebframework.ezorm.rdb.operator.DefaultDatabaseOperator;
import org.hswebframework.web.crud.configuration.ClickhouseProperties;

import java.util.function.Supplier;

/**
 * @author dengpengyu
 * @date 2023/9/21 14:20
 */
public interface Helper {

    RDBSchemaMetadata getRDBSchemaMetadata();

    Dialect getDialect();

    ReactiveSqlExecutor getReactiveSqlExecutor();

    ClickhouseProperties getClickhouseProperties();

    default RDBDatabaseMetadata getRDBDatabaseMetadata() {
        RDBDatabaseMetadata metadata = new RDBDatabaseMetadata(getDialect());

        RDBSchemaMetadata schema = getRDBSchemaMetadata();

        ReactiveSqlExecutor sqlExecutor = getReactiveSqlExecutor();

        metadata.setCurrentSchema(schema);
        metadata.addSchema(schema);
        metadata.addFeature(sqlExecutor);
        metadata.addFeature(ClickhouseSyncSqlExecutor.of(getClickhouseProperties()));

        return metadata;
    }

    @SneakyThrows
    default <T, K> ReactiveRepository<T, K> createRepository(Class<T> clazz) {
        RDBDatabaseMetadata metadata = getRDBDatabaseMetadata();
        DatabaseOperator operator = DefaultDatabaseOperator.of(metadata);

        JpaEntityTableMetadataParser parser = new JpaEntityTableMetadataParser();
        parser.setDatabaseMetadata(metadata);
      /*  parser.parseTableMetadata(clazz)
                .ifPresent(address -> {
                    operator.ddl()
                            .createOrAlter(address)
                            .commit()
                            .reactive()
                            .block();
                });*/

        RDBTableMetadata table = parser
                .parseTableMetadata(clazz)
                .orElseThrow(NullPointerException::new);

//        operator.ddl()
//                .createOrAlter(table)
//                .commit()
//                .reactive()
//                .block();

        Supplier supplier = new Supplier() {
            @SneakyThrows
            @Override
            public Object get() {
                return clazz.newInstance();
            }
        };
        EntityResultWrapper<T> wrapper = new EntityResultWrapper(supplier);
        wrapper.setMapping(table
                .<EntityColumnMapping>getFeature(MappingFeatureType.columnPropertyMapping.createFeatureId(clazz))
                .orElseThrow(NullPointerException::new));


        return new DefaultReactiveRepository<>(operator, table, clazz, wrapper);
    }

}
