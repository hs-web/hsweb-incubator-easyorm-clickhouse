package org.hswebframework.ezorm.rdb.supports.clickhouse;


import org.hswebframework.ezorm.rdb.executor.reactive.ReactiveSqlExecutor;

import org.hswebframework.ezorm.rdb.metadata.RDBSchemaMetadata;
import org.hswebframework.ezorm.rdb.metadata.dialect.Dialect;
import org.hswebframework.web.crud.configuration.ClickhouseProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;


/**
 * @author dengpengyu
 * @date 2023/9/21 14:28
 */
@Configuration
@EnableConfigurationProperties(ClickhouseProperties.class)
public class ClickhouseHelper implements Helper {

    @Autowired
    ClickhouseProperties properties;

    @Override
    public RDBSchemaMetadata getRDBSchemaMetadata() {
        return new ClickhouseSchemaMetadata(properties.getDatabase());
    }

    @Override
    public Dialect getDialect() {
        return new ClickhouseDialect();
    }

    @Override
    public ReactiveSqlExecutor getReactiveSqlExecutor() {
        WebClient clickhouseWebClient = WebClient
                .builder()
                .baseUrl(properties.getUrl())
                .defaultHeader("X-ClickHouse-User", properties.getUsername())
                .defaultHeader("X-ClickHouse-Key", properties.getPassword())
                .build();

        return new ClickhouseReactiveSqlExecutor(clickhouseWebClient);
    }

    @Override
    public ClickhouseProperties getClickhouseProperties() {
        return properties;
    }
}
