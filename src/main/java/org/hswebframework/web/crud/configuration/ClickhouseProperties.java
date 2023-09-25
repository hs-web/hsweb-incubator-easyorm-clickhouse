package org.hswebframework.web.crud.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author dengpengyu
 * @date 2023/9/13 15:29
 */
@Data
@ConfigurationProperties(prefix = "spring.clickhouse")
@Component
public class
ClickhouseProperties {

    private String url;

    private String database;

    private String username;

    private String password;
}
