# 说明

1. 本仓库需要配合[hsweb-easy-orm](https://github.com/hs-web/hsweb-easy-orm)，[hsweb-framework](https://github.com/hs-web/hsweb-framework)使用
2. 本仓库是针对hsweb-easy-orm 4.X、hsweb-commons-curd 4.X 适配 clickhouse22.X

# 使用

## hsweb-easy-orm编译

1. 首先克隆[hsweb-easy-orm](https://github.com/hs-web/hsweb-easy-orm)至本地

2. hsweb-easy-orm的pom文件中新增依赖

   ```xml
   <dependency>
       <groupId>org.springframework</groupId>
       <artifactId>spring-webflux</artifactId>
       <version>5.3.25</version>
   </dependency>
   ```

3. 将本项目中hsweb-easy-orm的clickhouse目录复制到hsweb-easy-orm的org.hswebframework.ezorm.rdb.supports下，复制后包路径应是org.hswebframework.ezorm.rdb.supports.clickhouse

4. hsweb-easy-orm-rdb\src\main\java\org\hswebframework\ezorm\rdb\metadata\dialect\Dialect.java中新增数据库方言如下

   ```java
       Dialect CLICKHOUSE =new ClickhouseDialect();
   ```

5. maven编译

## hsweb-commons-curd编译

1. 首先克隆[hsweb-framework](https://github.com/hs-web/hsweb-framework)至本地

2. 将本项目中hsweb-commons-curd文件夹下ClickhouseHttpSqlExecutorConfiguration.java 、ClickhouseProperties.java放置在hsweb-framework/hsweb-commons-curd模块中org.hswebframework.web.crud.configuration路径下。

3. 在org.hswebframework.web.crud.configuration.EasyormProperties.java文件中插入相关代码如下，**注意注释部分**

   ```java
   
       @Getter
       @AllArgsConstructor
       public enum DialectEnum {
           mysql(Dialect.MYSQL, "?") {
               @Override
               public RDBSchemaMetadata createSchema(String name) {
                   return new MysqlSchemaMetadata(name);
               }
           },
           mssql(Dialect.MSSQL, "@arg") {
               @Override
               public RDBSchemaMetadata createSchema(String name) {
                   return new SqlServerSchemaMetadata(name);
               }
           },
           oracle(Dialect.ORACLE, "?") {
               @Override
               public RDBSchemaMetadata createSchema(String name) {
                   return new OracleSchemaMetadata(name);
               }
           },
           postgres(Dialect.POSTGRES, "$") {
               @Override
               public RDBSchemaMetadata createSchema(String name) {
                   return new PostgresqlSchemaMetadata(name);
               }
           },
           h2(Dialect.H2, "$") {
               @Override
               public RDBSchemaMetadata createSchema(String name) {
                   return new H2SchemaMetadata(name);
               }
           }
           //start
           //这里是对clickhouse的支持
           ,
           clickhouse(Dialect.CLICKHOUSE,"?"){
               @Override
               public RDBSchemaMetadata createSchema(String name) {
                   return new ClickhouseSchemaMetadata(name);
               }
           }
           //end
           ;
   
           private Dialect dialect;
           private String bindSymbol;
   
           public abstract RDBSchemaMetadata createSchema(String name);
   ```

4. hsweb-framework/hsweb-commons-curd模块的src/main/resources/META-INF/spring.factories中新增org.hswebframework.web.crud.configuration.ClickhouseHttpSqlExecutorConfiguration，示例如下

   ```
   # Auto Configure
   org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
   org.hswebframework.web.crud.configuration.EasyormConfiguration,\
   org.hswebframework.web.crud.configuration.JdbcSqlExecutorConfiguration,\
   org.hswebframework.web.crud.configuration.R2dbcSqlExecutorConfiguration,\
   org.hswebframework.web.crud.configuration.ClickhouseHttpSqlExecutorConfiguration,\
   org.hswebframework.web.crud.web.CommonWebFluxConfiguration,\
   org.hswebframework.web.crud.web.CommonWebMvcConfiguration
   ```

5. maven编译

# 注意

1. 先编译hsweb-easy-orm再编译hsweb-commons-curd
2. 编译hsweb-commons-curd时需注意依赖hsweb-easy-orm的版本。
3. 由于clickhouse各版本系统表差异过大，本仓库暂只支持clickhouse22.X版本