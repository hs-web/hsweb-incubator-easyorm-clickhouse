package org.hswebframework.ezorm.rdb.supports.clickhouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.hswebframework.ezorm.core.meta.Feature;
import org.hswebframework.ezorm.rdb.executor.BatchSqlRequest;
import org.hswebframework.ezorm.rdb.executor.DefaultColumnWrapperContext;
import org.hswebframework.ezorm.rdb.executor.SqlRequest;
import org.hswebframework.ezorm.rdb.executor.SyncSqlExecutor;
import org.hswebframework.ezorm.rdb.executor.wrapper.ResultWrapper;
import org.hswebframework.web.crud.configuration.ClickhouseProperties;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @className ClickhouseReactiveSqlExecutor
 * @Description TODO
 * @Author zhong
 * @Date 2024/1/16 14:55
 * @Vesion 1.0
 */
@AllArgsConstructor(staticName = "of")
public class ClickhouseSyncSqlExecutor implements SyncSqlExecutor {
    private Logger log = LoggerFactory.getLogger(ClickhouseSyncSqlExecutor.class);

    private  RestTemplate restTemplate;

    private  String url;

    private  HttpHeaders headers;

    public ClickhouseSyncSqlExecutor(ClickhouseProperties clickhouseProperties) {
        restTemplate =new RestTemplate();
        this.url=clickhouseProperties.getUrl();
        headers = new HttpHeaders();
        headers.add("X-ClickHouse-User", clickhouseProperties.getUsername());
        headers.add("X-ClickHouse-Key", clickhouseProperties.getPassword());
    }

    public static Feature of(ClickhouseProperties clickhouseProperties) {
       return new ClickhouseSyncSqlExecutor(clickhouseProperties);
    }



    @Override
    @SneakyThrows
    public int update(SqlRequest request) {
        return   this
                .doExecute(Mono.just(request))
                .then(Mono.just(1)).toFuture().get(30, TimeUnit.SECONDS);

    }

    @Override
    @SneakyThrows
    public void execute(SqlRequest request) {

         this
                .doExecute(Mono.just(request))
                .then()
                .toFuture().get(30,TimeUnit.SECONDS);

    }

    @Override
    @SneakyThrows
    public <T, R> R select(SqlRequest request, ResultWrapper<T, R> wrapper) {
         this
                .doExecute(Mono.just(request))
                .flatMap(response -> convertQueryResult(response, wrapper))
         .collectList().toFuture().get(30,TimeUnit.SECONDS);

        return wrapper.getResult();

    }


    private Flux<JSONObject> doExecute(Publisher<SqlRequest> requests) {
        return Flux
                .from(requests)
                .expand(request -> {
                    if (request instanceof BatchSqlRequest) {
                        return Flux.fromIterable(((BatchSqlRequest) request).getBatch());
                    }
                    return Flux.empty();
                })

                .filter(SqlRequest::isNotEmpty)
                .concatMap(request -> {
                    String sql;
                    if (request.toNativeSql().toUpperCase().startsWith("INSERT")
                            || request.toNativeSql().toUpperCase().startsWith("ALTER")) {
                        sql = request.toNativeSql();
                    } else {
                        sql = request.toNativeSql() + " FORMAT JSON";
                    }
                    log.info("Execute ==> {}", sql);
                    HttpEntity requestEntity = new HttpEntity<>(sql, headers);
                    ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);
                    JSONObject result = JSON.parseObject(responseEntity.getBody());

                    return Mono.just(result);
                })
              ;
    }

    protected <E> Flux<E> convertQueryResult(JSONObject result, ResultWrapper<E, ?> wrapper) {

        Map<String, ClickhouseDataType> columnMeta = new HashMap<String, ClickhouseDataType>();

        JSONArray resultMeta = result.getJSONArray("meta");
        JSONArray resultEntityList = result.getJSONArray("data");

        if (CollectionUtils.isEmpty(resultMeta) || CollectionUtils.isEmpty(resultEntityList)) {
            return Flux.empty();
        }
        //把当前列和列类型一一对应
        for (Object o : resultMeta) {
            JSONObject e = (JSONObject) o;
            columnMeta.put(e.getString("name"), ClickhouseDataType.valueOf(e.getString("type").toUpperCase().replace("NULLABLE(", "").replace(")", "")));
        }
        //所有的列名
        ArrayList<String> columns = new ArrayList<>(columnMeta.keySet());

        return Flux.create(sink -> {
            wrapper.beforeWrap(() -> columns);

            for (Object oneObjectEntity : resultEntityList) {
                E rowInstance = wrapper.newRowInstance();
                JSONObject oneJsonObjectEntity = (JSONObject) oneObjectEntity;
                for (String columnName : columns) {
                    Object value = oneJsonObjectEntity.get(columnName);
                    ClickhouseDataType dataType = columnMeta.get(columnName);
                    try {
                        Class<?> typeClass = dataType.getJavaType();
                        Constructor<?> constructor = typeClass.getConstructor(String.class);

                        if (Objects.nonNull(value)) {
                            value = constructor.newInstance(value.toString());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    DefaultColumnWrapperContext<E> context = new DefaultColumnWrapperContext<>(columns.indexOf(columnName), columnName, value, rowInstance);
                    wrapper.wrapColumn(context);
                    rowInstance = context.getRowInstance();
                }
                if (!wrapper.completedWrapRow(rowInstance)) {
                    break;
                }
                if (rowInstance != null) {
                    sink.next(rowInstance);
                }
            }
            wrapper.completedWrap();
            sink.complete();
        });
    }}

