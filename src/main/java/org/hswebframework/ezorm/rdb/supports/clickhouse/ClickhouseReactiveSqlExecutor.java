package org.hswebframework.ezorm.rdb.supports.clickhouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.hswebframework.ezorm.rdb.executor.BatchSqlRequest;
import org.hswebframework.ezorm.rdb.executor.DefaultColumnWrapperContext;
import org.hswebframework.ezorm.rdb.executor.SqlRequest;
import org.hswebframework.ezorm.rdb.executor.reactive.ReactiveSqlExecutor;
import org.hswebframework.ezorm.rdb.executor.wrapper.ResultWrapper;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * @className ClickhouseRestfulSqlExecutor
 * @Description TODO
 * @Author dengpengyu
 * @Date 2023/9/4 14:40
 * @Vesion 1.0
 */
public class ClickhouseReactiveSqlExecutor implements ReactiveSqlExecutor {
    private Logger log = LoggerFactory.getLogger(ClickhouseReactiveSqlExecutor.class);
    private WebClient client;

    public ClickhouseReactiveSqlExecutor(WebClient client) {
        this.client = client;
    }

    @Override
    public Mono<Integer> update(Publisher<SqlRequest> request) {
        return this
                .doExecute(request)
                .then(Mono.just(1));
    }

    @Override
    public Mono<Void> execute(Publisher<SqlRequest> request) {
        return this
                .doExecute(request)
                .then();
    }

    @Override
    public <E> Flux<E> select(Publisher<SqlRequest> request, ResultWrapper<E, ?> wrapper) {

        return this
                .doExecute(request)
                .flatMap(response -> convertQueryResult(response, wrapper));
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
                    log.trace("Execute ==> {}", sql);
                    return client
                            .post()
                            .bodyValue(sql)
                            .exchangeToMono(response -> response
                                    .bodyToMono(String.class)
                                    .map(json -> {
                                        JSONObject result = JSON.parseObject(json);
                                        checkExecuteResult(sql, json);

                                        return result;
                                    }));
                });
    }

    private void checkExecuteResult(String sql, String code) {
        if (code.startsWith("Code")) {
            throw new RuntimeException(code);
        }
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
    }
}
