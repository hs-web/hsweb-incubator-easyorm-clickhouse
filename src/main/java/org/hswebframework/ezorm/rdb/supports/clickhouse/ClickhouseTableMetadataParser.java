package org.hswebframework.ezorm.rdb.supports.clickhouse;

import org.hswebframework.ezorm.rdb.executor.reactive.ReactiveSqlExecutor;
import org.hswebframework.ezorm.rdb.mapping.defaults.record.RecordResultWrapper;
import org.hswebframework.ezorm.rdb.metadata.RDBColumnMetadata;
import org.hswebframework.ezorm.rdb.metadata.RDBIndexMetadata;
import org.hswebframework.ezorm.rdb.metadata.RDBSchemaMetadata;
import org.hswebframework.ezorm.rdb.metadata.RDBTableMetadata;
import org.hswebframework.ezorm.rdb.metadata.parser.IndexMetadataParser;
import org.hswebframework.ezorm.rdb.supports.commons.RDBTableMetadataParser;
import org.hswebframework.utils.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hswebframework.ezorm.rdb.executor.SqlRequests.template;
import static org.hswebframework.ezorm.rdb.executor.wrapper.ResultWrappers.singleMap;

/**
 * @className Clickhouse
 * @Description TODO
 * @Author dengpengyu
 * @Date 2023/9/4 14:56
 * @Vesion 1.0
 */
public class ClickhouseTableMetadataParser extends RDBTableMetadataParser {
    private static final String TABLE_META_SQL = String.join(" ",
            "select",
            "column_name as `name`,",
            "data_type as `data_type`,",
            "character_maximum_length as `data_length`,",
            "numeric_precision as `data_precision`,",
            "numeric_scale as `data_scale`,",
            "column_comment as `comment`,",
            "table_name as `table_name`,",
            "case is_nullable when 0 then 0 else 1 end ",
            "from information_schema.columns where table_schema=#{schema} and table_name like #{table}");

    private static final String TABLE_COMMENT_SQL = String.join(" ",
            "select ",
            "`comment`",
            ",name as `table_name`",
            "from system.tables where database=#{schema} and name like #{table}");


    private static final String ALL_TABLE_SQL = "select table_name as `name` from system.tables where database=#{schema}";

    private static final String TABLE_EXISTS_SQL = "select count() as `total` from system.tables where database=#{schema} and name=#{table}";

    public ClickhouseTableMetadataParser(RDBSchemaMetadata schema) {
        super(schema);
    }

    @Override
    protected String getTableMetaSql(String name) {
        return TABLE_META_SQL;
    }

    @Override
    protected String getTableCommentSql(String name) {
        return TABLE_COMMENT_SQL;
    }

    @Override
    protected String getAllTableSql() {
        return ALL_TABLE_SQL;
    }

    @Override
    public String getTableExistsSql() {
        return TABLE_EXISTS_SQL;
    }

    @Override
    public List<RDBTableMetadata> parseAll() {
        return super.fastParseAll();
    }

    @Override
    public Flux<RDBTableMetadata> parseAllReactive() {
        return super.fastParseAllReactive();
    }
    @Override
    public Mono<RDBTableMetadata> parseByNameReactive(String name) {
        return tableExistsReactive(name)
                .filter(Boolean::booleanValue)
                .flatMap(ignore -> {
                    RDBTableMetadata metaData = createTable(name);
                    metaData.setName(name);
                    metaData.setAlias(name);
                    Map<String, Object> param = new HashMap<>();
                    param.put("table", name);
                    param.put("schema", schema.getName());
                    ReactiveSqlExecutor reactiveSqlExecutor = getReactiveSqlExecutor();
                    //列
                    Mono<List<RDBColumnMetadata>> columns = reactiveSqlExecutor
                            .select(template(getTableMetaSql(null), param), new RecordResultWrapper())
                            .map(record -> {
                                RDBColumnMetadata column = metaData.newColumn();
                                applyColumnInfo(column, record);
                                if (!StringUtils.isNullOrEmpty(column.getAlias())){
                                    column.setAlias(getAlias(column.getName()));
                                }
                                metaData.addColumn(column);
                                return column;
                            })
                            .collectList();
                    //注释
                    Mono<Map<String, Object>> comments = reactiveSqlExecutor
                            .select(template(getTableCommentSql(name), param), singleMap())
                            .doOnNext(comment -> metaData.setComment(String.valueOf(comment.get("comment"))))
                            .singleOrEmpty();

                    //加载索引
                    Flux<RDBIndexMetadata> index = schema.findFeature(IndexMetadataParser.ID)
                            .map(parser -> parser.parseTableIndexReactive(name))
                            .orElseGet(Flux::empty)
                            .doOnNext(metaData::addIndex);

                    return Flux
                            .merge(columns, comments, index)
                            .then(Mono.just(metaData));
                })
                ;
    }
    private String getAlias(String name) {//处理驼峰命名
        StringBuilder result = new StringBuilder();
        boolean nextUpperCase = false;
        for (int i = 0; i < name.length(); i++) {
            char currentChar = name.charAt(i);
            if (currentChar == '_') {
                nextUpperCase = true;
            } else {
                if (nextUpperCase) {
                    result.append(Character.toUpperCase(currentChar));
                    nextUpperCase = false;
                } else {
                    result.append(Character.toLowerCase(currentChar));
                }
            }
        }
        return result.toString();
    }
}
