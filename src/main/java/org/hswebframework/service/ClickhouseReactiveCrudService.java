package org.hswebframework.service;


import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.ezorm.rdb.supports.clickhouse.ClickhouseHelper;
import org.hswebframework.web.crud.service.ReactiveCrudService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author dengpengyu
 * @date 2023/9/20 11:38
 */
public abstract class ClickhouseReactiveCrudService<E, K> implements ReactiveCrudService<E, K> {

    private final Class<E> clazz;

    private ReactiveRepository<E, K> repository;

    @Autowired
    private ClickhouseHelper clickhouseHelper;

    public ClickhouseReactiveCrudService(Class<E> clazz) {
        this.clazz = clazz;
    }

    @Override
    public ReactiveRepository<E, K> getRepository() {
        if (repository == null) {
            repository = clickhouseHelper.createRepository(clazz);
        }
        return repository;
    }

//    public abstract Class<E> getEntityClass();
}
