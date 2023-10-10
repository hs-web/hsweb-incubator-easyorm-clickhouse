package org.hswebframework.web.crud.configuration;

import org.hswebframework.ezorm.rdb.mapping.defaults.DefaultReactiveRepository;
import org.hswebframework.utils.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * @className EasyormConfigurationBeanDefinitionRegistryPostProcessor
 * @Description TODO
 * @Author zhong
 * @Date 2023/10/8 14:07
 * @Vesion 1.0
 */
@Component
public class EasyormConfigurationBeanDefinitionRegistryPostProcessor implements BeanDefinitionRegistryPostProcessor, ApplicationContextAware {
    private ApplicationContext applicationContext;
    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        Environment environment =applicationContext.getEnvironment();
        if (StringUtils.isNullOrEmpty(environment.getProperty("easyorm.dialect"))){
            String[] beanNames = ((DefaultListableBeanFactory) registry).getBeanNamesForType(DefaultReactiveRepository.class);
            Arrays.stream(beanNames).forEach(item-> registry.removeBeanDefinition(item));
            registry.removeBeanDefinition("org.hswebframework.web.crud.configuration.AutoDDLProcessor_1");
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext=applicationContext;
    }
}
