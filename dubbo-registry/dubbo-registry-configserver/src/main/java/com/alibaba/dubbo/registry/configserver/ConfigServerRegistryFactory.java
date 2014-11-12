package com.alibaba.dubbo.registry.configserver;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;

/**
 * the config server registry factory
 * Created by tongchuan.lin<linckham@gmail.com><br/>
 *
 * @Date 2014/11/10
 * @Time 10:06
 */
public class ConfigServerRegistryFactory extends AbstractRegistryFactory {
    @Override
    protected Registry createRegistry(URL url){
        return new ConfigServerRegistry(url);
    }
}
