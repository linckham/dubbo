package com.alibaba.dubbo.registry.configserver;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.dubbo.rpc.RpcException;
import com.cmbc.configserver.client.ConfigClient;
import com.cmbc.configserver.client.ResourceListener;
import com.cmbc.configserver.client.impl.ConfigClientImpl;
import com.cmbc.configserver.domain.Configuration;
import com.cmbc.configserver.remoting.ConnectionStateListener;
import com.cmbc.configserver.remoting.netty.NettyClientConfig;
import com.cmbc.configserver.utils.PathUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * the registry that implements by Config Server
 * Created by tongchuan.lin<linckham@gmail.com><br/>
 *
 * @Date 2014/11/10
 * @Time 10:08
 */
public class ConfigServerRegistry extends FailbackRegistry {
    private final ConfigClient configClient;
    private final static String DEFAULT_CELL = "dubbo";
    private final static String DEFAULT_ROOT = DEFAULT_CELL;
    private final ConcurrentMap<URL, ConcurrentHashMap<NotifyListener, ResourceListener>> resourcesListenerMap = new ConcurrentHashMap<URL, ConcurrentHashMap<NotifyListener, ResourceListener>>();
    private final String root;

    public ConfigServerRegistry(URL url) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        this.root = group;

        List<String> configServerAddress = new ArrayList<String>();
        //TODO: support multi host:port
        configServerAddress.add(url.getHost() + ":" + url.getPort());
        configClient = new ConfigClientImpl(new NettyClientConfig(), configServerAddress, new ConnectionStateListener() {
            @Override
            public void reconnected() {
            }
        });

    }

    @Override
    protected void doRegister(URL url) {
        try {
            Configuration configuration = url2Configuration(url);
            if (null != configuration) {
                configClient.publish(configuration);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to config server " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doUnregister(URL url) {
        try {
            Configuration configuration = url2Configuration(url);
            if (null != configuration) {
                configClient.publish(configuration);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to config server " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            if (null == url || null == listener) {
                return;
            }
            List<URL> urls = new ArrayList<URL>();

            for (String path : toCategoryPath(url)) {
                Configuration configuration = PathUtils.path2Configuration(path);
                if (null != configuration) {
                    ConcurrentHashMap<NotifyListener, ResourceListener> notifyListeners = this.resourcesListenerMap.get(url);
                    if (null == notifyListeners) {
                        resourcesListenerMap.putIfAbsent(url, new ConcurrentHashMap<NotifyListener, ResourceListener>());
                        notifyListeners = this.resourcesListenerMap.get(url);
                    }

                    ResourceListener resourceListener = notifyListeners.get(listener);
                    if (null == resourceListener) {
                        notifyListeners.putIfAbsent(listener, new ResourceListener() {
                            @Override
                            public void notify(List<Configuration> configs) {
                                List<URL> urls = configuration2URL(configs);
                                ConfigServerRegistry.this.notify(url, listener, urls);
                            }
                        });
                        resourceListener = notifyListeners.get(listener);
                    }
                    List<Configuration> configs = this.configClient.subscribe(configuration, resourceListener);
                    List<URL> tmpUrls = configuration2URL(configs);
                    if (null != tmpUrls && !tmpUrls.isEmpty()) {
                        urls.addAll(tmpUrls);
                    }
                }
            }

            notify(url, listener, urls);

        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private String[] toCategoryPath(URL url) {
        String[] categories;
        if (Constants.ANY_VALUE.equals(url.getParameter(Constants.CATEGORY_KEY))) {
            categories = new String[]{Constants.PROVIDERS_CATEGORY, Constants.CONSUMERS_CATEGORY,
                    Constants.ROUTERS_CATEGORY, Constants.CONFIGURATORS_CATEGORY};
        } else {
            categories = url.getParameter(Constants.CATEGORY_KEY, new String[]{Constants.DEFAULT_CATEGORY});
        }
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            paths[i] = toServicePath(url) + Constants.PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        if (Constants.ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        return toRootDir() + URL.encode(name);
    }

    private String toRootPath() {
        return root;
    }

    private String toRootDir() {
        if (root.equals(Constants.PATH_SEPARATOR)) {
            return root;
        }
        return root + Constants.PATH_SEPARATOR;
    }

    @Override
    protected void doUnsubscribe(URL url, NotifyListener listener) {
        try {
            if (null == url || null == listener) {
                return;
            }

            Configuration configuration = url2Configuration(url);
            if (null == configuration) {
                return;
            }

            ConcurrentHashMap<NotifyListener, ResourceListener> notifyListeners = this.resourcesListenerMap.get(url);
            if (null != notifyListeners) {
                ResourceListener resourceListener = notifyListeners.get(listener);
                if (null != resourceListener) {
                    this.configClient.unsubscribe(configuration, resourceListener);
                }
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to un subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isAvailable() {
        return this.configClient != null && this.configClient.isAvailable();
    }

    public void destroy() {
        super.destroy();
        try {
            configClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close config server client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private Configuration url2Configuration(URL url) {
        if (null == url) return null;
        Configuration config = new Configuration();
        config.setCell(url.getParameter(Constants.GROUP_KEY, DEFAULT_CELL));
        config.setResource(url.getParameter(Constants.INTERFACE_KEY, "*"));
        config.setType(url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY));
        config.setContent(URL.encode(url.toFullString()));
        return config;
    }

    private List<URL> configuration2URL(List<Configuration> configs) {
        List<URL> urls = new ArrayList<URL>();
        if (null != configs) {
            for (Configuration config : configs) {
                try {
                    URL url = configuration2URL(config);
                    urls.add(url);
                } catch (Exception e) {
                    logger.warn("Failed to convert config to URL" + config + ", cause: " + e.getMessage(), e);
                }
            }
        }
        return urls;
    }


    private URL configuration2URL(Configuration configuration) throws Exception {
        String url = URL.decode(configuration.getContent());
        if (url.contains("://")) {
            return URL.valueOf(url);
        }
        return null;
    }
}
