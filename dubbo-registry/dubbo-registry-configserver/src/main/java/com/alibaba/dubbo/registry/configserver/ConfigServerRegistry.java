package com.alibaba.dubbo.registry.configserver;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.dubbo.rpc.RpcException;
import com.cmbc.configserver.client.ConfigClient;
import com.cmbc.configserver.client.ResourceListener;
import com.cmbc.configserver.client.impl.ConfigClientImpl;
import com.cmbc.configserver.domain.Configuration;
import com.cmbc.configserver.remoting.ConnectionStateListener;
import com.cmbc.configserver.remoting.netty.NettyClientConfig;
import com.cmbc.configserver.utils.ConfigUtils;
import com.cmbc.configserver.utils.PathUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
    private final Set<String> anyServices = new ConcurrentHashSet<String>();

    public ConfigServerRegistry(URL url) {
        super(url);
        List<String> configServerAddress = new ArrayList<String>();
        // URL format specification
        //1.If URL format is configserver://0.0.0.0,it means that we will mark the server address with the configuration address file;
        //2.If URL format is configserver://127.0.0.1:19999,it means that we will mark the server address with the host and post of the URL;
        //3.If URL format is configserver://127.0.0.1:19999?backup=127.0.0.1:20000,127.0.0.1:200001, it means that we will use multi host port to mark server address
        if (url.isAnyHost()) {
            logger.info(String.format("config server address list is reading from the specified configuration file %s .", ConfigUtils.getProperty(com.cmbc.configserver.utils.Constants.CONFIG_SERVER_ADDRESS_FILE_NAME_KEY, com.cmbc.configserver.utils.Constants.DEFAULT_CONFIG_SERVER_ADDRESS_FILE_NAME)));
        } else {
            logger.info(String.format("config server address list is reading from the specified URL %s .", url));
            configServerAddress.add(url.getHost() + ":" + url.getPort());
            //support multi host:port
            String backupURL = url.getParameter(Constants.BACKUP_KEY);
            if (StringUtils.isNotBlank(backupURL)) {
                String[] addressArray = backupURL.split(Constants.COMMA_SEPARATOR);
                configServerAddress.addAll(Arrays.asList(addressArray));
            }
        }

        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        this.root = group;
        configClient = new ConfigClientImpl(new NettyClientConfig(), configServerAddress, new ConnectionStateListener() {
            public void reconnected() {
                try{
                    recover();
                }
                catch (Exception e){
                    logger.error(e.getMessage(),e);
                }
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
            //administrator subscribe all the service of the specified group
            //admin url admin://192.168.0.24?interface=*&category=providers,consumers,routers,configurators&check=false&classifier=*&enabled=*&group=*&version=*
            if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
                String root = toRootPath();
                ConcurrentHashMap<NotifyListener, ResourceListener> notifyListeners = this.resourcesListenerMap.get(url);
                if (null == notifyListeners) {
                    this.resourcesListenerMap.putIfAbsent(url, new ConcurrentHashMap<NotifyListener, ResourceListener>());
                    notifyListeners = this.resourcesListenerMap.get(url);
                }

                ResourceListener resourceListener = notifyListeners.get(listener);
                if (null == resourceListener) {
                    notifyListeners.putIfAbsent(listener, new ResourceListener() {
                        public void notify(List<Configuration> configs) {
                            for (Configuration config : configs) {
                                String resource = URL.decode(config.getResource());
                                //check whether new resource is adding to the specified root
                                if (!anyServices.contains(resource)) {
                                    anyServices.add(resource);
                                    subscribe(url.setPath(resource)
                                                    .addParameters(Constants.INTERFACE_KEY, resource,Constants.CHECK_KEY, String.valueOf(false)),
                                            listener);
                                }
                            }
                        }
                    });
                    resourceListener = notifyListeners.get(listener);
                }

                Configuration rootConfiguration = PathUtils.path2Configuration(root);
                //get all the resources of the root configuration
                List<Configuration> configs = this.configClient.subscribe(rootConfiguration, resourceListener);
                if (null != configs && !configs.isEmpty()) {
                    for (Configuration config : configs) {
                        String resource = URL.decode(config.getResource());
                        if (!anyServices.contains(resource)) {
                            anyServices.add(resource);
                            subscribe(url.setPath(resource)
                                            .addParameters(Constants.INTERFACE_KEY, resource,Constants.CHECK_KEY, String.valueOf(false)),
                                    listener);
                        }
                    }
                }
            } else {
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
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to config server " + getUrl() + ", cause: " + e.getMessage(), e);
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
            throw new RpcException("Failed to un subscribe " + url + " to config server " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

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
        config.setResource(url.getParameter(Constants.INTERFACE_KEY, Constants.ANY_VALUE));
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


    private URL configuration2URL(Configuration configuration) {
        String url = URL.decode(configuration.getContent());
        if (url.contains("://")) {
            return URL.valueOf(url);
        }
        return null;
    }
}
