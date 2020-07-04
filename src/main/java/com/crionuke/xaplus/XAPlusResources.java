package com.crionuke.xaplus;

import com.crionuke.xaplus.exceptions.XAPlusSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Component
class XAPlusResources {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusEngine.class);

    // Key is uniqueName, value is wrapper for XADataSource, XAConnectionFactory or XAPlusFactory resources
    private final Map<String, Wrapper> resources;

    XAPlusResources() {
        resources = new ConcurrentHashMap<>();
    }

    Map<String, Wrapper> getResources() {
        return Collections.unmodifiableMap(resources);
    }

    Wrapper get(String uniqueName) {
        return resources.get(uniqueName);
    }

    XAPlusResource getXAPlusResource(String uniqueName) throws XAPlusSystemException {
        try {
            XAPlusFactoryWrapper wrapper = (XAPlusFactoryWrapper) resources.get(uniqueName);
            if (wrapper != null) {
                return wrapper.get();
            } else {
                throw new XAPlusSystemException(new IllegalArgumentException("resource " + uniqueName + " not found"));
            }
        } catch (ClassCastException | XAPlusException e) {
            throw new XAPlusSystemException(e);
        }
    }

    boolean isXAPlusResource(String uniqueName) {
        Wrapper wrapper = resources.get(uniqueName);
        return wrapper != null && wrapper instanceof XAPlusFactoryWrapper;
    }

    void register(XADataSource dataSource, String uniqueName) {
        if (dataSource == null) {
            throw new NullPointerException("dataSource in null");
        }
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName in null");
        }
        register(new XADataSourceWrapper(dataSource), uniqueName);
    }

    void register(XAConnectionFactory factory, String uniqueName) {
        if (factory == null) {
            throw new NullPointerException("factory in null");
        }
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName in null");
        }
        register(new XAConnectionFactoryWrapper(factory), uniqueName);
    }

    void register(XAPlusFactory factory, String serverId) {
        if (factory == null) {
            throw new NullPointerException("factory in null");
        }
        if (serverId == null) {
            throw new NullPointerException("serverId in null");
        }
        register(new XAPlusFactoryWrapper(factory), serverId);
    }

    private void register(Wrapper xaWrapper, String uniqueName) {
        if (resources.containsKey(uniqueName)) {
            throw new IllegalStateException("resource with name " + uniqueName + " already registered");
        }
        resources.put(uniqueName, xaWrapper);
        if (logger.isTraceEnabled()) {
            logger.trace("Resource={} registered with uniqueName={}", xaWrapper, uniqueName);
        }
    }

    abstract class Wrapper<T, R> {

        protected final T resource;

        Wrapper(T resource) {
            this.resource = resource;
        }

        abstract R get() throws Exception;
    }

    class XADataSourceWrapper extends Wrapper<XADataSource, XAConnection> {

        XADataSourceWrapper(XADataSource xaDataSource) {
            super(xaDataSource);
        }

        @Override
        XAConnection get() throws SQLException {
            return resource.getXAConnection();
        }
    }

    class XAConnectionFactoryWrapper extends Wrapper<XAConnectionFactory, javax.jms.XAConnection> {

        XAConnectionFactoryWrapper(XAConnectionFactory xaConnectionFactory) {
            super(xaConnectionFactory);
        }

        @Override
        javax.jms.XAConnection get() throws JMSException {
            return resource.createXAConnection();
        }
    }

    class XAPlusFactoryWrapper extends Wrapper<XAPlusFactory, XAPlusResource> {

        XAPlusFactoryWrapper(XAPlusFactory xaPlusFactory) {
            super(xaPlusFactory);
        }

        @Override
        XAPlusResource get() throws XAPlusException {
            return resource.createXAPlusResource();
        }
    }
}
