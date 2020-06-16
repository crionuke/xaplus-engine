package com.crionuke.xaplus;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties
@ConfigurationProperties("xaplus")
class XAPlusProperties {

    volatile private String serverId;
    volatile private int queueSize;
    volatile private int defaultTimeoutInSeconds;
    volatile private String tlog;

    XAPlusProperties() {
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getDefaultTimeoutInSeconds() {
        return defaultTimeoutInSeconds;
    }

    public void setDefaultTimeoutInSeconds(int defaultTimeoutInSeconds) {
        this.defaultTimeoutInSeconds = defaultTimeoutInSeconds;
    }

    public String getTlog() {
        return tlog;
    }

    public void setTlog(String tlog) {
        this.tlog = tlog;
    }
}
