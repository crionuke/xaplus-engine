package org.xaplus.engine;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusProperties {

    private final String serverId;
    private final int queueSize;
    private final int defaultTimeoutInSeconds;

    XAPlusProperties(String serverId, int queueSize, int defaultTimeoutInSeconds) {
        this.serverId = serverId;
        this.queueSize = queueSize;
        this.defaultTimeoutInSeconds = defaultTimeoutInSeconds;
    }

    String getServerId() {
        return serverId;
    }

    int getQueueSize() {
        return queueSize;
    }

    int getDefaultTimeoutInSeconds() {
        return defaultTimeoutInSeconds;
    }
}
