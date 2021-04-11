package org.xaplus.engine;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusProperties {

    private final String serverId;
    private final int queueSize;
    private final int transactionsTimeoutInSeconds;
    private final int recoveryTimeoutInSeconds;

    XAPlusProperties(String serverId, int queueSize, int transactionsTimeoutInSeconds, int recoveryTimeoutInSeconds) {
        this.serverId = serverId;
        this.queueSize = queueSize;
        this.transactionsTimeoutInSeconds = transactionsTimeoutInSeconds;
        this.recoveryTimeoutInSeconds = recoveryTimeoutInSeconds;
    }

    String getServerId() {
        return serverId;
    }

    int getQueueSize() {
        return queueSize;
    }

    int getTransactionsTimeoutInSeconds() {
        return transactionsTimeoutInSeconds;
    }

    int getRecoveryTimeoutInSeconds() {
        return recoveryTimeoutInSeconds;
    }
}
