package org.xaplus.engine;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusProperties {

    private final static int MAX_SERVER_ID_LENGTH = 52;

    private final String serverId;
    private final int queueSize;
    private final int transactionsTimeoutInSeconds;
    private final int recoveryTimeoutInSeconds;
    private final int recoveryPeriodInSeconds;

    XAPlusProperties(String serverId, int queueSize, int transactionsTimeoutInSeconds, int recoveryTimeoutInSeconds,
                     int recoveryPeriodInSeconds) {
        List<String> messages = new ArrayList<>();
        if (serverId == null) {
            messages.add("serverId is null");
        } else if (serverId.length() > MAX_SERVER_ID_LENGTH) {
            messages.add("too long serverId, limited by " + MAX_SERVER_ID_LENGTH + " bytes, serverId=" + serverId);
        }
        if (queueSize <= 0) {
            messages.add("queueSize must be greater zero, queueSize=" + queueSize);
        }
        if (transactionsTimeoutInSeconds <= 0) {
            messages.add("transaction timeout must be greater zero, transactionsTimeoutInSeconds=" +
                    transactionsTimeoutInSeconds);
        }
        if (recoveryTimeoutInSeconds <= 0) {
            messages.add("recovery timeout must be greater zero, recoveryTimeoutInSeconds=" + recoveryTimeoutInSeconds);
        }
        if (recoveryPeriodInSeconds < 0) {
            messages.add("recovery period must be greater or equal to zero to disable, " +
                    "recoveryPeriodInSeconds=" + recoveryPeriodInSeconds);
        }
        if (messages.size() > 0) {
            // Fire all problems by on exceptions
            throw new IllegalArgumentException(String.join("; ", messages));
        }

        this.serverId = serverId;
        this.queueSize = queueSize;
        this.transactionsTimeoutInSeconds = transactionsTimeoutInSeconds;
        this.recoveryTimeoutInSeconds = recoveryTimeoutInSeconds;
        this.recoveryPeriodInSeconds = recoveryPeriodInSeconds;
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

    int getRecoveryPeriodInSeconds() {
        return recoveryPeriodInSeconds;
    }
}
