package org.xaplus.engine;

import java.util.*;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusTimerTracker {

    private int recoveryTimeoutInSeconds;
    private long recoveryStartTime;
    private Map<XAPlusXid, XAPlusTransaction> transactions;

    XAPlusTimerTracker(int recoveryTimeoutInSeconds) {
        this.recoveryTimeoutInSeconds = recoveryTimeoutInSeconds;
        this.recoveryStartTime = 0;
        this.transactions = new HashMap<>();
    }

    boolean track(XAPlusTransaction transaction) {
        return transactions.put(transaction.getXid(), transaction) == null;
    }

    boolean remove(XAPlusXid xid) {
        return transactions.remove(xid) != null;
    }

    List<XAPlusTransaction> removeExpiredTransactions() {
        List<XAPlusTransaction> expired = new ArrayList<>();
        long time = System.currentTimeMillis();
        Iterator<Map.Entry<XAPlusXid, XAPlusTransaction>> iterator = transactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<XAPlusXid, XAPlusTransaction> entry = iterator.next();
            XAPlusTransaction transaction = entry.getValue();
            if (time >= transaction.getExpireTimeInMillis()) {
                expired.add(transaction);
                iterator.remove();
            }
        }
        return expired;
    }

    void recoveryStarted() {
        recoveryStartTime = System.currentTimeMillis();
    }

    boolean isRecoveryTimedOut() {
        if (recoveryStartTime > 0) {
            if (System.currentTimeMillis() - recoveryStartTime > recoveryTimeoutInSeconds * 1000) {
                return true;
            }
        }
        return false;
    }

    void resetRecoveryTracker() {
        recoveryStartTime = 0;
    }
}
