package org.xaplus.engine;

import java.util.*;

final class XAPlusTimerState {
    private final Map<XAPlusXid, XAPlusTransaction> transactions;

    XAPlusTimerState() {
        transactions = new HashMap<>();
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
}
