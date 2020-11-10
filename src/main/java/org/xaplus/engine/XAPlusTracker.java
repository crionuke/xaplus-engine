package org.xaplus.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class XAPlusTracker {

    private final Map<XAPlusXid, XAPlusTransaction> transactions;

    XAPlusTracker() {
        transactions = new HashMap<>();
    }

    boolean track(XAPlusTransaction transaction) {
        XAPlusXid xid = transaction.getXid();
        return transactions.put(xid, transaction) == null;
    }

    boolean contains(XAPlusXid xid) {
        return transactions.containsKey(xid);
    }

    XAPlusTransaction getTransaction(XAPlusXid xid) {
        return transactions.get(xid);
    }

    XAPlusXid getTransactionXid(XAPlusXid branchXid) {
        for (XAPlusTransaction transaction : transactions.values()) {
            if (transaction.contains(branchXid)) {
                return transaction.getXid();
            }
        }
        return null;
    }

    XAPlusTransaction remove(XAPlusXid xid) {
        return transactions.remove(xid);
    }
}
