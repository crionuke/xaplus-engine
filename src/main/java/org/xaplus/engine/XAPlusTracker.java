package org.xaplus.engine;

import java.util.HashMap;
import java.util.Map;

class XAPlusTracker {

    final Map<XAPlusXid, XAPlusTransaction> transactions;

    XAPlusTracker() {
        transactions = new HashMap<>();
    }

    boolean track(XAPlusTransaction transaction) {
        XAPlusXid xid = transaction.getXid();
        return transactions.put(xid, transaction) == null;
    }

    XAPlusTransaction getTransaction(XAPlusXid xid) {
        return transactions.get(xid);
    }

    XAPlusTransaction remove(XAPlusXid xid) {
        return transactions.remove(xid);
    }
}
