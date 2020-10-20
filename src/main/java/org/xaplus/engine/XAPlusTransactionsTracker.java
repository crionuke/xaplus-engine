package org.xaplus.engine;

import java.util.HashMap;
import java.util.Map;

class XAPlusTransactionsTracker {

    final Map<XAPlusXid, XAPlusTransaction> transactions;

    XAPlusTransactionsTracker() {
        transactions = new HashMap<>();
    }

    boolean track(XAPlusTransaction transaction) {
        XAPlusXid xid = transaction.getXid();
        return transactions.put(xid, transaction) == null;
    }

    boolean contains(XAPlusXid xid) {
        return transactions.containsKey(xid);
    }

    XAPlusTransaction transaction(XAPlusXid xid) {
        return transactions.get(xid);
    }

    XAPlusTransaction remove(XAPlusXid xid) {
        return transactions.remove(xid);
    }
}
