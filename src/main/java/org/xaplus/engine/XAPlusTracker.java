package org.xaplus.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class XAPlusTracker {

    private final Map<XAPlusXid, XAPlusTransaction> transactions;
    private final Map<XAPlusXid, XAPlusXid> branchToTransactionXid;
    private final Set<XAPlusXid> orders;

    XAPlusTracker() {
        transactions = new HashMap<>();
        branchToTransactionXid = new HashMap<>();
        orders = new HashSet<>();
    }

    boolean track(XAPlusTransaction transaction) {
        XAPlusXid xid = transaction.getXid();
        if (transactions.put(xid, transaction) == null) {
            transaction.getAllXids()
                    .forEach((branchXid) -> branchToTransactionXid.put(branchXid, xid));
            return true;
        } else {
            return false;
        }
    }

    void addOrder(XAPlusXid xid) {
        orders.add(xid);
    }

    boolean contains(XAPlusXid xid) {
        return transactions.containsKey(xid);
    }

    boolean hasOrder(XAPlusXid xid) {
        return orders.contains(xid);
    }

    XAPlusTransaction getTransaction(XAPlusXid xid) {
        return transactions.get(xid);
    }

    XAPlusXid getTransactionXid(XAPlusXid branchXid) {
        return branchToTransactionXid.get(branchXid);
    }

    XAPlusTransaction remove(XAPlusXid xid) {
        XAPlusTransaction transaction = transactions.remove(xid);
        if (transaction != null) {
            transaction.getAllXids()
                    .forEach((branchXid) -> branchToTransactionXid.remove(branchXid, xid));
        }
        orders.remove(xid);
        return transaction;
    }
}
