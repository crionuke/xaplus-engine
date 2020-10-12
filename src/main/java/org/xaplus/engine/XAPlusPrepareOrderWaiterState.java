package org.xaplus.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class XAPlusPrepareOrderWaiterState {
    private final Map<XAPlusXid, XAPlusTransaction> transactions;
    private final Set<XAPlusXid> prepareOrders;

    XAPlusPrepareOrderWaiterState() {
        transactions = new HashMap<>();
        prepareOrders = new HashSet<>();
    }

    boolean track(XAPlusTransaction transaction) {
        return transactions.put(transaction.getXid(), transaction) == null;
    }

    void addPrepareOrder(XAPlusXid xid) {
        prepareOrders.add(xid);
    }

    XAPlusTransaction getTransaction(XAPlusXid xid) {
        return transactions.get(xid);
    }

    XAPlusTransaction remove(XAPlusXid xid) {
        XAPlusTransaction transaction = transactions.remove(xid);
        prepareOrders.remove(xid);
        return transaction;
    }

    Boolean check(XAPlusXid xid) {
        return prepareOrders.contains(xid) && transactions.containsKey(xid);
    }
}
