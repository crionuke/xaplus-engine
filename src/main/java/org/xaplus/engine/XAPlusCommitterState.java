package org.xaplus.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class XAPlusCommitterState {
    private final Map<XAPlusXid, XAPlusTransaction> transactions;
    private final Map<XAPlusXid, XAPlusXid> branchToTransactionXids;
    private final Map<XAPlusXid, Set<XAPlusXid>> waiting;

    XAPlusCommitterState() {
        transactions = new HashMap<>();
        branchToTransactionXids = new HashMap<>();
        waiting = new HashMap<>();
    }

    boolean track(XAPlusTransaction transaction) {
        XAPlusXid xid = transaction.getXid();
        if (transactions.put(xid, transaction) == null) {
            HashSet branches = new HashSet();
            transaction.getXaResources().forEach((x, r) -> {
                branches.add(x);
            });
            transaction.getXaPlusResources().forEach((x, r) -> {
                branches.add(x);
                branchToTransactionXids.put(x, xid);
            });
            waiting.put(xid, branches);
            return true;
        } else {
            return false;
        }
    }

    XAPlusTransaction getTransaction(XAPlusXid xid) {
        return transactions.get(xid);
    }

    XAPlusXid getTransactionXid(XAPlusXid branchXid) {
        return branchToTransactionXids.get(branchXid);
    }

    void setCommitted(XAPlusXid xid, XAPlusXid branchXid) {
        Set<XAPlusXid> remaining = waiting.get(xid);
        if (remaining != null) {
            remaining.remove(branchXid);
        }
    }

    void setDone(XAPlusXid branchXid) {
        XAPlusXid xid = branchToTransactionXids.get(branchXid);
        if (xid != null) {
            Set<XAPlusXid> remaining = waiting.get(xid);
            if (remaining != null) {
                remaining.remove(branchXid);
            }
        }
    }

    XAPlusTransaction remove(XAPlusXid xid) {
        XAPlusTransaction transaction = transactions.remove(xid);
        if (transaction != null) {
            transaction.getXaPlusResources().forEach((x, r) -> branchToTransactionXids.remove(x));
            waiting.remove(xid);
            return transaction;
        } else {
            return null;
        }
    }

    Boolean check(XAPlusXid xid) {
        if (transactions.containsKey(xid) && waiting.containsKey(xid)) {
            return waiting.get(xid).isEmpty();
        } else {
            return false;
        }
    }
}
