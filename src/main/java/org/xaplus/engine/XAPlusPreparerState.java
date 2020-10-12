package org.xaplus.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class XAPlusPreparerState {
    private final Map<XAPlusXid, XAPlusTransaction> transactions;
    private final Map<XAPlusXid, XAPlusXid> branchToTransactionXids;
    private final Map<XAPlusXid, Set<XAPlusXid>> preparing;
    private final Map<XAPlusXid, Set<XAPlusXid>> waiting;

    XAPlusPreparerState() {
        transactions = new HashMap<>();
        branchToTransactionXids = new HashMap<>();
        preparing = new HashMap<>();
        waiting = new HashMap<>();
    }

    boolean track(XAPlusTransaction transaction) {
        XAPlusXid xid = transaction.getXid();
        if (transactions.put(xid, transaction) == null) {
            HashSet preparingBranches = new HashSet();
            HashSet waitingBranches = new HashSet();
            transaction.getXaResources().forEach((x, r) -> {
                preparingBranches.add(x);
            });
            transaction.getXaPlusResources().forEach((x, r) -> {
                waitingBranches.add(x);
                branchToTransactionXids.put(x, xid);
            });
            preparing.put(xid, preparingBranches);
            waiting.put(xid, waitingBranches);
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

    void setPrepared(XAPlusXid xid, XAPlusXid branchXid) {
        Set<XAPlusXid> remaining = preparing.get(xid);
        if (remaining != null) {
            remaining.remove(branchXid);
        }
    }

    void setReady(XAPlusXid branchXid) {
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
            preparing.remove(xid);
            waiting.remove(xid);
            return transaction;
        } else {
            return null;
        }
    }

    Boolean check(XAPlusXid xid) {
        if (transactions.containsKey(xid) && preparing.containsKey(xid) && waiting.containsKey(xid)) {
            return preparing.get(xid).isEmpty() && waiting.get(xid).isEmpty();
        } else {
            return false;
        }
    }
}
