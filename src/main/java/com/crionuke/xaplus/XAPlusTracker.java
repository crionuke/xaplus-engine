package com.crionuke.xaplus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAResource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusTracker {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTracker.class);

    private final Map<XAPlusXid, Transaction> transactions;
    private final Map<XAPlusXid, XAPlusXid> branchToTransactionXids;
    private final Set<XAPlusXid> prepareOrders;
    private final Set<XAPlusXid> commitOrders;

    XAPlusTracker() {
        transactions = new HashMap<>();
        branchToTransactionXids = new HashMap<>();
        prepareOrders = new HashSet<>();
        commitOrders = new HashSet<>();
    }

    void track(XAPlusXid xid, Map<XAPlusXid, String> uniqueNames,
               Map<XAPlusXid, XAResource> xaResources, Map<XAPlusXid, XAPlusResource> xaPlusResources) {
        Transaction transaction = new Transaction(uniqueNames, xaResources, xaPlusResources);
        transactions.put(xid, transaction);
        xaResources.forEach((x, r) -> {
            branchToTransactionXids.put(x, xid);
        });
        xaPlusResources.forEach((x, r) -> {
            branchToTransactionXids.put(x, xid);
        });
    }

    Transaction getTransaction(XAPlusXid xid) {
        return transactions.get(xid);
    }

    Transaction remove(XAPlusXid xid) {
        Transaction transaction = transactions.remove(xid);
        if (transaction != null) {
            transaction.getEnlistedResources().forEach((x, r) -> branchToTransactionXids.remove(x));
        }
        prepareOrders.remove(xid);
        commitOrders.remove(xid);
        return transaction;
    }

    XAPlusXid getTransactionXid(XAPlusXid branchXid) {
        return branchToTransactionXids.get(branchXid);
    }

    void addPrepareOrder(XAPlusXid xid) {
        prepareOrders.add(xid);
    }

    void addCommitOrder(XAPlusXid xid) {
        commitOrders.add(xid);
    }

    boolean canSubordinatePrepare(XAPlusXid xid) {
        return prepareOrders.contains(xid) && transactions.containsKey(xid);
    }

    boolean canSubordinateCommit(XAPlusXid xid) {
        if (commitOrders.contains(xid)) {
            Transaction transaction = getTransaction(xid);
            if (transaction != null) {
                return transaction.isPrepared();
            }
        }
        return false;
    }

    Map<XAPlusXid, XAResource> getLoggedForCommit(String serverId) {
        Map<XAPlusXid, XAResource> result = new HashMap<>();
        for (Map.Entry<XAPlusXid, Transaction> entry : transactions.entrySet()) {
            Transaction transaction = entry.getValue();
            if (transaction.isLogged()) {
                result.putAll(transaction.getPreparedResourcesFor(serverId));
            }
        }
        return result;
    }

    Map<XAPlusXid, XAResource> getLoggedForRollback(String serverId) {
        Map<XAPlusXid, XAResource> result = new HashMap<>();
        for (Map.Entry<XAPlusXid, Transaction> entry : transactions.entrySet()) {
            Transaction transaction = entry.getValue();
            if (transaction.isLogged()) {
                result.putAll(transaction.getEnlistedResourcesFor(serverId));
            }
        }
        return result;
    }

    class Transaction {

        private final Map<XAPlusXid, Branch> enlisted;
        private final Map<XAPlusXid, Branch> prepared;
        private final Map<XAPlusXid, Branch> committed;
        private final Map<XAPlusXid, Branch> rolledBack;
        private final Set<XAPlusXid> preparing;
        private final Set<XAPlusXid> readying;
        private final Set<XAPlusXid> committing;
        private final Set<XAPlusXid> rollingBack;
        private final Set<XAPlusXid> doing;
        private volatile boolean logged;

        Transaction(Map<XAPlusXid, String> uniqueNames, Map<XAPlusXid, XAResource> xaResources,
                    Map<XAPlusXid, XAPlusResource> xaPlusResources) {
            enlisted = new HashMap<>();
            prepared = new HashMap<>();
            committed = new HashMap<>();
            rolledBack = new HashMap<>();
            preparing = new HashSet<>();
            readying = new HashSet<>();
            committing = new HashSet<>();
            rollingBack = new HashSet<>();
            doing = new HashSet<>();
            if (xaResources != null) {
                xaResources.forEach((x, r) -> {
                    if (uniqueNames.get(x) != null) {
                        enlisted.put(x, new Branch(uniqueNames.get(x), r));
                        preparing.add(x);
                        committing.add(x);
                        rollingBack.add(x);
                    } else {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Inconsistance uniqueNames and XA resources for branchXid={}", x);
                        }
                    }
                });
            }
            if (xaPlusResources != null) {
                xaPlusResources.forEach((x, r) -> {
                    if (uniqueNames.get(x) != null) {
                        enlisted.put(x, new Branch(uniqueNames.get(x), r));
                        readying.add(x);
                        doing.add(x);
                    } else {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Inconsistance uniqueNames and XA+ resources for branchXid={}", x);
                        }
                    }
                });
            }
            logged = false;
        }

        void setReady(XAPlusXid branchXid) {
            if (readying.remove(branchXid)) {
                Branch branch = enlisted.get(branchXid);
                if (branch != null) {
                    prepared.put(branchXid, branch);
                }
            }
        }

        void setReadOnly(XAPlusXid branchXid) {
            preparing.remove(branchXid);
        }

        void setCommitted(XAPlusXid branchXid) {
            if (committing.remove(branchXid)) {
                Branch branch = prepared.get(branchXid);
                if (branch != null) {
                    committed.put(branchXid, branch);
                }
            }
        }

        void setRolledBack(XAPlusXid branchXid) {
            if (rollingBack.remove(branchXid)) {
                Branch branch = enlisted.get(branchXid);
                if (branch != null) {
                    rolledBack.put(branchXid, branch);
                }
            }
        }

        boolean isPrepared() {
            return preparing.isEmpty();
        }

        void setPrepared(XAPlusXid branchXid) {
            if (preparing.remove(branchXid)) {
                Branch branch = enlisted.get(branchXid);
                if (branch != null) {
                    prepared.put(branchXid, branch);
                }
            }
        }

        boolean isOnePhaseDone() {
            return isPrepared() && readying.isEmpty();
        }

        boolean isTwoPhaseDone() {
            return preparing.isEmpty() && committing.isEmpty() && doing.isEmpty();
        }

        boolean isRollbackDone() {
            return rollingBack.isEmpty() && doing.isEmpty();
        }

        boolean isLogged() {
            return logged;
        }

        void setDone(XAPlusXid branchXid) {
            if (doing.remove(branchXid)) {
                Branch branch = prepared.get(branchXid);
                if (branch != null) {
                    committed.put(branchXid, branch);
                }
                branch = enlisted.get(branchXid);
                rolledBack.put(branchXid, branch);
            }
        }

        void setCommitAsFailed(XAPlusXid branchXid) {
            committing.remove(branchXid);
            doing.remove(branchXid);
        }

        void setRollbackAsFailed(XAPlusXid branchXid) {
            rollingBack.remove(branchXid);
            doing.remove(branchXid);
        }

        void setLogged() {
            logged = true;
        }

        Map<XAPlusXid, XAResource> getEnlistedResources() {
            return extractResources(enlisted);
        }

        Map<XAPlusXid, XAResource> getEnlistedResourcesFor(String serverId) {
            Map<XAPlusXid, XAResource> result = new HashMap<>();
            enlisted.forEach((x, b) -> {
                if (b.getUniqueName().equals(serverId)) {
                    result.put(x, b.getResource());
                }
            });
            return result;
        }

        Map<XAPlusXid, XAResource> getPreparedResources() {
            return extractResources(prepared);
        }

        Map<XAPlusXid, XAResource> getPreparedResourcesFor(String serverId) {
            Map<XAPlusXid, XAResource> result = new HashMap<>();
            prepared.forEach((x, b) -> {
                if (b.getUniqueName().equals(serverId)) {
                    result.put(x, b.getResource());
                }
            });
            return result;
        }

        Map<XAPlusXid, String> getEnlistedBranchesUniqueNames() {
            return extractUniqueNames(enlisted);
        }

        Map<XAPlusXid, String> getPreparedBranchesUniqueNames() {
            return extractUniqueNames(prepared);
        }

        Map<XAPlusXid, String> getCommittedBranchesUniqueNames() {
            return extractUniqueNames(committed);
        }

        Map<XAPlusXid, String> getRolledBackBranchesUniqueNames() {
            return extractUniqueNames(rolledBack);
        }

        private Map<XAPlusXid, XAResource> extractResources(Map<XAPlusXid, Branch> container) {
            Map<XAPlusXid, XAResource> result = new HashMap<>();
            container.forEach((x, b) -> {
                result.put(x, b.getResource());
            });
            return result;
        }

        private Map<XAPlusXid, String> extractUniqueNames(Map<XAPlusXid, Branch> container) {
            Map<XAPlusXid, String> result = new HashMap<>();
            container.forEach((x, b) -> {
                result.put(x, b.getUniqueName());
            });
            return result;
        }
    }

    class Branch {
        private final String uniqueName;
        private final XAResource resource;

        Branch(String uniqueName, XAResource resource) {
            if (uniqueName == null) {
                throw new NullPointerException("uniqueName is null");
            }
            if (resource == null) {
                throw new NullPointerException("resource is null");
            }
            this.uniqueName = uniqueName;
            this.resource = resource;
        }

        String getUniqueName() {
            return uniqueName;
        }

        XAResource getResource() {
            return resource;
        }
    }
}
