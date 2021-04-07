package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.xa.XAPlusCommitBranchRequestEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchRequestEvent;
import org.xaplus.engine.events.xa.XAPlusRollbackBranchRequestEvent;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public class XAPlusTransaction {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTransaction.class);

    private final XAPlusXid xid;
    private final String serverId;
    private final String superiorServerId;
    private final long creationTimeInMillis;
    private final long expireTimeInMillis;
    private final List<javax.sql.XAConnection> connections;
    private final List<javax.jms.XAJMSContext> contexts;
    private final Map<XAPlusXid, Branch> xaBranches;
    private final Map<XAPlusXid, Branch> xaPlusBranches;
    private final XAPlusFuture future;
    private volatile boolean rollbackOnly;

    XAPlusTransaction(XAPlusXid xid, int timeoutInSeconds, String serverId) {
        this.xid = xid;
        this.serverId = serverId;
        superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
        creationTimeInMillis = System.currentTimeMillis();
        expireTimeInMillis = creationTimeInMillis + timeoutInSeconds * 1000;
        connections = new CopyOnWriteArrayList<>();
        contexts = new CopyOnWriteArrayList<>();
        xaBranches = new ConcurrentHashMap<>();
        xaPlusBranches = new ConcurrentHashMap<>();
        future = new XAPlusFuture();
        rollbackOnly = false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "=(superiorServerId=" + superiorServerId
                + ", " + (expireTimeInMillis - System.currentTimeMillis()) + " ms to expire"
                + ", enlisted " + xaBranches.size() + " XA and " + xaPlusBranches.size() + " XA+ resources"
                + ", xid=" + xid + ")";
    }

    XAPlusXid getXid() {
        return xid;
    }

    boolean hasXAPlusBranches() {
        return !xaPlusBranches.isEmpty();
    }

    Map<XAPlusXid, String> getBranches() {
        Map<XAPlusXid, String> branches = new HashMap<>();
        for (Branch branch : xaBranches.values()) {
            branches.put(branch.getBranchXid(), branch.getUniqueName());
        }
        for (Branch branch : xaPlusBranches.values()) {
            branches.put(branch.getBranchXid(), branch.getUniqueName());
        }
        return branches;
    }

    long getCreationTimeInMillis() {
        return creationTimeInMillis;
    }

    long getExpireTimeInMillis() {
        return expireTimeInMillis;
    }

    XAPlusFuture getFuture() {
        return future;
    }

    void enlist(XAPlusXid branchXid, String uniqueName, javax.sql.XAConnection connection)
            throws SQLException, XAException {
        connections.add(connection);
        XAResource resource = connection.getXAResource();
        startXABranch(branchXid, uniqueName, resource);
    }

    void enlist(XAPlusXid branchXid, String uniqueName, javax.jms.XAJMSContext context)
            throws XAException {
        contexts.add(context);
        XAResource resource = context.getXAResource();
        startXABranch(branchXid, uniqueName, resource);
    }

    void close() {
        for (javax.sql.XAConnection connection : connections) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Close connection failed, {}", e.getMessage(), e);
            }
        }
        for (javax.jms.XAJMSContext context : contexts) {
            context.close();
        }
    }

    void enlist(XAPlusXid branchXid, String serverId, XAPlusResource resource) {
        xaPlusBranches.put(branchXid, new Branch(xid, branchXid, resource, serverId));
    }

    void clear(List<XAPlusXid> xids) {
        Iterator<Map.Entry<XAPlusXid, Branch>> iterator = xaPlusBranches.entrySet().iterator();
        while (iterator.hasNext()) {
            XAPlusXid xid = iterator.next().getKey();
            if (xids.contains(xid)) {
                continue;
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("XA+ branch removed as not required, xid={}", xid);
                }
                iterator.remove();
            }
        }
    }

    boolean contains(XAPlusXid branchXid) {
        return xaBranches.containsKey(branchXid) || xaPlusBranches.containsKey(branchXid);
    }

    boolean isSubordinate() {
        return !superiorServerId.equals(serverId);
    }

    boolean isSuperior() {
        return superiorServerId.equals(serverId);
    }

    boolean isPrepareDone() {
        for (Branch xaBranch : xaBranches.values()) {
            if (!xaBranch.isPrepared()) {
                return false;
            }
        }
        for (Branch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isPrepared()) {
                return false;
            }
        }
        return true;
    }

    boolean isCommitDone() {
        for (Branch xaBranch : xaBranches.values()) {
            if (!xaBranch.isCommitted()) {
                return false;
            }
        }
        for (Branch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isCommitted()) {
                return false;
            }
        }
        return true;
    }

    boolean isRollbackDone() {
        for (Branch xaBranch : xaBranches.values()) {
            if (!xaBranch.isRolledBack()) {
                return false;
            }
        }
        for (Branch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isRolledBack()) {
                return false;
            }
        }
        return true;
    }

    boolean isRollbackOnly() {
        return rollbackOnly;
    }

    void markAsRollbackOnly() {
        rollbackOnly = true;
    }

    void prepare(XAPlusDispatcher dispatcher) throws InterruptedException {
        for (Branch xaBranch : xaBranches.values()) {
            xaBranch.prepare(dispatcher);
        }
    }

    void branchPrepared(XAPlusXid branchXid) {
        if (xaBranches.containsKey(branchXid)) {
            xaBranches.get(branchXid).markAsPrepared();
        } else if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsPrepared();
        }
    }

    void commit(XAPlusDispatcher dispatcher) throws InterruptedException {
        for (Branch xaBranch : xaBranches.values()) {
            xaBranch.commit(dispatcher);
        }
        for (Branch xaPlusBranch : xaPlusBranches.values()) {
            xaPlusBranch.commit(dispatcher);
        }
    }

    void branchCommitted(XAPlusXid branchXid) {
        if (xaBranches.containsKey(branchXid)) {
            xaBranches.get(branchXid).markAsCommitted();
        } else if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsCommitted();
        }
    }

    void rollback(XAPlusDispatcher dispatcher) throws InterruptedException {
        for (Branch xaBranch : xaBranches.values()) {
            xaBranch.rollback(dispatcher);
        }
        for (Branch xaPlusBranch : xaPlusBranches.values()) {
            xaPlusBranch.rollback(dispatcher);
        }
    }

    void branchRolledBack(XAPlusXid branchXid) {
        if (xaBranches.containsKey(branchXid)) {
            xaBranches.get(branchXid).markAsRolledback();
        } else if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsRolledback();
        }
    }

    void branchFailed(XAPlusXid branchXid) {
        if (xaBranches.containsKey(branchXid)) {
            xaBranches.get(branchXid).markAsFailed();
        } else if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsFailed();
        }
    }

    boolean hasFailures() {
        for (Branch xaBranch : xaBranches.values()) {
            if (xaBranch.isFailed()) {
                return true;
            }
        }
        for (Branch xaPlusBranch : xaPlusBranches.values()) {
            if (xaPlusBranch.isFailed()) {
                return true;
            }
        }
        return false;
    }

    void reset() {
        for (Branch xaBranch : xaBranches.values()) {
            xaBranch.reset();
        }
        for (Branch xaPlusBranch : xaPlusBranches.values()) {
            xaPlusBranch.reset();
        }
    }

    private void startXABranch(XAPlusXid branchXid, String uniqueName, XAResource resource) throws XAException {
        if (logger.isTraceEnabled()) {
            logger.trace("Starting branch, branchXid={}, resource={}", branchXid, resource);
        }
        resource.start(branchXid, XAResource.TMNOFLAGS);
        if (logger.isDebugEnabled()) {
            logger.debug("Branch started, branchXid={}, resource={}", branchXid, resource);
        }
        xaBranches.put(branchXid, new Branch(xid, branchXid, resource, uniqueName));
    }

    class Branch {
        final XAPlusXid xid;
        final XAPlusXid branchXid;
        final XAResource xaResource;
        final String uniqueName;

        volatile boolean prepared;
        volatile boolean committed;
        volatile boolean rolledBack;
        volatile boolean failed;

        Branch(XAPlusXid xid, XAPlusXid branchXid, XAResource xaResource, String uniqueName) {
            this.xid = xid;
            this.branchXid = branchXid;
            this.xaResource = xaResource;
            this.uniqueName = uniqueName;
            prepared = false;
            committed = false;
            rolledBack = false;
            failed = false;
        }

        XAPlusXid getXid() {
            return xid;
        }

        XAPlusXid getBranchXid() {
            return branchXid;
        }

        XAResource getXaResource() {
            return xaResource;
        }

        String getUniqueName() {
            return uniqueName;
        }

        void prepare(XAPlusDispatcher dispatcher) throws InterruptedException {
            dispatcher.dispatch(new XAPlusPrepareBranchRequestEvent(xid, branchXid, xaResource));
        }

        boolean isPrepared() {
            return prepared;
        }

        void markAsPrepared() {
            prepared = true;
        }

        void commit(XAPlusDispatcher dispatcher) throws InterruptedException {
            dispatcher.dispatch(new XAPlusCommitBranchRequestEvent(xid, branchXid, xaResource));
        }

        void markAsCommitted() {
            committed = true;
        }

        boolean isCommitted() {
            return committed;
        }

        void rollback(XAPlusDispatcher dispatcher) throws InterruptedException {
            dispatcher.dispatch(new XAPlusRollbackBranchRequestEvent(xid, branchXid, xaResource));
        }

        void markAsRolledback() {
            rolledBack = true;
        }

        boolean isRolledBack() {
            return rolledBack;
        }

        boolean isFailed() {
            return failed;
        }

        void markAsFailed() {
            failed = true;
        }

        void reset() {
            failed = false;
        }
    }
}
