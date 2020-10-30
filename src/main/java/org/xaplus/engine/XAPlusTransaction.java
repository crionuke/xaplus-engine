package org.xaplus.engine;

import org.xaplus.engine.events.xa.XAPlusCommitBranchRequestEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchRequestEvent;
import org.xaplus.engine.events.xa.XAPlusRollbackBranchRequestEvent;

import javax.transaction.xa.XAResource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public class XAPlusTransaction {

    private final XAPlusXid xid;
    private final String serverId;
    private final String superiorServerId;
    private final long creationTimeInMillis;
    private final long expireTimeInMillis;
    private final Map<XAPlusXid, XABranch> xaBranches;
    private final Map<XAPlusXid, XAPlusBranch> xaPlusBranches;
    private final XAPlusFuture future;

    XAPlusTransaction(XAPlusXid xid, int timeoutInSeconds, String serverId) {
        this.xid = xid;
        this.serverId = serverId;
        superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
        creationTimeInMillis = System.currentTimeMillis();
        expireTimeInMillis = creationTimeInMillis + timeoutInSeconds * 1000;
        xaBranches = new ConcurrentHashMap<>();
        xaPlusBranches = new ConcurrentHashMap<>();
        future = new XAPlusFuture();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "=(superiorServerId=" + superiorServerId
                + ", isSuperior=" + isSuperior()
                + ", isSubordinate=" + isSubordinate()
                + ", isPrepared=" + isPrepared()
                + ", isReadied=" + isReadied()
                + ", isCommitted=" + isCommitted()
                + ", isRolledBack=" + isRolledBack()
                + ", isDone=" + isDone()
                + ", " + (expireTimeInMillis - System.currentTimeMillis()) + " ms to expire"
                + ", enlisted " + xaBranches.size() + " XA and " + xaPlusBranches.size() + " XA+ resources"
                + ", xid=" + xid;
    }

    XAPlusXid getXid() {
        return xid;
    }

    Set<XAPlusXid> getAllXids() {
        Set<XAPlusXid> xids = new HashSet<>();
        for (XABranch branch : xaBranches.values()) {
            xids.add(branch.getBranchXid());
        }
        for (XAPlusBranch branch : xaPlusBranches.values()) {
            xids.add(branch.getBranchXid());
        }
        return xids;
    }

    Map<XAPlusXid, String> getBranches() {
        Map<XAPlusXid, String> branches = new HashMap<>();
        for (XABranch branch : xaBranches.values()) {
            branches.put(branch.getBranchXid(), branch.getUniqueName());
        }
        for (XAPlusBranch branch : xaPlusBranches.values()) {
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

    // TODO: enlist and connections too, to close by engine when transaction completed

    void enlist(XAPlusXid branchXid, String uniqueName, XAResource resource) {
        xaBranches.put(branchXid, new XABranch(xid, branchXid, resource, uniqueName));
    }

    void enlist(XAPlusXid branchXid, String serverId, XAPlusResource resource) {
        xaPlusBranches.put(branchXid, new XAPlusBranch(xid, branchXid, resource, serverId));
    }

    boolean isSubordinate() {
        return !superiorServerId.equals(serverId);
    }

    boolean isSuperior() {
        return superiorServerId.equals(serverId);
    }

    void prepare(XAPlusDispatcher dispatcher) throws InterruptedException {
        for (XABranch xaBranch : xaBranches.values()) {
            xaBranch.prepare(dispatcher);
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            xaPlusBranch.prepare(dispatcher);
        }
    }

    boolean isPrepared() {
        for (XABranch xaBranch : xaBranches.values()) {
            if (!xaBranch.isPrepared()) {
                return false;
            }
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isPrepared()) {
                return false;
            }
        }
        return true;
    }

    void commit(XAPlusDispatcher dispatcher) throws InterruptedException {
        for (XABranch xaBranch : xaBranches.values()) {
            xaBranch.commit(dispatcher);
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            xaPlusBranch.commit(dispatcher);
        }
    }

    boolean isCommitted() {
        for (XABranch xaBranch : xaBranches.values()) {
            if (!xaBranch.isCommitted()) {
                return false;
            }
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isCommitted()) {
                return false;
            }
        }
        return true;
    }

    void rollback(XAPlusDispatcher dispatcher) throws InterruptedException {
        for (XABranch xaBranch : xaBranches.values()) {
            xaBranch.rollback(dispatcher);
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            xaPlusBranch.rollback(dispatcher);
        }
    }

    boolean isRolledBack() {
        for (XABranch xaBranch : xaBranches.values()) {
            if (!xaBranch.isRolledBack()) {
                return false;
            }
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isRolledBack()) {
                return false;
            }
        }
        return true;
    }

    void branchPrepared(XAPlusXid branchXid, boolean failed) {
        if (xaBranches.containsKey(branchXid)) {
            xaBranches.get(branchXid).markAsPrepared(failed);
        } else if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsPrepared(failed);
        }
    }

    void branchCommitted(XAPlusXid branchXid, boolean failed) {
        if (xaBranches.containsKey(branchXid)) {
            xaBranches.get(branchXid).markAsCommitted(failed);
        } else if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsCommitted(failed);
        }
    }

    void branchRolledBack(XAPlusXid branchXid, boolean failed) {
        if (xaBranches.containsKey(branchXid)) {
            xaBranches.get(branchXid).markAsRolledback(failed);
        } else if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsRolledback(failed);
        }
    }

    void branchReadied(XAPlusXid branchXid) {
        if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsReadied();
        }
    }

    boolean isReadied() {
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isReadied()) {
                return false;
            }
        }
        return true;
    }

    void branchDone(XAPlusXid branchXid) {
        if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsDone();
        }
    }

    boolean isDone() {
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isDone()) {
                return false;
            }
        }
        return true;
    }

    void branchAbsent(XAPlusXid branchXid) {
        if (xaPlusBranches.containsKey(branchXid)) {
            xaPlusBranches.get(branchXid).markAsAbsent();
        }
    }

    boolean isDoneOrAbsent() {
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            if (!xaPlusBranch.isDone() && !xaPlusBranch.isAbsent()) {
                return false;
            }
        }
        return true;
    }

    void resetFailures() {
        for (XABranch xaBranch : xaBranches.values()) {
            xaBranch.resetFail();
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            xaPlusBranch.resetFail();
        }
    }

    boolean hasFailures() {
        for (XABranch xaBranch : xaBranches.values()) {
            if (xaBranch.isFailed()) {
                return true;
            }
        }
        for (XAPlusBranch xaPlusBranch : xaPlusBranches.values()) {
            if (xaPlusBranch.isFailed()) {
                return true;
            }
        }
        return false;
    }

    class XABranch {
        final XAPlusXid xid;
        final XAPlusXid branchXid;
        final XAResource xaResource;
        final String uniqueName;

        volatile boolean prepared;
        volatile boolean committed;
        volatile boolean rolledBack;
        volatile boolean failed;

        XABranch(XAPlusXid xid, XAPlusXid branchXid, XAResource xaResource, String uniqueName) {
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

        void markAsPrepared(boolean failed) {
            if (failed) {
                this.failed = true;
            }
            prepared = true;
        }

        void commit(XAPlusDispatcher dispatcher) throws InterruptedException {
            dispatcher.dispatch(new XAPlusCommitBranchRequestEvent(xid, branchXid, xaResource));
        }

        void markAsCommitted(boolean failed) {
            if (failed) {
                this.failed = true;
            }
            committed = true;
        }

        boolean isCommitted() {
            return committed;
        }

        void rollback(XAPlusDispatcher dispatcher) throws InterruptedException {
            dispatcher.dispatch(new XAPlusRollbackBranchRequestEvent(xid, branchXid, xaResource));
        }

        void markAsRolledback(boolean failed) {
            if (failed) {
                this.failed = true;
            }
            rolledBack = true;
        }

        boolean isRolledBack() {
            return rolledBack;
        }

        boolean isFailed() {
            return failed;
        }

        void resetFail() {
            failed = false;
        }
    }

    class XAPlusBranch extends XABranch {

        final XAPlusResource xaPlusResource;

        volatile boolean readied;
        volatile boolean done;
        volatile boolean absent;

        XAPlusBranch(XAPlusXid xid, XAPlusXid branchXid, XAPlusResource resource, String uniqueName) {
            super(xid, branchXid, resource, uniqueName);
            this.xaPlusResource = resource;
            readied = false;
            done = false;
            absent = false;
        }

        XAPlusResource getXaPlusResource() {
            return xaPlusResource;
        }

        void markAsReadied() {
            readied = true;
        }

        boolean isReadied() {
            return readied;
        }

        void markAsDone() {
            done = true;
        }

        boolean isDone() {
            return done;
        }

        void markAsAbsent() {
            absent = true;
        }

        boolean isAbsent() {
            return absent;
        }
    }
}
