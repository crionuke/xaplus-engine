package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xaplus.*;
import org.xaplus.engine.exceptions.XAPlusSystemException;

import javax.transaction.xa.XAResource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class XAPlusRecoveryCommitterService extends Bolt implements
        XAPlusRecoveryPreparedEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryCommitterService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusRecoveryCommitterTracker tracker;
    private final XAPlusRecoveryOrdersTracker ordersTracker;
    private final XAPlusRecoveryRetriesTracker retriesTracker;

    XAPlusRecoveryCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                   XAPlusDispatcher dispatcher, XAPlusResources resources,
                                   XAPlusRecoveryCommitterTracker tracker, XAPlusRecoveryOrdersTracker ordersTracker,
                                   XAPlusRecoveryRetriesTracker retriesTracker) {
        super(properties.getServerId() + "-recovery-committer", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = tracker;
        this.ordersTracker = ordersTracker;
        this.retriesTracker = retriesTracker;
    }

    @Override
    public void handleRecoveryPrepared(XAPlusRecoveryPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (tracker.isStarted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery already started");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Commit recovery, {}", System.currentTimeMillis());
            }
            tracker.start(event.getJdbcConnections(), event.getJmsContexts(), event.getXaResources(),
                    event.getRecoveredXids(), event.getDanglingTransactions());
            recoveryResources();
            // TODO: close all connections opened to recover resources
            // TODO: handle timeout for recovery process
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToCommit(XAPlusRemoteSuperiorOrderToCommitEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        Map<XAPlusXid, String> branches = retriesTracker.remove(xid);
        if (branches != null) {
            for (Map.Entry<XAPlusXid, String> entry : branches.entrySet()) {
                XAPlusXid branchXid = entry.getKey();
                String uniqueName = entry.getValue();
                XAResource resource = tracker.getXaResources().get(uniqueName);
                if (resource != null) {
                    dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(xid, resource, uniqueName));
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unknown XA xaResource to commit branch, resource={}, xid={}",
                                uniqueName, branchXid);
                    }
                }
            }
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToRollback(XAPlusRemoteSuperiorOrderToRollbackEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        Map<XAPlusXid, String> branches = retriesTracker.remove(xid);
        if (branches != null) {
            for (Map.Entry<XAPlusXid, String> entry : branches.entrySet()) {
                XAPlusXid branchXid = entry.getKey();
                String uniqueName = entry.getValue();
                XAResource resource = tracker.getXaResources().get(uniqueName);
                if (resource != null) {
                    dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(xid, resource, uniqueName));
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unknown XA xaResource to rollback branch, resource={}, xid={}",
                                uniqueName, branchXid);
                    }
                }
            }
        }
    }

    @Override
    public void handleRemoteSubordinateDone(XAPlusRemoteSubordinateDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = ordersTracker.getUniqueName(xid);
        if (uniqueName != null) {
            if (ordersTracker.getStatus(xid)) {
                dispatcher.dispatch(new XAPlusDanglingTransactionCommittedEvent(xid, uniqueName));
            } else {
                dispatcher.dispatch(new XAPlusDanglingTransactionRolledBackEvent(xid, uniqueName));
            }
            ordersTracker.remove(xid);
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRecoveryPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToCommitEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private void recoveryResources() throws InterruptedException {
        Map<String, Set<XAPlusXid>> recoveredXids = tracker.getRecoveredXids();
        Set<String> superiorServers = new HashSet<>();
        for (String uniqueName : recoveredXids.keySet()) {
            XAResource xaResource = tracker.getXaResources().get(uniqueName);
            if (xaResource != null) {
                Set<XAPlusXid> xids = recoveredXids.get(uniqueName);
                for (XAPlusXid xid : xids) {
                    Map<XAPlusUid, Boolean> danglingXids = tracker.getDanglingTransactions();
                    if (danglingXids.containsKey(xid.getGlobalTransactionIdUid())) {
                        boolean status = danglingXids.get(xid.getGlobalTransactionIdUid());
                        if (status) {
                            dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(xid, xaResource, uniqueName));
                        } else {
                            dispatcher.dispatch(
                                    new XAPlusRollbackRecoveredXidRequestEvent(xid, xaResource, uniqueName));
                        }
                    } else {
                        String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
                        if (superiorServerId.equals(properties.getServerId())) {
                            // Rollback recovered transaction's xid as was no commit command found
                            dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(
                                    xid, xaResource, uniqueName));
                        } else {
                            retriesTracker.track(xid, uniqueName);
                            if (superiorServers.add(superiorServerId)) {
                                try {
                                    dispatcher.dispatch(new XAPlusRetryFromSuperiorRequestEvent(superiorServerId,
                                            resources.getXAPlusResource(superiorServerId)));
                                } catch (XAPlusSystemException e) {
                                    if (logger.isWarnEnabled()) {
                                        logger.warn("Unknown XA+ resource to retry orders, xaResource={}",
                                                superiorServerId);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("Unknown XA resource to recovery, resource={}", uniqueName);
                }
            }
        }
        // TODO: Handle orphan transaction found in journal but not recovered from XA resource
    }
}
