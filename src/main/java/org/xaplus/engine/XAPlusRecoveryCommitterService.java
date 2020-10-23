package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusRetryCommitOrderRequestEvent;
import org.xaplus.engine.events.XAPlusRetryFromSuperiorRequestEvent;
import org.xaplus.engine.events.XAPlusRetryRollbackOrderRequestEvent;
import org.xaplus.engine.events.journal.XAPlusCommitRecoveredXidDecisionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitRecoveredXidDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackRecoveredXidDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusRollbackRecoveredXidDecisionLoggedEvent;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

import javax.transaction.xa.XAResource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class XAPlusRecoveryCommitterService extends Bolt implements
        XAPlusRecoveryPreparedEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler,
        XAPlusCommitRecoveredXidDecisionLoggedEvent.Handler,
        XAPlusRollbackRecoveredXidDecisionLoggedEvent.Handler {
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
            tracker.start(event.getJdbcConnections(), event.getJmsConnections(), event.getXaResources(),
                    event.getRecoveredXids(), event.getDanglingTransactions());
            retryOrders();
            recoveryResources();
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
                    dispatcher.dispatch(new XAPlusLogCommitRecoveredXidDecisionEvent(branchXid, uniqueName));
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unknown XA xaResource to commit branch, xaResource={}, xid={}",
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
                    dispatcher.dispatch(new XAPlusLogRollbackRecoveredXidDecisionEvent(branchXid, uniqueName));
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unknown XA xaResource to rollback branch, xaResource={}, xid={}",
                                uniqueName, branchXid);
                    }
                }
            }
        }
    }

    @Override
    public void handleCommitRecoveredXidDecisionLogged(XAPlusCommitRecoveredXidDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        XAResource xaResource = tracker.getXaResources().get(uniqueName);
        if (xaResource != null) {
            dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(xid, xaResource, uniqueName));
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown XA xaResource to commit recovered branch, xaResource={}, xid={}",
                        uniqueName, xid);
            }
        }
    }

    @Override
    public void handleRollbackRecoveredXidDecisionLogged(XAPlusRollbackRecoveredXidDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        XAResource xaResource = tracker.getXaResources().get(uniqueName);
        if (xaResource != null) {
            dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(xid, xaResource, uniqueName));
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown XA xaResource to rollback recovered branch, xaResource={}, xid={}",
                        uniqueName, xid);
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
        dispatcher.subscribe(this, XAPlusCommitRecoveredXidDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackRecoveredXidDecisionLoggedEvent.class);
    }

    private void retryOrders() throws InterruptedException {
        Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = tracker.getDanglingTransactions();
        for (String uniqueName : danglingTransactions.keySet()) {
            // Ignore transactions on non XA+ resources
            if (resources.isXAPlusResource(uniqueName)) {
                try {
                    XAPlusResource resource = resources.getXAPlusResource(uniqueName);
                    Map<XAPlusXid, Boolean> transactions = danglingTransactions.get(uniqueName);
                    for (XAPlusXid xid : transactions.keySet()) {
                        boolean status = transactions.get(xid);
                        if (status) {
                            dispatcher.dispatch(new XAPlusRetryCommitOrderRequestEvent(xid, resource));
                        } else {
                            dispatcher.dispatch(new XAPlusRetryRollbackOrderRequestEvent(xid, resource));
                        }
                        ordersTracker.track(xid, uniqueName, status);
                    }
                } catch (XAPlusSystemException e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Internal error. Retry order for non XA+ or unknown xaResource, xaResource={}",
                                uniqueName);
                    }
                }
            }
        }
    }

    private void recoveryResources() throws InterruptedException {
        Map<String, Set<XAPlusXid>> recoveredXids = tracker.getRecoveredXids();
        Set<String> superiorServers = new HashSet<>();
        for (String uniqueName : recoveredXids.keySet()) {
            XAResource xaResource = tracker.getXaResources().get(uniqueName);
            if (xaResource != null) {
                Set<XAPlusXid> xids = recoveredXids.get(uniqueName);
                for (XAPlusXid xid : xids) {
                    Map<XAPlusXid, Boolean> danglingXids = tracker.getDanglingTransactions().get(uniqueName);
                    if (danglingXids != null && danglingXids.containsKey(xid)) {
                        boolean status = danglingXids.get(xid);
                        if (status) {
                            dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(xid, xaResource, uniqueName));
                        } else {
                            dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(xid,
                                    xaResource, uniqueName));
                        }
                    } else {
                        String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
                        if (superiorServerId.equals(properties.getServerId())) {
                            // Rollback recovered getTransaction's xid as was no commit command found
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
                                        logger.warn("Unknown XA+ xaResource to retry orders, xaResource={}",
                                                superiorServerId);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("Unknown XA xaResource to recovery, xaResource={}", uniqueName);
                }
            }
        }
    }
}
