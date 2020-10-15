package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.*;
import org.xaplus.engine.events.journal.XAPlusCommitRecoveredXidDecisionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitRecoveredXidDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackRecoveredXidDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusRollbackRecoveredXidDecisionLoggedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusRecoveryService extends Bolt implements
        XAPlusRecoveryRequestEvent.Handler,
        XAPlusResourceRecoveredEvent.Handler,
        XAPlusRecoveryResourceFailedEvent.Handler,
        XAPlusDanglingTransactionsFoundEvent.Handler,
        XAPlusFindDanglingTransactionsFailedEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler,
        XAPlusCommitRecoveredXidDecisionLoggedEvent.Handler,
        XAPlusRollbackRecoveredXidDecisionLoggedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusRecoveryState state;
    private final XAPlusRecoverySuperiorTracker superiorTracker;
    private final XAPlusRecoverySubordinateTracker subordinateTracker;

    XAPlusRecoveryService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                          XAPlusResources resources) {
        super("recovery", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        state = new XAPlusRecoveryState();
        superiorTracker = new XAPlusRecoverySuperiorTracker();
        subordinateTracker = new XAPlusRecoverySubordinateTracker();
    }

    @Override
    public void handleRecoveryRequest(XAPlusRecoveryRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (state.isStarted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery already started, skip request");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery started");
            }
            state.setStarted();
            dispatcher.dispatch(new XAPlusFindDanglingTransactionsRequestEvent());
            // Recovery all registered resources
            Map<String, XAPlusResources.Wrapper> wrappers = resources.getResources();
            for (Map.Entry<String, XAPlusResources.Wrapper> entry : wrappers.entrySet()) {
                String uniqueName = entry.getKey();
                XAPlusResources.Wrapper wrapper = entry.getValue();
                try {
                    XAResource resource = null;
                    if (wrapper instanceof XAPlusResources.XADataSourceWrapper) {
                        javax.sql.XAConnection connection = ((XAPlusResources.XADataSourceWrapper) wrapper).get();
                        state.track(uniqueName, connection);
                        resource = connection.getXAResource();
                    } else if (wrapper instanceof XAPlusResources.XAConnectionFactoryWrapper) {
                        javax.jms.XAConnection connection =
                                ((XAPlusResources.XAConnectionFactoryWrapper) wrapper).get();
                        state.track(uniqueName, connection);
                        resource = connection.createXASession().getXAResource();
                    }
                    if (resource != null) {
                        dispatcher.dispatch(new XAPlusRecoveryResourceRequestEvent(uniqueName, resource));
                    }
                } catch (SQLException | JMSException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Recovery resource {} failed with {}", uniqueName, e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public void handleResourceRecovered(XAPlusResourceRecoveredEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (state.isStarted()) {
            state.setRecoveredXids(event.getUniqueName(), event.getRecoveredXids());
            checkTracker();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery not started yet");
            }
        }
    }

    @Override
    public void handleRecoveryResourceFailed(XAPlusRecoveryResourceFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (state.isStarted()) {
            state.setRecoveryFailed(event.getUniqueName());
            checkTracker();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery not started yet");
            }
        }
    }

    @Override
    public void handleDanglingTransactionFound(XAPlusDanglingTransactionsFoundEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (state.isStarted()) {
            state.setDanglingTransactions(event.getDanglingTransactions());
            checkTracker();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery not started yet");
            }
        }
    }

    @Override
    public void handleFindDanglingTransactionsFailed(XAPlusFindDanglingTransactionsFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (state.isStarted()) {
            state.setFailed();
            checkTracker();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery not started yet");
            }
        }
    }

    @Override
    public void handleRemoteSubordinateDone(XAPlusRemoteSubordinateDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = superiorTracker.remove(xid);
        if (uniqueName != null) {
            dispatcher.dispatch(new XAPlusDanglingTransactionCommittedEvent(xid, uniqueName));
            //TODO: detect how transaction completed (commited or rolledback)
            //dispatcher.dispatch(new XAPlusDanglingTransactionRolledBackEvent(xid, uniqueName));
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToCommit(XAPlusRemoteSuperiorOrderToCommitEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        Map<String, XAPlusXid> branches = subordinateTracker.remove(xid);
        if (branches != null) {
            for (Map.Entry<String, XAPlusXid> entry : branches.entrySet()) {
                String uniqueName = entry.getKey();
                XAPlusXid branchXid = entry.getValue();
                XAResource resource = state.getXAResource(uniqueName);
                if (resource != null) {
                    dispatcher.dispatch(new XAPlusLogCommitRecoveredXidDecisionEvent(branchXid, uniqueName));
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unknown XA resource with uniqueName={} to commit branchXid={}",
                                uniqueName, branches);
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
        Map<String, XAPlusXid> branches = subordinateTracker.remove(xid);
        if (branches != null) {
            for (Map.Entry<String, XAPlusXid> entry : branches.entrySet()) {
                String uniqueName = entry.getKey();
                XAPlusXid branchXid = entry.getValue();
                XAResource resource = state.getXAResource(uniqueName);
                if (resource != null) {
                    dispatcher.dispatch(new XAPlusLogRollbackRecoveredXidDecisionEvent(branchXid, uniqueName));
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unknown XA resource with uniqueName={} to commit branchXid={}",
                                uniqueName, branches);
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
        XAResource xaResource = state.getXAResource(uniqueName);
        if (xaResource != null) {
            dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(xid, xaResource, uniqueName));
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown XA resource wi1th uniqueName={} to commit recovered xid={}", uniqueName, xid);
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
        XAResource xaResource = state.getXAResource(uniqueName);
        if (xaResource != null) {
            dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(xid, xaResource, uniqueName));
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown XA resource with uniqueName={} to rollback recovered xid={}", uniqueName, xid);
            }
        }
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRecoveryRequestEvent.class);
        dispatcher.subscribe(this, XAPlusResourceRecoveredEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveryResourceFailedEvent.class);
        dispatcher.subscribe(this, XAPlusDanglingTransactionsFoundEvent.class);
        dispatcher.subscribe(this, XAPlusFindDanglingTransactionsFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToCommitEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
        dispatcher.subscribe(this, XAPlusCommitRecoveredXidDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackRecoveredXidDecisionLoggedEvent.class);
    }

    private void checkTracker() throws InterruptedException {
        //TODO: waiting while all XA operation finished to closed connections
        if (state.isReady()) {
            if (logger.isInfoEnabled()) {
                logger.info("Server ready to recovery, starting");
            }
            retry();
            recovery();
            reset();
        } else if (state.isRecovered() && state.isFailed()) {
            if (logger.isInfoEnabled()) {
                logger.info("Server recovery failed, reset state");
            }
            reset();
        }
    }

    private void reset() {
        state.reset();
        superiorTracker.reset();
        subordinateTracker.reset();
    }

    private void retry() throws InterruptedException {
        Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = state.getDanglingTransactions();
        for (Map.Entry<String, Map<XAPlusXid, Boolean>> entry : danglingTransactions.entrySet()) {
            String uniqueName = entry.getKey();
            Map<XAPlusXid, Boolean> transactions = entry.getValue();
            superiorTracker.clear(uniqueName);
            // Ignore transactions on non XA+ resources
            if (resources.isXAPlusResource(uniqueName)) {
                for (Map.Entry<XAPlusXid, Boolean> transaction : transactions.entrySet()) {
                    XAPlusXid xid = transaction.getKey();
                    Boolean status = transaction.getValue();
                    try {
                        XAPlusResource resource = resources.getXAPlusResource(uniqueName);
                        if (status) {
                            dispatcher.dispatch(new XAPlusRetryCommitOrderRequestEvent(xid, resource));
                        } else {
                            dispatcher.dispatch(new XAPlusRetryRollbackOrderRequestEvent(xid, resource));
                        }
                        superiorTracker.track(xid, uniqueName);
                    } catch (XAPlusSystemException e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Internal error. Retry for non XA+ or unknown resource with " +
                                    "uniqueName={}. But previous check say that resource is XA+", uniqueName);
                        }
                    }
                }
            }
        }
    }

    private void recovery() throws InterruptedException {
        Set<String> superiorServers = new HashSet<>();
        for (Map.Entry<String, Set<XAPlusXid>> entry : state.getRecoveredXids().entrySet()) {
            String uniqueName = entry.getKey();
            XAResource xaResource = state.getXAResource(uniqueName);
            if (xaResource != null) {
                Set<XAPlusXid> xids = entry.getValue();
                for (XAPlusXid xid : xids) {
                    Map<XAPlusXid, Boolean> resourceXids = state.getDanglingTransactions().get(uniqueName);
                    if (resourceXids != null && resourceXids.containsKey(xid)) {
                        boolean status = resourceXids.get(xid);
                        if (status) {
                            dispatcher.dispatch(
                                    new XAPlusCommitRecoveredXidRequestEvent(xid, xaResource, uniqueName));
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
                            subordinateTracker.track(xid, uniqueName);
                            if (superiorServers.add(superiorServerId)) {
                                try {
                                    dispatcher.dispatch(new XAPlusRetryFromSuperiorRequestEvent(
                                            resources.getXAPlusResource(superiorServerId)));
                                } catch (XAPlusSystemException e) {
                                    if (logger.isWarnEnabled()) {
                                        logger.warn("Unknown XA+ resource with uniqueName={} to retry",
                                                superiorServerId, xid);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("Unknown XA resource with uniqueName={} to recovery", uniqueName);
                }
            }
        }
    }
}