package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
import com.crionuke.xaplus.events.xaplus.XAPlusRemoteSubordinateDoneEvent;
import com.crionuke.xaplus.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import com.crionuke.xaplus.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import com.crionuke.xaplus.exceptions.XAPlusSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
    private final RecoveryTracker recoveryTracker;
    private final SuperiorTracker superiorTracker;
    private final SubordinateTracker subordinateTracker;

    XAPlusRecoveryService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                          XAPlusResources resources) {
        super("recovery", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        recoveryTracker = new RecoveryTracker();
        superiorTracker = new SuperiorTracker();
        subordinateTracker = new SubordinateTracker();
    }

    @Override
    public void handleRecoveryServerRequest(XAPlusRecoveryRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (recoveryTracker.isStarted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery already started");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery started");
            }
            recoveryTracker.setStarted();
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
                        recoveryTracker.track(uniqueName, connection);
                        resource = connection.getXAResource();
                    } else if (wrapper instanceof XAPlusResources.XAConnectionFactoryWrapper) {
                        javax.jms.XAConnection connection =
                                ((XAPlusResources.XAConnectionFactoryWrapper) wrapper).get();
                        recoveryTracker.track(uniqueName, connection);
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
        if (recoveryTracker.isStarted()) {
            recoveryTracker.setRecoveredXids(event.getUniqueName(), event.getRecoveredXids());
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
        if (recoveryTracker.isStarted()) {
            recoveryTracker.setRecoveryFailed(event.getUniqueName());
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
        if (recoveryTracker.isStarted()) {
            recoveryTracker.setDanglingTransactions(event.getDanglingTransactions());
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
        if (recoveryTracker.isStarted()) {
            recoveryTracker.setFailed();
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
            dispatcher.dispatch(new XAPlusDanglingTransactionDoneEvent(xid, uniqueName));
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
                XAResource resource = recoveryTracker.getXAResource(uniqueName);
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
                XAResource resource = recoveryTracker.getXAResource(uniqueName);
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
        XAResource xaResource = recoveryTracker.getXAResource(uniqueName);
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
        XAResource xaResource = recoveryTracker.getXAResource(uniqueName);
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
        if (recoveryTracker.isReady()) {
            retry();
            recovery();
            reset();
        } else if (recoveryTracker.isRecovered() && recoveryTracker.isFailed()) {
            reset();
        }
    }

    private void reset() {
        recoveryTracker.reset();
        superiorTracker.reset();
        subordinateTracker.reset();
    }

    private void retry() throws InterruptedException {
        Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = recoveryTracker.getDanglingTransactions();
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
        for (Map.Entry<String, Set<XAPlusXid>> entry : recoveryTracker.getRecoveredXids().entrySet()) {
            String uniqueName = entry.getKey();
            XAResource xaResource = recoveryTracker.getXAResource(uniqueName);
            if (xaResource != null) {
                Set<XAPlusXid> xids = entry.getValue();
                for (XAPlusXid xid : xids) {
                    Map<XAPlusXid, Boolean> resourceXids = recoveryTracker.getDanglingTransactions().get(uniqueName);
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

    private class RecoveryTracker {

        private boolean started;
        private boolean failed;
        private Map<String, javax.sql.XAConnection> jdbcConnections;
        private Map<String, javax.jms.XAConnection> jmsConnections;
        private Map<String, XAResource> xaResources;
        private Map<String, Set<XAPlusXid>> recoveredXids;
        private Set<String> recovering;
        private Map<String, Map<XAPlusXid, Boolean>> danglingTransactions;

        RecoveryTracker() {
            started = false;
            failed = false;
            jdbcConnections = new HashMap<>();
            jmsConnections = new HashMap<>();
            xaResources = new HashMap<>();
            recoveredXids = new HashMap<>();
            recovering = new HashSet<>();
            danglingTransactions = null;
        }

        void track(String uniqueName, javax.sql.XAConnection xaConnection) throws SQLException {
            jdbcConnections.put(uniqueName, xaConnection);
            xaResources.put(uniqueName, xaConnection.getXAResource());
            recovering.add(uniqueName);
        }

        void track(String uniqueName, javax.jms.XAConnection xaConnection) throws JMSException {
            jmsConnections.put(uniqueName, xaConnection);
            xaResources.put(uniqueName, xaConnection.createXASession().getXAResource());
            recovering.add(uniqueName);
        }

        void setStarted() {
            this.started = true;
        }

        void setFailed() {
            this.failed = true;
        }

        void setRecoveredXids(String uniqueName, Set<XAPlusXid> xids) {
            recovering.remove(uniqueName);
            recoveredXids.put(uniqueName, xids);
        }

        void setRecoveryFailed(String uniqueName) {
            recovering.remove(uniqueName);
        }

        boolean isStarted() {
            return started;
        }

        boolean isFailed() {
            return failed;
        }

        boolean isRecovered() {
            return recovering.isEmpty();
        }

        boolean isReady() {
            return recovering.isEmpty() && danglingTransactions != null;
        }

        Map<String, Set<XAPlusXid>> getRecoveredXids() {
            return recoveredXids;
        }

        Map<String, Map<XAPlusXid, Boolean>> getDanglingTransactions() {
            return danglingTransactions;
        }

        void setDanglingTransactions(Map<String, Map<XAPlusXid, Boolean>> danglingTransactions) {
            this.danglingTransactions = danglingTransactions;
        }

        XAResource getXAResource(String uniqueName) {
            return xaResources.get(uniqueName);
        }

        void reset() {
            started = false;
            //TODO: close all connections
            jdbcConnections.clear();
            jmsConnections.clear();
            xaResources.clear();
            recoveredXids.clear();
            recovering.clear();
            danglingTransactions.clear();
        }
    }

    private class SuperiorTracker {
        private final Map<XAPlusXid, String> doing;

        SuperiorTracker() {
            doing = new HashMap<>();
        }

        void track(XAPlusXid xid, String uniqueName) {
            doing.put(xid, uniqueName);
        }

        void clear(String uniqueName) {
            Iterator<Map.Entry<XAPlusXid, String>> iterator = doing.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<XAPlusXid, String> entry = iterator.next();
                String entryValue = entry.getValue();
                if (entryValue.equals(uniqueName)) {
                    iterator.remove();
                }
            }
        }

        String remove(XAPlusXid xid) {
            return doing.remove(xid);
        }

        void reset() {
            doing.clear();
        }
    }

    private class SubordinateTracker {
        private Map<XAPlusUid, Map<String, XAPlusXid>> waiting;

        SubordinateTracker() {
            waiting = new HashMap<>();
        }

        void track(XAPlusXid xid, String uniqueName) {
            XAPlusUid gtrid = xid.getGlobalTransactionIdUid();
            Map<String, XAPlusXid> xids = waiting.get(gtrid);
            if (xids == null) {
                xids = new HashMap<>();
                waiting.put(gtrid, xids);
            }
            xids.put(uniqueName, xid);
        }

        Map<String, XAPlusXid> remove(XAPlusXid xid) {
            return waiting.remove(xid.getGlobalTransactionIdUid());
        }

        void reset() {
            waiting.clear();
        }
    }
}