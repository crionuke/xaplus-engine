package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xa.*;
import org.xaplus.engine.events.xaplus.*;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusService extends Bolt implements
        XAPlusPrepareBranchRequestEvent.Handler,
        XAPlusCommitBranchRequestEvent.Handler,
        XAPlusRollbackBranchRequestEvent.Handler,
        XAPlusRetryCommitOrderRequestEvent.Handler,
        XAPlusRetryRollbackOrderRequestEvent.Handler,
        XAPlusCommitRecoveredXidRequestEvent.Handler,
        XAPlusRollbackRecoveredXidRequestEvent.Handler,
        XAPlusForgetRecoveredXidRequestEvent.Handler,
        XAPlusReportFailedStatusRequestEvent.Handler,
        XAPlusReportReadyStatusRequestEvent.Handler,
        XAPlusRetryFromSuperiorRequestEvent.Handler,
        XAPlusRecoveryResourceRequestEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;

    XAPlusService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super(properties.getServerId() + "-xaplus", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
    }

    @Override
    public void handlePrepareBranchRequest(XAPlusPrepareBranchRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("End branch, xid={}", branchXid);
            }
            resource.end(branchXid, XAResource.TMSUCCESS);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch ended, xid={}", branchXid);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Prepare branch, xid={}", branchXid);
            }
            int vote = resource.prepare(branchXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch prepared, xid={}, voted {}", branchXid,
                        XAPlusConstantsDecoder.decodePrepareVote(vote));
            }
            dispatcher.dispatch(new XAPlusBranchPreparedEvent(xid, branchXid));
        } catch (XAException prepareException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Prepare branch failed as {}, xid={}", prepareException.getMessage(), branchXid);
            }
            dispatcher.dispatch(new XAPlusPrepareBranchFailedEvent(xid, branchXid, prepareException));
        }
    }

    @Override
    public void handleCommitBranchRequest(XAPlusCommitBranchRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Committing branch, xid={}", branchXid);
            }
            // TODO: use onePhase optimization
            resource.commit(branchXid, false);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch committed, xid={}", branchXid);
            }
            dispatcher.dispatch(new XAPlusBranchCommittedEvent(xid, branchXid));
        } catch (XAException committingException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Commit branch failed as {}, xid={}", committingException.getMessage(), branchXid);
            }
            dispatcher.dispatch(new XAPlusCommitBranchFailedEvent(xid, branchXid, committingException));
        }
    }

    @Override
    public void handleRollbackBranchRequest(XAPlusRollbackBranchRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Rolling back branch, xid={}", branchXid);
            }
            resource.rollback(branchXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch rolled back, xid={}", branchXid);
            }
            dispatcher.dispatch(new XAPlusBranchRolledBackEvent(xid, branchXid));
        } catch (XAException rollingBackException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Rollback branch failed as {}, xid={}", rollingBackException.getMessage(), branchXid);
            }
            dispatcher.dispatch(new XAPlusRollbackBranchFailedEvent(xid, branchXid, rollingBackException));
        }
    }

    @Override
    public void handleRetryCommitOrderRequest(XAPlusRetryCommitOrderRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Retrying commit order for branch, xid={}", xid);
            }
            resource.commit(xid, false);
            if (logger.isDebugEnabled()) {
                logger.debug("Retry commit order for branch completed, xid={}", xid);
            }
        } catch (XAException commitException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Retry commit order failed as {}, xid={}", commitException, xid);
            }
        }
    }

    @Override
    public void handleRetryRollbackOrderRequest(XAPlusRetryRollbackOrderRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Retrying rollback order for branch, xid={}", xid);
            }
            resource.rollback(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Retry rollback order for branch completed, xid={}", xid);
            }
        } catch (XAException rollbackException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Retry rollback order failed as {}, xid={}", rollbackException, xid);
            }
        }
    }

    @Override
    public void handleCommitRecoveredXidRequest(XAPlusCommitRecoveredXidRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusRecoveredResource recoveredResource = event.getResource();
        // Basesed on https://github.com/bitronix/btm/blob/master/btm/src/main/java/bitronix/tm/recovery/RecoveryHelper.java
        boolean success = true;
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Committing recovered xid, xid={}", xid);
            }
            recoveredResource.getXaResource().commit(xid, false);
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid committed, xid={}", xid);
            }
        } catch (XAException committingException) {
            int errorCode = committingException.errorCode;
            String description;
            if (errorCode == XAException.XAER_NOTA) {
                description = "Forgotten heuristic?";
            } else if (errorCode == XAException.XA_HEURCOM) {
                description = "Heuristic decision compatible with the global state of this transaction. " +
                        "Forget this xid";
                dispatcher.dispatch(new XAPlusForgetRecoveredXidRequestEvent(xid, recoveredResource));
            } else if (errorCode == XAException.XA_HEURHAZ || errorCode == XAException.XA_HEURMIX
                    || errorCode == XAException.XA_HEURRB) {
                description = "Heuristic decision incompatible with the global state of this transaction! " +
                        "Forget this xid";
                dispatcher.dispatch(new XAPlusForgetRecoveredXidRequestEvent(xid, recoveredResource));
                success = false;
            } else {
                description = "Unable to commit in-doubt branch";
                success = false;
            }
            if (logger.isWarnEnabled()) {
                logger.warn("Commit recovered xid failed with errorCode={}. {}, xid={}, xaResource={}",
                        XAPlusConstantsDecoder.decodeXAExceptionErrorCode(committingException), description, xid,
                        recoveredResource.getUniqueName());
            }
        }
        if (success) {
            dispatcher.dispatch(new XAPlusRecoveredXidCommittedEvent(xid));
        } else {
            dispatcher.dispatch(new XAPlusCommitRecoveredXidFailedEvent(xid));
        }
    }

    @Override
    public void handleRollbackRecoveredXidRequest(XAPlusRollbackRecoveredXidRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusRecoveredResource recoveredResource = event.getResource();
        // Basesed on https://github.com/bitronix/btm/blob/master/btm/src/main/java/bitronix/tm/recovery/RecoveryHelper.java
        boolean success = true;
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Rolling back recovered xid={}", xid);
            }
            recoveredResource.getXaResource().rollback(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid={} rolled back", xid);
            }
        } catch (XAException rollingBackException) {
            int errorCode = rollingBackException.errorCode;
            String description;
            if (errorCode == XAException.XAER_NOTA) {
                description = "Forgotten heuristic?";
            } else if (errorCode == XAException.XA_HEURRB) {
                description = "Heuristic decision compatible with the global state of this transaction. " +
                        "Forget this xid";
                dispatcher.dispatch(new XAPlusForgetRecoveredXidRequestEvent(xid, recoveredResource));
            } else if (errorCode == XAException.XA_HEURHAZ || errorCode == XAException.XA_HEURMIX ||
                    errorCode == XAException.XA_HEURCOM) {
                description = "Heuristic decision incompatible with the global state of this transaction! " +
                        "Forget this xid";
                dispatcher.dispatch(new XAPlusForgetRecoveredXidRequestEvent(xid, recoveredResource));
                success = false;
            } else {
                description = "Unable to rollback in-doubt branch";
                success = false;
            }
            if (logger.isWarnEnabled()) {
                logger.warn("Rollback recovered xid failed with errorCode={}. {}, xid={}, xaResource={}",
                        XAPlusConstantsDecoder.decodeXAExceptionErrorCode(rollingBackException), description, xid,
                        recoveredResource.getUniqueName());
            }
        }
        if (success) {
            dispatcher.dispatch(new XAPlusRecoveredXidRolledBackEvent(xid));
        } else {
            dispatcher.dispatch(new XAPlusRollbackRecoveredXidFailedEvent(xid));
        }
    }

    @Override
    public void handleForgetRecoveredXidRequest(XAPlusForgetRecoveredXidRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusRecoveredResource recoveredResource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Forgetting recovered xid, xid={}", xid);
            }
            recoveredResource.getXaResource().forget(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid forgot, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusRecoveredXidForgottenEvent(xid));
        } catch (XAException forgetException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Forget recovered xid failed as {}, xid={}", forgetException.getMessage(), xid);
            }
            dispatcher.dispatch(new XAPlusForgetRecoveredXidFailedEvent(xid, forgetException));
        }
    }

    @Override
    public void handleReportReadyStatusRequest(XAPlusReportReadyStatusRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Reporting ready status for xid, xid={}", xid);
            }
            resource.ready(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Ready status for xid reported, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusReadyStatusReportedEvent(xid));
        } catch (XAPlusException readyException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Report ready status for xid failed as {}, xid={}", readyException.getMessage(), xid);
            }
            dispatcher.dispatch(new XAPlusReportReadyStatusFailedEvent(xid, readyException));
        }
    }

    @Override
    public void handleReportFailedStatusRequest(XAPlusReportFailedStatusRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Reporting failed status for xid, xid={}", xid);
            }
            resource.failed(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Failed status for xid reported, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusFailedStatusReportedEvent(xid));
        } catch (XAPlusException readyException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Report failed status for xid failed as {}, xid={}", readyException.getMessage(), xid);
            }
            dispatcher.dispatch(new XAPlusReportFailedStatusFailedEvent(xid, readyException));
        }
    }

    @Override
    public void handleRetryFromSuperiorRequest(XAPlusRetryFromSuperiorRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusResource resource = event.getResource();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Requesting retry from superior, xid={}", xid);
            }
            resource.retry(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Retry from superior requested, xid={}", xid);
            }
        } catch (XAPlusException retryException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Request retry from superior failed as {}, xid={}",
                        retryException.getMessage(), xid);
            }
        }
    }

    @Override
    public void handleRecoveryResourceRequest(XAPlusRecoveryResourceRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusRecoveredResource recoveredResource = event.getRecoveredResource();
        if (logger.isTraceEnabled()) {
            logger.trace("Recovering resource, recoveredResource={}", recoveredResource);
        }
        try {
            int count = recoveredResource.recovery();
            if (logger.isDebugEnabled()) {
                logger.debug("{} xid(s) recovered from {} for {}",
                        count, recoveredResource.getUniqueName(), recoveredResource.getServerId());
            }
            dispatcher.dispatch(new XAPlusResourceRecoveredEvent(recoveredResource));
        } catch (XAException xae) {
            if (logger.isWarnEnabled()) {
                logger.warn("Recovery failed as {}, resource={} ", xae.getMessage(), recoveredResource);
            }
            dispatcher.dispatch(new XAPlusRecoveryResourceFailedEvent(recoveredResource, xae));
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusPrepareBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusCommitBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryCommitOrderRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryRollbackOrderRequestEvent.class);
        dispatcher.subscribe(this, XAPlusCommitRecoveredXidRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackRecoveredXidRequestEvent.class);
        dispatcher.subscribe(this, XAPlusForgetRecoveredXidRequestEvent.class);
        dispatcher.subscribe(this, XAPlusReportReadyStatusRequestEvent.class);
        dispatcher.subscribe(this, XAPlusReportFailedStatusRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryFromSuperiorRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveryResourceRequestEvent.class);
    }
}