package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.*;

import javax.annotation.PostConstruct;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusService extends Bolt implements
        XAPlusPrepareBranchRequestEvent.Handler,
        XAPlusCommitBranchRequestEvent.Handler,
        XAPlusRetryCommitBranchRequestEvent.Handler,
        XAPlusRetryRollbackBranchRequestEvent.Handler,
        XAPlusRollbackBranchRequestEvent.Handler,
        XAPlusRetryCommitOrderRequestEvent.Handler,
        XAPlusRetryRollbackOrderRequestEvent.Handler,
        XAPlusCommitRecoveredXidRequestEvent.Handler,
        XAPlusRollbackRecoveredXidRequestEvent.Handler,
        XAPlusForgetRecoveredXidRequestEvent.Handler,
        XAPlusReportReadyStatusRequestEvent.Handler,
        XAPlusRecoveryResourceRequestEvent.Handler,
        XAPlusReportDoneStatusRequestEvent.Handler,
        XAPlusRetryFromSuperiorRequestEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;

    XAPlusService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super("xaplus", properties.getQueueSize());
        this.properties = properties;
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
                logger.trace("End branch with xid={}", branchXid);
            }
            resource.end(branchXid, XAResource.TMSUCCESS);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch with xid={} ended", branchXid);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Prepare branch with xid={}", branchXid);
            }
            int vote = resource.prepare(branchXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch with xid={} prepared, voted {}", branchXid,
                        XAPlusConstantsDecoder.decodePrepareVote(vote));
            }
            if (vote != XAResource.XA_RDONLY) {
                dispatcher.dispatch(new XAPlusBranchPreparedEvent(xid, branchXid));
            } else {
                dispatcher.dispatch(new XAPlusBranchReadOnlyEvent(xid, branchXid));
            }
        } catch (XAException prepareException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Prepare branch with xid={} failed with {}", branchXid, prepareException.getMessage());
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
                logger.trace("Committing branch with xid={}", branchXid);
            }
            // TODO: use onePhase optimization
            resource.commit(branchXid, false);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch with xid={} committed", branchXid);
            }
            dispatcher.dispatch(new XAPlusBranchCommittedEvent(xid, branchXid));
        } catch (XAException committingException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Commit branch with xid={} failed with {}", branchXid, committingException.getMessage());
            }
            dispatcher.dispatch(new XAPlusCommitBranchFailedEvent(xid, branchXid, committingException));
        }
    }

    @Override
    public void handleRetryCommitBranchRequest(XAPlusRetryCommitBranchRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getBranchXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Retrying commit branch with xid={}", branchXid);
            }
            resource.commit(branchXid, false);
            if (logger.isDebugEnabled()) {
                logger.debug("Retry commit branch with xid={} completed", branchXid);
            }
        } catch (XAException commitException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Retry commit branch with xid={} failed with {}", branchXid, commitException);
            }
        }
    }

    @Override
    public void handleRetryRollbackBranchRequest(XAPlusRetryRollbackBranchRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getBranchXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Retrying rollback branch with xid={}", branchXid);
            }
            resource.rollback(branchXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Retry rollback branch with xid={} completed", branchXid);
            }
        } catch (XAException rollbackException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Retry rollback branch with xid={} failed with {}", branchXid, rollbackException);
            }
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
                logger.trace("Rolling back branch with xid={}", branchXid);
            }
            resource.rollback(branchXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch with xid={} rolled back", branchXid);
            }
            dispatcher.dispatch(new XAPlusBranchRolledBackEvent(xid, branchXid));
        } catch (XAException rollingBackException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Rollback branch with xid={} failed with {}", branchXid, rollingBackException.getMessage());
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
                logger.trace("Retrying commit order for branch with xid={}", xid);
            }
            resource.commit(xid, false);
            if (logger.isDebugEnabled()) {
                logger.debug("Retry commit order for branch with xid={} completed", xid);
            }
        } catch (XAException commitException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Retry commit order failed with {}", commitException);
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
                logger.trace("Retrying rollback order for branch with xid={}", xid);
            }
            resource.rollback(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Retry rollback order for branch with xid={} completed", xid);
            }
        } catch (XAException rollbackException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Retry rollback order failed with {}", rollbackException);
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
        XAResource resource = event.getResource();
        String uniqueName = event.getUniqueName();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Committing recovered xid={}", xid);
            }
            resource.commit(xid, false);
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid={} committed", xid);
            }
            dispatcher.dispatch(new XAPlusRecoveredXidCommittedEvent(xid, uniqueName));
            // TODO: forget only in some properly cases
            dispatcher.dispatch(new XAPlusForgetRecoveredXidRequestEvent(xid, resource));
        } catch (XAException committingException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Commit recovered xid={} failed with {} ", xid, committingException.getMessage());
            }
            // TODO: handle error codes properly like - bitronix.tm.retry.RecoveryHelper.commit()
            dispatcher.dispatch(new XAPlusCommitRecoveredXidFailedEvent(xid, committingException));
        }
    }

    @Override
    public void handleRollbackRecoveredXidRequest(XAPlusRollbackRecoveredXidRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAResource resource = event.getResource();
        String uniqueName = event.getUniqueName();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Rolling back recovered xid={}", xid);
            }
            resource.rollback(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid={} rolled back", xid);
            }
            dispatcher.dispatch(new XAPlusRecoveredXidRolledBackEvent(xid, uniqueName));
            // TODO: forget only in some properly cases
            dispatcher.dispatch(new XAPlusForgetRecoveredXidRequestEvent(xid, resource));
        } catch (XAException rollingBackException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Rollback recovered xid={} failed with {} ", xid, rollingBackException.getMessage());
            }
            // TODO: handle error codes properly like - bitronix.tm.retry.RecoveryHelper.rollback()
            dispatcher.dispatch(new XAPlusRollbackRecoveredXidFailedEvent(xid, rollingBackException));
        }
    }

    @Override
    public void handleForgetRecoveredXidRequest(XAPlusForgetRecoveredXidRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Forgetting recovered xid={}", xid);
            }
            resource.forget(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid={} forgot", xid);
            }
            dispatcher.dispatch(new XAPlusRecoveredXidForgottenEvent(xid));
        } catch (XAException forgetException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Forget recovered xid={} failed with {} ", xid, forgetException.getMessage());
            }
            dispatcher.dispatch(new XAPlusForgetRecoveredXidFailedEvent(xid, forgetException));
        }
    }

    @Override
    public void handleReportDoneStatusRequest(XAPlusReportDoneStatusRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusResource resource = event.getResource();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Reporting done status for xid={}", xid);
            }
            resource.done(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Done status for xid={} reported", xid);
            }
            dispatcher.dispatch(new XAPlusDoneStatusReportedEvent(xid));
        } catch (XAPlusException doneException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Report done status for xid={} failed with {} ", xid, doneException.getMessage());
            }
            dispatcher.dispatch(new XAPlusReportDoneStatusFailedEvent(xid, doneException));
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
                logger.trace("Reporting ready status for xid={}", xid);
            }
            resource.ready(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Ready status for xid={} reported", xid);
            }
            dispatcher.dispatch(new XAPlusReadyStatusReportedEvent(xid));
        } catch (XAPlusException readyException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Report ready status for xid={} failed with {} ", xid, readyException.getMessage());
            }
            dispatcher.dispatch(new XAPlusReportReadyStatusFailedEvent(xid, readyException));
        }
    }

    @Override
    public void handleRecoveryResourceRequest(XAPlusRecoveryResourceRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        String uniqueName = event.getUniqueName();
        XAResource resource = event.getResource();
        if (logger.isTraceEnabled()) {
            logger.trace("Recovering resource with uniqueName={}", uniqueName);
        }
        XAPlusResourceRecovery resourceRecovery = new XAPlusResourceRecovery(properties.getServerId(), resource);
        try {
            Set<XAPlusXid> xids = resourceRecovery.recovery();
            if (logger.isDebugEnabled()) {
                logger.debug("{} xid(s) recovered from {}", xids.size(), uniqueName);
            }
            dispatcher.dispatch(new XAPlusResourceRecoveredEvent(uniqueName, xids));
        } catch (XAException xae) {
            if (logger.isWarnEnabled()) {
                logger.warn("Recovery resource with uniqueName={} failed with {} ", uniqueName, xae.getMessage());
            }
            dispatcher.dispatch(new XAPlusRecoveryResourceFailedEvent(uniqueName, xae));
        }
    }

    @Override
    public void handleRetryFromSuperiorRequest(XAPlusRetryFromSuperiorRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusResource resource = event.getResource();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Requesting retry from superior");
            }
            resource.retry(properties.getServerId());
            if (logger.isDebugEnabled()) {
                logger.debug("Retry from superior requested");
            }
        } catch (XAPlusException retryException) {
            if (logger.isWarnEnabled()) {
                logger.warn("Request retry from superior failed with {}", retryException.getMessage());
            }
        }
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusPrepareBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusCommitBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryCommitBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryRollbackBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackBranchRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryCommitOrderRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryRollbackOrderRequestEvent.class);
        dispatcher.subscribe(this, XAPlusCommitRecoveredXidRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackRecoveredXidRequestEvent.class);
        dispatcher.subscribe(this, XAPlusForgetRecoveredXidRequestEvent.class);
        dispatcher.subscribe(this, XAPlusReportReadyStatusRequestEvent.class);
        dispatcher.subscribe(this, XAPlusReportDoneStatusRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveryResourceRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRetryFromSuperiorRequestEvent.class);
    }
}