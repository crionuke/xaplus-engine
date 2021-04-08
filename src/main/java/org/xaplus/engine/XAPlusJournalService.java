package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.*;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateRetryRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryCommitOrderRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryRollbackOrderRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

import java.sql.SQLException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusJournalService extends Bolt implements
        XAPlusLogCommitTransactionDecisionEvent.Handler,
        XAPlusLogRollbackTransactionDecisionEvent.Handler,
        XAPlusFindRecoveredXidStatusRequestEvent.Handler,
        XAPlusRemoteSubordinateRetryRequestEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTLog tlog;

    XAPlusJournalService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                         XAPlusResources resources, XAPlusTLog tlog) {
        super(properties.getServerId() + "-journal", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tlog = tlog;
    }

    @Override
    public void handleLogCommitTransactionDecision(XAPlusLogCommitTransactionDecisionEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        try {
            tlog.logCommitDecision(xid.getGlobalTransactionIdUid());
            if (logger.isDebugEnabled()) {
                logger.debug("Commit decision logged, xid={}, branches={}", xid, transaction.getBranches());
            }
            dispatcher.dispatch(new XAPlusCommitTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit decision failed as {}, xid={}, branches={}",
                        sqle.getMessage(), xid, transaction.getBranches());
            }
            dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionFailedEvent(transaction, sqle));
        }
    }

    @Override
    public void handleLogRollbackTransactionDecision(XAPlusLogRollbackTransactionDecisionEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        try {
            tlog.logRollbackDecision(xid.getGlobalTransactionIdUid());
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback decision logged, xid={}, branches={}", xid, transaction.getBranches());
            }
            dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback decision failed as {}, xid={}", sqle.getMessage(), xid);
            }
            dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionFailedEvent(transaction, sqle));
        }
    }

    @Override
    public void handleFindRecoveredXidStatusRequest(XAPlusFindRecoveredXidStatusRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusRecoveredResource recoveredResource = event.getRecoveredResource();
        try {
            boolean status = tlog.findTransactionStatus(xid.getGlobalTransactionIdUid());
            dispatcher.dispatch(new XAPlusRecoveredXidStatusFoundEvent(xid, recoveredResource, status));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Find transaction status request failed as {}, xid={}", sqle.getMessage(), xid);
            }
            dispatcher.dispatch(new XAPlusFindRecoveredXidStatusFailedEvent(xid, recoveredResource, sqle));
        }
    }

    @Override
    public void handleRemoteSubordinateRetryRequest(XAPlusRemoteSubordinateRetryRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        try {
            boolean status = tlog.findTransactionStatus(xid.getGlobalTransactionIdUid());
            String subordinateServerId = xid.getBranchQualifierUid().extractServerId();
            try {
                XAPlusResource resource = resources.getXAPlusResource(subordinateServerId);
                if (status) {
                    dispatcher.dispatch(new XAPlusRetryCommitOrderRequestEvent(xid, resource));
                } else {
                    dispatcher.dispatch(new XAPlusRetryRollbackOrderRequestEvent(xid, resource));
                }
            } catch (XAPlusSystemException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Non XA+ or unknown resource with name={}, xid={}", subordinateServerId, xid);
                }
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Recovery transaction status failed as {}, xid={}", sqle.getMessage(), xid);
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusFindRecoveredXidStatusRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateRetryRequestEvent.class);
    }
}
