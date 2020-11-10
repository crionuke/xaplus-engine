package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusCompletedTransactionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogComplettedTransactionFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportDoneStatusRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportFailedStatusRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

class XAPlusSubordinateCompleterService extends Bolt implements
        XAPlusCompletedTransactionLoggedEvent.Handler,
        XAPlusLogComplettedTransactionFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSubordinateCompleterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusSubordinateCompleterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                      XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-subordinate-completer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = tracker;
    }

    @Override
    public void handleCompletedTransactionLogged(XAPlusCompletedTransactionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate()) {
            XAPlusXid xid = transaction.getXid();
            String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
            try {
                XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                dispatcher.dispatch(new XAPlusReportDoneStatusRequestEvent(xid, resource));
                if (event.getStatus()) {
                    dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
                } else {
                    dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
                }
            } catch (XAPlusSystemException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Non XA+ or unknown resource with name={}, {}",
                            superiorServerId, transaction);
                }
            }
        }
    }

    @Override
    public void handleLogCompletedTransactionFailed(XAPlusLogComplettedTransactionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate()) {
            XAPlusXid xid = transaction.getXid();
            String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
            try {
                XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                dispatcher.dispatch(new XAPlusReportFailedStatusRequestEvent(xid, resource));
                if (event.getStatus()) {
                    dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
                } else {
                    dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
                }
            } catch (XAPlusSystemException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Non XA+ or unknown resource with name={}, {}",
                            superiorServerId, transaction);
                }
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusCompletedTransactionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusLogComplettedTransactionFailedEvent.class);
    }
}
