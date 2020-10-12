package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.XAPlusTimeoutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import javax.annotation.PostConstruct;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusManagerService extends Bolt implements
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTracker tracker;

    XAPlusManagerService(XAPlusProperties XAPlusProperties, XAPlusThreadPool threadPool,
                         XAPlusDispatcher dispatcher) {
        super("manager", XAPlusProperties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        tracker = new XAPlusTracker();
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction with xid={} done", transaction.getXid());
            }
            transaction.getFuture().put(new XAPlusResult());
        }
    }

    @Override
    public void handleRollbackDone(XAPlusRollbackDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction with xid={} rolled back", transaction.getXid());
            }
            transaction.getFuture().put(new XAPlusResult());
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        Exception exception = event.getException();
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("2pc of transaction with xid={} failed as {}",
                        transaction.getXid(), exception.getMessage());
            }
            transaction.getFuture().put(new XAPlusResult(new XAPlusCommitException(exception)));
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        Exception exception = event.getException();
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback of transaction with xid={} failed as {}",
                        transaction.getXid(), exception.getMessage());
            }
            transaction.getFuture().put(new XAPlusResult(new XAPlusRollbackException(exception)));
        }
    }

    @Override
    public void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        XAPlusXid xid = event.getTransaction().getXid();
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction with xid={} timed out", xid);
            }
            transaction.getFuture().put(new XAPlusResult(new XAPlusTimeoutException()));
        }
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
    }
}

