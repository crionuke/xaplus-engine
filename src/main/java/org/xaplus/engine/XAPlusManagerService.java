package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.xaplus.engine.events.recovery.XAPlusPrepareRecoveryRequestEvent;
import org.xaplus.engine.events.recovery.XAPlusStartRecoveryRequestEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionClosedEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.util.*;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusManagerService extends Bolt implements
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusStartRecoveryRequestEvent.Handler,
        XAPlusTickEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final long startTime;
    private final SortedSet<XAPlusTransaction> inFlightTransactions;

    XAPlusManagerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super(properties.getServerId() + "-manager", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        startTime = System.currentTimeMillis();
        inFlightTransactions = new TreeSet<>(Comparator
                .comparingLong((transaction) -> transaction.getCreationTimeInMillis()));
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.add(transaction)) {
            if (logger.isInfoEnabled()) {
                logger.info("User start 2pc protocol, {}", transaction);
            }
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.add(transaction)) {
            if (logger.isInfoEnabled()) {
                logger.info("User start rollback protocol, {}", transaction);
            }
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.remove(transaction)) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction done, {}", transaction);
            }
            close(transaction);
            transaction.getFuture().put(new XAPlusResult(true));
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.remove(transaction)) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction 2pc failed, {}", transaction);
            }
            close(transaction);
            transaction.getFuture().put(new XAPlusResult(new XAPlusCommitException("2pc commit exception")));
        }
    }

    @Override
    public void handleRollbackDone(XAPlusRollbackDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.remove(transaction)) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction rolled back, {}", transaction);
            }
            close(transaction);
            transaction.getFuture().put(new XAPlusResult(false));
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.remove(transaction)) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction rollback failed, {}", transaction);
            }
            close(transaction);
            transaction.getFuture().put(new XAPlusResult(new XAPlusRollbackException("rollback exception")));
        }
    }

    @Override
    public void handleStartRecoveryRequest(XAPlusStartRecoveryRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        long inFlightCutoff;
        if (inFlightTransactions.isEmpty()) {
            // If no transaction yet use app start time
            inFlightCutoff = startTime;
        } else {
            inFlightCutoff = inFlightTransactions.first().getCreationTimeInMillis();
        }
        dispatcher.dispatch(new XAPlusPrepareRecoveryRequestEvent(inFlightCutoff));
    }

    @Override
    public void handleTick(XAPlusTickEvent event) throws InterruptedException {
        long time = System.currentTimeMillis();
        Set<XAPlusTransaction> expiredTransactions = new HashSet<>();
        for (XAPlusTransaction transaction : inFlightTransactions) {
            if (time > transaction.getExpireTimeInMillis()) {
                expiredTransactions.add(transaction);
            }
        }
        if (expiredTransactions.size() > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Found {} expired transactions", expiredTransactions.size());
            }
            for (XAPlusTransaction transaction : expiredTransactions) {
                dispatcher.dispatch(new XAPlusTransactionTimedOutEvent(transaction));
                transaction.getFuture().put(new XAPlusResult(new XAPlusTimeoutException("timeout exception")));
                inFlightTransactions.remove(transaction);
                close(transaction);
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusStartRecoveryRequestEvent.class);
        dispatcher.subscribe(this, XAPlusTickEvent.class);
    }

    private void close(XAPlusTransaction transaction) throws InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug("Transaction closed, {}", transaction);
        }
        transaction.close();
        dispatcher.dispatch(new XAPlusTransactionClosedEvent(transaction));
    }
}

