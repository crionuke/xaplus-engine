package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.xaplus.engine.events.recovery.XAPlusPrepareRecoveryRequestEvent;
import org.xaplus.engine.events.recovery.XAPlusUserRecoveryRequestEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionClosedEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.util.*;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusManagerService extends Bolt implements
        XAPlusUserCreateTransactionEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusUserRecoveryRequestEvent.Handler,
        XAPlusTickEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final SortedSet<XAPlusTransaction> inFlightTransactions;
    private long lastCutoff;
    private long lastRecoveryTime;

    XAPlusManagerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super(properties.getServerId() + "-manager", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        inFlightTransactions = new TreeSet<>(Comparator
                .comparingLong((transaction) -> transaction.getCreationTimeInMillis()));
        lastCutoff = System.currentTimeMillis();
        lastRecoveryTime = System.currentTimeMillis();
    }

    @Override
    public void handleUserCreateTransaction(XAPlusUserCreateTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.add(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("User create transaction, {}", transaction);
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
            updateCutoff();
            close(transaction);
            transaction.getFuture().putResult(new XAPlusResult(true));
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
            updateCutoff();
            close(transaction);
            transaction.getFuture().putResult(new XAPlusResult(new XAPlusCommitException("2pc commit exception")));
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
            updateCutoff();
            close(transaction);
            transaction.getFuture().putResult(new XAPlusResult(false));
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
            updateCutoff();
            close(transaction);
            transaction.getFuture().putResult(new XAPlusResult(new XAPlusRollbackException("rollback exception")));
        }
    }

    @Override
    public void handleUserRecoveryRequest(XAPlusUserRecoveryRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("User request recovery");
        }
        startRecovery();
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
                transaction.getFuture().putResult(new XAPlusResult(new XAPlusTimeoutException("timeout exception")));
                inFlightTransactions.remove(transaction);
                close(transaction);
            }
            updateCutoff();
        }
        if (properties.getRecoveryPeriodInSeconds() > 0) {
            if (time > lastRecoveryTime + properties.getRecoveryPeriodInSeconds() * 1000) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Recovery started by timer");
                }
                startRecovery();
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusUserCreateTransactionEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusUserRecoveryRequestEvent.class);
        dispatcher.subscribe(this, XAPlusTickEvent.class);
    }

    private void close(XAPlusTransaction transaction) throws InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug("Transaction closed, {}", transaction);
        }
        transaction.close();
        dispatcher.dispatch(new XAPlusTransactionClosedEvent(transaction));
    }

    private void updateCutoff() {
        if (inFlightTransactions.isEmpty()) {
            lastCutoff = System.currentTimeMillis();
        } else {
            lastCutoff = inFlightTransactions.first().getCreationTimeInMillis();
        }
    }

    private void startRecovery() throws InterruptedException {
        lastRecoveryTime = System.currentTimeMillis();
        dispatcher.dispatch(new XAPlusPrepareRecoveryRequestEvent(lastCutoff));
    }
}

