package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.tm.XAPlusTransactionClosedEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;

import javax.jms.JMSException;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusRecoveryPreparerService extends Bolt implements
        XAPlusRecoveryRequestEvent.Handler,
        XAPlusResourceRecoveredEvent.Handler,
        XAPlusRecoveryResourceFailedEvent.Handler,
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlusTransactionClosedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryPreparerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusRecoveryPreparerTracker tracker;
    private final long startTime;
    private final SortedSet<XAPlusTransaction> inFlightTransactions;

    XAPlusRecoveryPreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                  XAPlusResources resources, XAPlusRecoveryPreparerTracker tracker) {
        super(properties.getServerId() + "-recovery-preparer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = tracker;
        startTime = System.currentTimeMillis();
        inFlightTransactions = new TreeSet<>(Comparator
                .comparingLong((transaction) -> transaction.getCreationTimeInMillis()));
    }

    @Override
    public void handleRecoveryRequest(XAPlusRecoveryRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (tracker.isStarted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery already started");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Prepare to recovery, {}", System.currentTimeMillis());
            }
            tracker.start();
            // Recovery all registered XA resources
            Map<String, XAPlusResources.Wrapper> wrappers = resources.getResources();
            for (String uniqueName : wrappers.keySet()) {
                XAPlusResources.Wrapper wrapper = wrappers.get(uniqueName);
                try {
                    XAPlusRecoveredResource recoveredResource = null;
                    if (wrapper instanceof XAPlusResources.XADataSourceWrapper) {
                        javax.sql.XAConnection connection = ((XAPlusResources.XADataSourceWrapper) wrapper).get();
                        recoveredResource = new XAPlusRecoveredResource(uniqueName, getInFlightCutoff(), connection);
                    } else if (wrapper instanceof XAPlusResources.XAConnectionFactoryWrapper) {
                        javax.jms.XAJMSContext context =
                                ((XAPlusResources.XAConnectionFactoryWrapper) wrapper).get();
                        recoveredResource = new XAPlusRecoveredResource(uniqueName, getInFlightCutoff(), context);
                    }
                    if (recoveredResource != null) {
                        tracker.track(recoveredResource);
                        dispatcher.dispatch(new XAPlusRecoveryResourceRequestEvent(recoveredResource));
                    }
                } catch (SQLException | JMSException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Recovery xaResource failed with {}, uniqueName={}", e.getMessage(), uniqueName);
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
        if (tracker.isStarted()) {
            tracker.resourceRecovered(event.getRecoveredResource());
            check();
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Recovery not started yet");
            }
        }
    }

    @Override
    public void handleRecoveryResourceFailed(XAPlusRecoveryResourceFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (tracker.isStarted()) {
            tracker.resourceFailed(event.getRecoveredResource());
            check();
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Recovery not started yet");
            }
        }
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        inFlightTransactions.add(transaction);
        if (logger.isTraceEnabled()) {
            logger.trace("Transaction added to in-flight list, {}", transaction);
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        inFlightTransactions.add(transaction);
        if (logger.isTraceEnabled()) {
            logger.trace("Transaction added to in-flight list, {}", transaction);
        }
    }

    @Override
    public void handleTransactionClosed(XAPlusTransactionClosedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.remove(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed from in-flight list, {}", transaction);
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRecoveryRequestEvent.class);
        dispatcher.subscribe(this, XAPlusResourceRecoveredEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveryResourceFailedEvent.class);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionClosedEvent.class);
    }

    private void check() throws InterruptedException {
        if (tracker.isRecoveryFinished()) {
            dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(tracker.getRecoveredResources()));
            tracker.reset();
        }
    }

    private long getInFlightCutoff() {
        if (inFlightTransactions.isEmpty()) {
            // If no transaction yet use app start time
            return startTime;
        } else {
            return inFlightTransactions.first().getCreationTimeInMillis();
        }
    }
}
