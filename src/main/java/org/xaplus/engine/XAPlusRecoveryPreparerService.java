package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusDanglingTransactionsFoundEvent;
import org.xaplus.engine.events.journal.XAPlusFindDanglingTransactionsFailedEvent;
import org.xaplus.engine.events.journal.XAPlusFindDanglingTransactionsRequestEvent;
import org.xaplus.engine.events.recovery.*;

import javax.jms.JMSException;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.Map;

class XAPlusRecoveryPreparerService extends Bolt implements
        XAPlusRecoveryRequestEvent.Handler,
        XAPlusResourceRecoveredEvent.Handler,
        XAPlusRecoveryResourceFailedEvent.Handler,
        XAPlusDanglingTransactionsFoundEvent.Handler,
        XAPlusFindDanglingTransactionsFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryPreparerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusRecoveryPreparerTracker tracker;

    XAPlusRecoveryPreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                  XAPlusResources resources, XAPlusRecoveryPreparerTracker tracker) {
        super(properties.getServerId() + "-recovery-preparer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = tracker;
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
                logger.debug("Recovery started at {}", System.currentTimeMillis());
            }
            tracker.start();
            dispatcher.dispatch(new XAPlusFindDanglingTransactionsRequestEvent());
            // Recovery all registered resources
            Map<String, XAPlusResources.Wrapper> wrappers = resources.getResources();
            for (String uniqueName : wrappers.keySet()) {
                XAPlusResources.Wrapper wrapper = wrappers.get(uniqueName);
                try {
                    XAResource resource = null;
                    if (wrapper instanceof XAPlusResources.XADataSourceWrapper) {
                        javax.sql.XAConnection connection = ((XAPlusResources.XADataSourceWrapper) wrapper).get();
                        tracker.track(uniqueName, connection);
                        resource = connection.getXAResource();
                    } else if (wrapper instanceof XAPlusResources.XAConnectionFactoryWrapper) {
                        javax.jms.XAConnection connection =
                                ((XAPlusResources.XAConnectionFactoryWrapper) wrapper).get();
                        tracker.track(uniqueName, connection);
                        resource = connection.createXASession().getXAResource();
                    }
                    if (resource != null) {
                        dispatcher.dispatch(new XAPlusRecoveryResourceRequestEvent(uniqueName, resource));
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
            tracker.putRecoveredXids(event.getUniqueName(), event.getRecoveredXids());
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
            tracker.recoveryResourceFailed(event.getUniqueName());
            check();
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Recovery not started yet");
            }
        }
    }

    @Override
    public void handleDanglingTransactionFound(XAPlusDanglingTransactionsFoundEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (tracker.isStarted()) {
            tracker.putDanglingTransactions(event.getDanglingTransactions());
            check();
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
        if (tracker.isStarted()) {
            tracker.findDanglingTransactionsFailed();
            check();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery not started yet");
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRecoveryRequestEvent.class);
        dispatcher.subscribe(this, XAPlusResourceRecoveredEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveryResourceFailedEvent.class);
        dispatcher.subscribe(this, XAPlusDanglingTransactionsFoundEvent.class);
        dispatcher.subscribe(this, XAPlusFindDanglingTransactionsFailedEvent.class);
    }

    private void check() throws InterruptedException {
        if (tracker.isRecovered()) {
            if (tracker.isFailed()) {
                if (logger.isInfoEnabled()) {
                    logger.info("Recovery failed, close connections and reset");
                }
                tracker.close();
                tracker.reset();
            } else {
                dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(tracker.getJdbcConnections(),
                        tracker.getJmsConnections(), tracker.getXaResources(), tracker.getRecoveredXids(),
                        tracker.getDanglingTransactions()));
                tracker.reset();
            }
        }
    }
}
