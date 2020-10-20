package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.journal.XAPlusDanglingTransactionsFoundEvent;
import org.xaplus.engine.events.journal.XAPlusFindDanglingTransactionsFailedEvent;
import org.xaplus.engine.events.journal.XAPlusFindDanglingTransactionsRequestEvent;
import org.xaplus.engine.events.recovery.*;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.Map;

@Service
class XAPlusRecoveryPreparerService extends Bolt implements
        XAPlusRecoveryRequestEvent.Handler,
        XAPlusResourceRecoveredEvent.Handler,
        XAPlusRecoveryResourceFailedEvent.Handler,
        XAPlusDanglingTransactionsFoundEvent.Handler,
        XAPlusFindDanglingTransactionsFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryPreparerService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusRecoveryPreparerState state;

    XAPlusRecoveryPreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                  XAPlusResources resources) {
        super("recovery-preparer", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        state = new XAPlusRecoveryPreparerState();
    }

    @Override
    public void handleRecoveryRequest(XAPlusRecoveryRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (state.isStarted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery already started");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery started at {}", System.currentTimeMillis());
            }
            state.start();
            dispatcher.dispatch(new XAPlusFindDanglingTransactionsRequestEvent());
            // Recovery all registered resources
            Map<String, XAPlusResources.Wrapper> wrappers = resources.getResources();
            for (String uniqueName : wrappers.keySet()) {
                XAPlusResources.Wrapper wrapper = wrappers.get(uniqueName);
                try {
                    XAResource resource = null;
                    if (wrapper instanceof XAPlusResources.XADataSourceWrapper) {
                        javax.sql.XAConnection connection = ((XAPlusResources.XADataSourceWrapper) wrapper).get();
                        state.track(uniqueName, connection);
                        resource = connection.getXAResource();
                    } else if (wrapper instanceof XAPlusResources.XAConnectionFactoryWrapper) {
                        javax.jms.XAConnection connection =
                                ((XAPlusResources.XAConnectionFactoryWrapper) wrapper).get();
                        state.track(uniqueName, connection);
                        resource = connection.createXASession().getXAResource();
                    }
                    if (resource != null) {
                        dispatcher.dispatch(new XAPlusRecoveryResourceRequestEvent(uniqueName, resource));
                    }
                } catch (SQLException | JMSException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Recovery resource failed with {}, resource={}", e.getMessage(), uniqueName);
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
        if (state.isStarted()) {
            state.putRecoveredXids(event.getUniqueName(), event.getRecoveredXids());
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
        if (state.isStarted()) {
            state.recoveryResourceFailed(event.getUniqueName());
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
        if (state.isStarted()) {
            state.putDanglingTransactions(event.getDanglingTransactions());
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
        if (state.isStarted()) {
            state.findDanglingTransactionsFailed();
            checkTracker();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery not started yet");
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
    }

    private void checkTracker() throws InterruptedException {
        if (state.isRecovered()) {
            if (state.isFailed()) {
                if (logger.isInfoEnabled()) {
                    logger.info("Recovery failed, close connections and reset state");
                }
                state.close();
                state.reset();
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Ready to recovery, starting");
                }
                dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(state.getJdbcConnections(),
                        state.getJmsConnections(), state.getXaResources(), state.getRecoveredXids(),
                        state.getDanglingTransactions()));
                state.reset();
            }
        }
    }
}
