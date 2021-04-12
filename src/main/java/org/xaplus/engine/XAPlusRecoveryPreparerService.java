package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.*;

import javax.jms.JMSException;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusRecoveryPreparerService extends Bolt implements
        XAPlusPrepareRecoveryRequestEvent.Handler,
        XAPlusResourceRecoveredEvent.Handler,
        XAPlusRecoveryResourceFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryPreparerService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusRecoveryPreparerTracker tracker;

    XAPlusRecoveryPreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                  XAPlusResources resources) {
        super(properties.getServerId() + "-recovery-preparer", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = new XAPlusRecoveryPreparerTracker();
    }

    @Override
    public void handlePrepareRecoveryRequest(XAPlusPrepareRecoveryRequestEvent event) throws InterruptedException {
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
                        recoveredResource = new XAPlusRecoveredResource(uniqueName, properties.getServerId(),
                                event.getInFlightCutoff(), connection);
                    } else if (wrapper instanceof XAPlusResources.XAConnectionFactoryWrapper) {
                        javax.jms.XAJMSContext context =
                                ((XAPlusResources.XAConnectionFactoryWrapper) wrapper).get();
                        recoveredResource = new XAPlusRecoveredResource(uniqueName, properties.getServerId(),
                                event.getInFlightCutoff(), context);
                    }
                    if (recoveredResource != null) {
                        tracker.track(recoveredResource);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Request recovery for resource, uniqueName={}",
                                    recoveredResource.getUniqueName());
                        }
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
            XAPlusRecoveredResource recoveredResource = event.getRecoveredResource();
            if (logger.isDebugEnabled()) {
                logger.debug("Resource recovered, uniqueName={}", recoveredResource.getUniqueName());
            }
            tracker.resourceRecovered(recoveredResource);
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
            XAPlusRecoveredResource recoveredResource = event.getRecoveredResource();
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery resource failed, uniqueName={}", recoveredResource.getUniqueName());
            }
            tracker.resourceFailed(recoveredResource);
            check();
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Recovery not started yet");
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusPrepareRecoveryRequestEvent.class);
        dispatcher.subscribe(this, XAPlusResourceRecoveredEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveryResourceFailedEvent.class);
    }

    private void check() throws InterruptedException {
        if (tracker.isRecoveryPrepared()) {
            dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(tracker.getRecoveredResources()));
            tracker.reset();
        }
    }
}
