package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.xaplus.engine.events.journal.XAPlusFindRecoveredXidStatusFailedEvent;
import org.xaplus.engine.events.journal.XAPlusFindRecoveredXidStatusRequestEvent;
import org.xaplus.engine.events.journal.XAPlusRecoveredXidStatusFoundEvent;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryFromSuperiorRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusRecoveryCommitterService extends Bolt implements
        XAPlusRecoveryPreparedEvent.Handler,
        XAPlusRecoveredXidStatusFoundEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler,
        XAPlusFindRecoveredXidStatusFailedEvent.Handler,
        XAPlusRecoveredXidCommittedEvent.Handler,
        XAPlusCommitRecoveredXidFailedEvent.Handler,
        XAPlusRecoveredXidRolledBackEvent.Handler,
        XAPlusRollbackRecoveredXidFailedEvent.Handler,
        XAPlusTickEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryCommitterService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusRecoveryCommitterTracker tracker;

    XAPlusRecoveryCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                   XAPlusDispatcher dispatcher, XAPlusResources resources) {
        super(properties.getServerId() + "-recovery-committer", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = new XAPlusRecoveryCommitterTracker(properties.getRecoveryTimeoutInSeconds());
    }

    @Override
    public void handleRecoveryPrepared(XAPlusRecoveryPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        if (tracker.isStarted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery already started");
            }
        } else {
            Set<XAPlusRecoveredResource> recoveredResources = event.getRecoveredResources();
            if (logger.isDebugEnabled()) {
                logger.debug("Commit recovery for {} resource/s, {}",
                        recoveredResources.size(), System.currentTimeMillis());
            }
            tracker.start(recoveredResources);
            for (XAPlusRecoveredResource recoveredResource : recoveredResources) {
                for (XAPlusXid recoveredXid : recoveredResource.getRecoveredXids()) {
                    String superiorServerId = recoveredXid.getGtrid().getServerId();
                    tracker.track(recoveredResource, recoveredXid);
                    if (superiorServerId.equals(properties.getServerId())) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Recovered transaction status from tlog, xid={}", recoveredXid);
                        }
                        dispatcher.dispatch(
                                new XAPlusFindRecoveredXidStatusRequestEvent(recoveredXid, recoveredResource));
                    } else {
                        try {
                            XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Request transaction status from superior, superiorServerId={}, xid={}",
                                        superiorServerId, recoveredXid);
                            }
                            dispatcher.dispatch(new XAPlusRetryFromSuperiorRequestEvent(recoveredXid, resource));
                        } catch (XAPlusSystemException e) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("Non XA+ or unknown resource with name={}, {}",
                                        superiorServerId, e.getMessage());
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void handleRecoveredXidStatusFound(XAPlusRecoveredXidStatusFoundEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.statusFound(xid)) {
            XAPlusRecoveredResource recoveredResource = event.getRecoveredResource();
            if (event.getStatus()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Status found, commit recovered xid, xid={}", xid);
                }
                dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(xid, recoveredResource));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Status found, rollback recovered xid, xid={}", xid);
                }
                dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(xid, recoveredResource));
            }
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToCommit(XAPlusRemoteSuperiorOrderToCommitEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.statusFound(xid)) {
            XAPlusRecoveredResource recoveredResource = tracker.getRecoveredResourceFor(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Order to commit, commit recovered xid, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(xid, recoveredResource));
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToRollback(XAPlusRemoteSuperiorOrderToRollbackEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.statusFound(xid)) {
            XAPlusRecoveredResource recoveredResource = tracker.getRecoveredResourceFor(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Order to rollback, rollback recovered xid, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(xid, recoveredResource));
        }
    }

    @Override
    public void handleFindRecoveredXidStatusFailed(XAPlusFindRecoveredXidStatusFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.findStatusFailed(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Find status failed, xid={}", xid);
            }
            check();
        }
    }

    @Override
    public void handleRecoveredXidCommitted(XAPlusRecoveredXidCommittedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.xidRecovered(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid committed, xid={}", xid);
            }
            check();
        }
    }

    @Override
    public void handleCommitRecoveredXidFailed(XAPlusCommitRecoveredXidFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.xidRecovered(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Commit recovered xid failed, xid={}", xid);
            }
            check();
        }
    }

    @Override
    public void handleRecoveredXidRolledBack(XAPlusRecoveredXidRolledBackEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.xidRecovered(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovered xid rolled back, xid={}", xid);
            }
            check();
        }
    }

    @Override
    public void handleRollbackRecoveredXidFailed(XAPlusRollbackRecoveredXidFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.xidRecovered(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback recovered xid failed, xid={}", xid);
            }
            check();
        }
    }

    @Override
    public void handleTick(XAPlusTickEvent event) throws InterruptedException {
        if (tracker.isExpired()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery timed out, {}", System.currentTimeMillis());
                tracker.reset();
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRecoveryPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveredXidStatusFoundEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToCommitEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
        dispatcher.subscribe(this, XAPlusFindRecoveredXidStatusFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveredXidCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusCommitRecoveredXidFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveredXidRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackRecoveredXidFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTickEvent.class);
    }

    private void check() throws InterruptedException {
        if (tracker.isRecoveryCommitted()) {
            dispatcher.dispatch(new XAPlusRecoveryFinishedEvent(tracker.getFinishedXids()));
            tracker.reset();
        }
    }
}
