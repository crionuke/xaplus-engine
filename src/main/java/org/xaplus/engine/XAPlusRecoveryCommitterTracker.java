package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusRecoveryCommitterTracker {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryCommitterTracker.class);

    private final long recoveryTimeoutInSeconds;

    private boolean started;
    private long expireTimeInMillis;
    private Set<XAPlusRecoveredResource> recoveredResources;
    private Map<XAPlusXid, XAPlusRecoveredResource> resourceByXid;
    private Set<XAPlusXid> waitingXids;
    private Set<XAPlusXid> finishedXids;

    XAPlusRecoveryCommitterTracker(long recoveryTimeoutInSeconds) {
        this.recoveryTimeoutInSeconds = recoveryTimeoutInSeconds;

        started = false;
        expireTimeInMillis = 0;
        recoveredResources = new HashSet<>();
        resourceByXid = new HashMap<>();
        waitingXids = new HashSet<>();
        finishedXids = new HashSet<>();
    }

    Set<XAPlusRecoveredResource> getRecoveredResources() {
        return recoveredResources;
    }

    void start(Set<XAPlusRecoveredResource> resources) {
        recoveredResources.addAll(resources);
        started = true;
        expireTimeInMillis = System.currentTimeMillis() + recoveryTimeoutInSeconds * 1000;
    }

    boolean isStarted() {
        return started;
    }

    boolean isExpired() {
        return started && System.currentTimeMillis() > expireTimeInMillis;
    }

    void track(XAPlusRecoveredResource recoveredResource, XAPlusXid xid) {
        resourceByXid.put(xid, recoveredResource);
        waitingXids.add(xid);
    }

    XAPlusRecoveredResource getRecoveredResourceFor(XAPlusXid xid) {
        return resourceByXid.get(xid);
    }

    boolean statusFound(XAPlusXid xid) {
        return waitingXids.contains(xid);
    }

    boolean findStatusFailed(XAPlusXid xid) {
        return waitingXids.remove(xid);
    }

    boolean xidRecovered(XAPlusXid xid) {
        if (waitingXids.remove(xid)) {
            finishedXids.add(xid);
            return true;
        } else {
            return false;
        }
    }

    boolean isRecoveryCommitted() {
        return waitingXids.isEmpty();
    }

    public Set<XAPlusXid> getFinishedXids() {
        return finishedXids;
    }

    void reset() {
        started = false;
        // Close all resources
        for (XAPlusRecoveredResource recoveredResource : recoveredResources) {
            recoveredResource.close();
        }
        recoveredResources.clear();
        resourceByXid.clear();
        waitingXids.clear();
        finishedXids.clear();
    }
}
