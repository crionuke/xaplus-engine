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

    private boolean started;
    private Set<XAPlusRecoveredResource> recoveredResources;
    private Map<XAPlusXid, XAPlusRecoveredResource> resourceByXid;
    private Set<XAPlusXid> waiting;

    XAPlusRecoveryCommitterTracker() {
        started = false;
        recoveredResources = new HashSet<>();
        resourceByXid = new HashMap<>();
        waiting = new HashSet<>();
    }

    Set<XAPlusRecoveredResource> getRecoveredResources() {
        return recoveredResources;
    }

    void start(Set<XAPlusRecoveredResource> resources) {
        recoveredResources.addAll(resources);
        started = true;
    }

    boolean isStarted() {
        return started;
    }

    void track(XAPlusRecoveredResource recoveredResource, XAPlusXid xid) {
        resourceByXid.put(xid, recoveredResource);
        waiting.add(xid);
    }

    XAPlusRecoveredResource getRecoveredResourceFor(XAPlusXid xid) {
        return resourceByXid.get(xid);
    }

    boolean statusFound(XAPlusXid xid) {
        return waiting.contains(xid);
    }

    boolean findStatusFailed(XAPlusXid xid) {
        return waiting.remove(xid);
    }

    boolean xidRecovered(XAPlusXid xid) {
        return waiting.remove(xid);
    }

    boolean isRecoveryCommitted() {
        return waiting.isEmpty();
    }

    void reset() {
        started = false;
        recoveredResources.clear();
        resourceByXid.clear();
        waiting.clear();
    }
}
