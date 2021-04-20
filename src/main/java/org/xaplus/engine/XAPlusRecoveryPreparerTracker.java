package org.xaplus.engine;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusRecoveryPreparerTracker {

    private boolean started;
    private Set<XAPlusRecoveredResource> waiting;
    private Set<XAPlusRecoveredResource> recovered;

    XAPlusRecoveryPreparerTracker() {
        started = false;
        waiting = new HashSet<>();
        recovered = new HashSet<>();
    }

    void start() {
        this.started = true;
    }

    boolean isStarted() {
        return started;
    }

    void track(XAPlusRecoveredResource resource) {
        waiting.add(resource);
    }

    void resourceRecovered(XAPlusRecoveredResource resource) {
        if (waiting.remove(resource)) {
            recovered.add(resource);
        }
    }

    void resourceFailed(XAPlusRecoveredResource resource) {
        if (waiting.remove(resource)) {
            resource.close();
        }
    }

    boolean isRecoveryPrepared() {
        return waiting.isEmpty();
    }

    public Set<XAPlusRecoveredResource> getRecoveredResources() {
        return recovered;
    }

    void reset() {
        started = false;
        waiting.clear();
        recovered.clear();
    }
}
