package org.xaplus.engine;

import java.util.HashMap;
import java.util.Map;

final class XAPlusRecoveryOrdersTracker {
    private final Map<XAPlusXid, Boolean> branches;
    private final Map<XAPlusXid, String> uniqueNames;

    XAPlusRecoveryOrdersTracker() {
        branches = new HashMap<>();
        uniqueNames = new HashMap<>();
    }

    void track(XAPlusXid xid, String uniqueName, Boolean status) {
        branches.put(xid, status);
        uniqueNames.put(xid, uniqueName);
    }

    String getUniqueName(XAPlusXid xid) {
        return uniqueNames.get(xid);
    }

    Boolean getStatus(XAPlusXid xid) {
        return branches.get(xid);
    }

    void remove(XAPlusXid xid) {
        branches.remove(xid);
        uniqueNames.remove(xid);
    }
}
