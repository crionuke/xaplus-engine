package org.xaplus.engine;

import java.util.HashMap;
import java.util.Map;

final class XAPlusRecoverySuperiorTracker {
    private final Map<XAPlusXid, Boolean> xids;
    private final Map<XAPlusXid, String> uniqueNames;

    XAPlusRecoverySuperiorTracker() {
        xids = new HashMap<>();
        uniqueNames = new HashMap<>();
    }

    void track(XAPlusXid xid, String uniqueName, Boolean status) {
        xids.put(xid, status);
        uniqueNames.put(xid, uniqueName);
    }

    String getUniqueName(XAPlusXid xid) {
        return uniqueNames.get(xid);
    }

    Boolean getStatus(XAPlusXid xid) {
        return xids.get(xid);
    }

    void remove(XAPlusXid xid) {
        xids.remove(xid);
        uniqueNames.remove(xid);
    }

    void reset() {
        xids.clear();
        uniqueNames.clear();
    }
}
