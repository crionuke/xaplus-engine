package org.xaplus.engine;

import java.util.HashMap;
import java.util.Map;

final class XAPlusRetriesTracker {

    private Map<XAPlusUid, Map<String, XAPlusXid>> waiting;

    XAPlusRetriesTracker() {
        waiting = new HashMap<>();
    }

    void track(XAPlusXid xid, String uniqueName) {
        XAPlusUid gtrid = xid.getGlobalTransactionIdUid();
        Map<String, XAPlusXid> xids = waiting.get(gtrid);
        if (xids == null) {
            xids = new HashMap<>();
            waiting.put(gtrid, xids);
        }
        xids.put(uniqueName, xid);
    }

    Map<String, XAPlusXid> remove(XAPlusXid xid) {
        return waiting.remove(xid.getGlobalTransactionIdUid());
    }

    void reset() {
        waiting.clear();
    }
}
