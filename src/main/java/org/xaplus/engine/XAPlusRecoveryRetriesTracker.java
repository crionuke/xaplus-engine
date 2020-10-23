package org.xaplus.engine;

import java.util.HashMap;
import java.util.Map;

final class XAPlusRecoveryRetriesTracker {

    private Map<XAPlusUid, Map<XAPlusXid, String>> waiting;

    XAPlusRecoveryRetriesTracker() {
        waiting = new HashMap<>();
    }

    void track(XAPlusXid xid, String uniqueName) {
        XAPlusUid gtrid = xid.getGlobalTransactionIdUid();
        Map<XAPlusXid, String> xids = waiting.get(gtrid);
        if (xids == null) {
            xids = new HashMap<>();
            waiting.put(gtrid, xids);
        }
        xids.put(xid, uniqueName);
    }

    Map<XAPlusXid, String> remove(XAPlusXid xid) {
        return waiting.remove(xid.getGlobalTransactionIdUid());
    }

    void reset() {
        waiting.clear();
    }
}
