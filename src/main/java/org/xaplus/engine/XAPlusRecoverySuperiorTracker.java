package org.xaplus.engine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

final class XAPlusRecoverySuperiorTracker {
    private final Map<XAPlusXid, String> doing;

    XAPlusRecoverySuperiorTracker() {
        doing = new HashMap<>();
    }

    void track(XAPlusXid xid, String uniqueName) {
        doing.put(xid, uniqueName);
    }

    void clear(String uniqueName) {
        Iterator<Map.Entry<XAPlusXid, String>> iterator = doing.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<XAPlusXid, String> entry = iterator.next();
            String entryValue = entry.getValue();
            if (entryValue.equals(uniqueName)) {
                iterator.remove();
            }
        }
    }

    String remove(XAPlusXid xid) {
        return doing.remove(xid);
    }

    void reset() {
        doing.clear();
    }
}
