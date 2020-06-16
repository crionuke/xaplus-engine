package com.crionuke.xaplus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusResourceRecovery {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusResourceRecovery.class);

    private final String serverId;
    private final XAResource xaResource;

    XAPlusResourceRecovery(String serverId, XAResource xaResource) {
        this.serverId = serverId;
        this.xaResource = xaResource;
    }

    Set<XAPlusXid> recovery() throws XAException {
        Set<XAPlusXid> xids = new HashSet<>();
        int xidCount;
        xidCount = recover(xids, XAResource.TMSTARTRSCAN);
        while (xidCount > 0) {
            xidCount = recover(xids, XAResource.TMNOFLAGS);
        }
        recover(xids, XAResource.TMENDRSCAN);
        return xids;
    }

    private int recover(Set<XAPlusXid> xids, int flag) throws XAException {
        Xid[] recovered = xaResource.recover(flag);
        if (recovered == null) {
            return 0;
        }
        Set<XAPlusXid> freshly = new HashSet<>();
        for (Xid xid : recovered) {
            if (xid.getFormatId() != XAPlusXid.FORMAT_ID) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skipping xid={} with format={}", xid, xid.getFormatId());
                }
                continue;
            }
            XAPlusXid xaPlusXid = new XAPlusXid(xid);
            if (xids.contains(xaPlusXid)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Already recovered xid={}, skipping it", xaPlusXid);
                }
                continue;
            }
            String extractedServerId = xaPlusXid.getGlobalTransactionIdUid().extractServerId();
            if (extractedServerId == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skipping xid={} as its serverId is null", xaPlusXid);
                }
                continue;
            }
            if (!extractedServerId.equals(serverId)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skipping xid={} as its serverId={} does not match this serverId={}",
                            xaPlusXid, extractedServerId, serverId);
                }
                continue;
            }
            freshly.add(xaPlusXid);
        }
        xids.addAll(freshly);
        return freshly.size();
    }
}
