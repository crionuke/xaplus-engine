package org.xaplus.engine;

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
class XAPlusRecover {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecover.class);

    private final String serverId;
    private final XAResource xaResource;

    XAPlusRecover(String serverId, XAResource xaResource) {
        this.serverId = serverId;
        this.xaResource = xaResource;
    }

    Set<XAPlusXid> recovery() throws XAException {
        Set<XAPlusXid> xids = new HashSet<>();
        int xidCount;
        xidCount = recover(xids, XAResource.TMSTARTRSCAN);
        logger.debug("Recover {} xids with TMSTARTSCAN", xidCount);
        while (xidCount > 0) {
            xidCount = recover(xids, XAResource.TMNOFLAGS);
            logger.debug("Recover {} xids with TMNOFLAGS", xidCount);
        }
        recover(xids, XAResource.TMENDRSCAN);
        logger.debug("Recover {} xids with TMENDRSCAN", xidCount);
        return xids;
    }

    private int recover(Set<XAPlusXid> xids, int flag) throws XAException {
        Xid[] recovered = xaResource.recover(flag);
        if (recovered == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("No xid to recovery, flag=", flag);
            }
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
