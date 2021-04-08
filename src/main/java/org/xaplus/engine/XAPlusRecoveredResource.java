package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public class XAPlusRecoveredResource {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveredResource.class);

    private final String uniqueName;
    private final long inFlightCutoff;
    private final javax.sql.XAConnection jdbcConnection;
    private final javax.jms.XAJMSContext jmsContext;
    private final XAResource xaResource;
    private final Set<XAPlusXid> recoveredXids;

    XAPlusRecoveredResource(String uniqueName, long inFlightCutoff, javax.sql.XAConnection jdbcConnection) throws SQLException {
        this.uniqueName = uniqueName;
        this.inFlightCutoff = inFlightCutoff;
        this.jdbcConnection = jdbcConnection;
        this.jmsContext = null;
        this.xaResource = jdbcConnection.getXAResource();
        this.recoveredXids = new HashSet<>();
    }

    XAPlusRecoveredResource(String uniqueName, long inFlightCutoff, javax.jms.XAJMSContext jmsContext) {
        this.uniqueName = uniqueName;
        this.inFlightCutoff = inFlightCutoff;
        this.jdbcConnection = null;
        this.jmsContext = jmsContext;
        this.xaResource = jmsContext.getXAResource();
        this.recoveredXids = new HashSet<>();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(uniqueName=" + uniqueName + ")";
    }

    String getUniqueName() {
        return uniqueName;
    }

    XAResource getXaResource() {
        return xaResource;
    }

    Set<XAPlusXid> getRecoveredXids() {
        return recoveredXids;
    }

    void close() {
        if (jdbcConnection != null) {
            try {
                jdbcConnection.close();
                if (logger.isDebugEnabled()) {
                    logger.warn("Connection closed to {}", uniqueName);
                }
            } catch (SQLException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Close connection to {} failed as {}", uniqueName, e.getMessage());
                }
            }
        }
        if (jmsContext != null) {
            jmsContext.close();
        }
    }

    int recovery(String serverId) throws XAException {
        Set<XAPlusXid> xids = new HashSet<>();
        int xidCount;
        xidCount = recover(xids, serverId, XAResource.TMSTARTRSCAN);
        logger.debug("Recover {} xids with TMSTARTSCAN", xidCount);
        while (xidCount > 0) {
            xidCount = recover(xids, serverId, XAResource.TMNOFLAGS);
            logger.debug("Recover {} xids with TMNOFLAGS", xidCount);
        }
        xidCount = recover(xids, serverId, XAResource.TMENDRSCAN);
        logger.debug("Recover {} xids with TMENDRSCAN", xidCount);
        // Only use transactions created before inFlightCutoff to recovery
        Set<XAPlusXid> filteredXids = xids.stream()
                .filter(xid -> xid.getBranchQualifierUid().extractTimestamp() < inFlightCutoff)
                .collect(Collectors.toSet());
        recoveredXids.addAll(filteredXids);
        return xids.size();
    }

    private int recover(Set<XAPlusXid> xids, String serverId, int flag) throws XAException {
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
