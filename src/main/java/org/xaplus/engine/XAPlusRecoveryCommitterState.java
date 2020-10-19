package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class XAPlusRecoveryCommitterState {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryCommitterState.class);

    private boolean started;
    private Map<String, XAConnection> jdbcConnections;
    private Map<String, javax.jms.XAConnection> jmsConnections;
    private Map<String, XAResource> xaResources;
    private Map<String, Set<XAPlusXid>> recoveredXids;
    private Map<String, Map<XAPlusXid, Boolean>> danglingTransactions;

    XAPlusRecoveryCommitterState() {
        started = false;
        jdbcConnections = new HashMap<>();
        jmsConnections = new HashMap<>();
        xaResources = new HashMap<>();
        recoveredXids = new HashMap<>();
        danglingTransactions = new HashMap<>();
    }

    void start(Map<String, XAConnection> jdbcConnections, Map<String, javax.jms.XAConnection> jmsConnections,
               Map<String, XAResource> xaResources, Map<String, Set<XAPlusXid>> recoveredXids,
               Map<String, Map<XAPlusXid, Boolean>> danglingTransactions) {
        this.jdbcConnections = jdbcConnections;
        this.jmsConnections = jmsConnections;
        this.xaResources = xaResources;
        this.recoveredXids = recoveredXids;
        this.danglingTransactions = danglingTransactions;
    }

    boolean isStarted() {
        return started;
    }

    Map<String, XAConnection> getJdbcConnections() {
        return jdbcConnections;
    }

    Map<String, javax.jms.XAConnection> getJmsConnections() {
        return jmsConnections;
    }

    Map<String, XAResource> getXaResources() {
        return xaResources;
    }

    Map<String, Set<XAPlusXid>> getRecoveredXids() {
        return recoveredXids;
    }

    Map<String, Map<XAPlusXid, Boolean>> getDanglingTransactions() {
        return danglingTransactions;
    }

    void reset() {
        started = false;
        for (String uniqueName : jdbcConnections.keySet()) {
            XAConnection connection = jdbcConnections.get(uniqueName);
            try {
                connection.close();
            } catch (SQLException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Close connection to {} failed, {}", uniqueName, e.getMessage());
                }
            }
        }
        jdbcConnections.clear();
        for (String uniqueName : jmsConnections.keySet()) {
            javax.jms.XAConnection connection = jmsConnections.get(uniqueName);
            try {
                connection.close();
            } catch (JMSException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Close connection to {} failed, {}", uniqueName, e.getMessage());
                }
            }
        }
        jmsConnections.clear();
        xaResources.clear();
        recoveredXids.clear();
        danglingTransactions.clear();
    }
}
