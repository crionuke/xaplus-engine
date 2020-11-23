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

class XAPlusRecoveryCommitterTracker {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryCommitterTracker.class);

    private boolean started;
    private Map<String, XAConnection> jdbcConnections;
    private Map<String, javax.jms.XAJMSContext> jmsContexts;
    private Map<String, XAResource> xaResources;
    private Map<String, Set<XAPlusXid>> recoveredXids;
    private Map<String, Map<XAPlusXid, Boolean>> danglingTransactions;

    XAPlusRecoveryCommitterTracker() {
        started = false;
        jdbcConnections = new HashMap<>();
        jmsContexts = new HashMap<>();
        xaResources = new HashMap<>();
        recoveredXids = new HashMap<>();
        danglingTransactions = new HashMap<>();
    }

    void start(Map<String, XAConnection> jdbcConnections, Map<String, javax.jms.XAJMSContext> jmsContexts,
               Map<String, XAResource> xaResources, Map<String, Set<XAPlusXid>> recoveredXids,
               Map<String, Map<XAPlusXid, Boolean>> danglingTransactions) {
        this.jdbcConnections = jdbcConnections;
        this.jmsContexts = jmsContexts;
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

    Map<String, javax.jms.XAJMSContext> getJmsContexts() {
        return jmsContexts;
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
        for (String uniqueName : jmsContexts.keySet()) {
            javax.jms.XAJMSContext context = jmsContexts.get(uniqueName);
            context.close();
        }
        jmsContexts.clear();
        xaResources.clear();
        recoveredXids.clear();
        danglingTransactions.clear();
    }
}
