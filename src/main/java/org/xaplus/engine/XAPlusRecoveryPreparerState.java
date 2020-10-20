package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class XAPlusRecoveryPreparerState {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryPreparerState.class);

    private boolean started;
    private Map<String, javax.sql.XAConnection> jdbcConnections;
    private Map<String, javax.jms.XAConnection> jmsConnections;
    private Map<String, XAResource> xaResources;
    private Set<String> remain;
    private Map<String, Set<XAPlusXid>> recoveredXids;
    private Map<String, Map<XAPlusXid, Boolean>> danglingTransactions;
    private boolean failed;

    XAPlusRecoveryPreparerState() {
        started = false;
        failed = false;
        jdbcConnections = new HashMap<>();
        jmsConnections = new HashMap<>();
        xaResources = new HashMap<>();
        recoveredXids = new HashMap<>();
        remain = new HashSet<>();
        danglingTransactions = new HashMap<>();
    }

    void start() {
        this.started = true;
    }

    boolean isStarted() {
        return started;
    }

    void track(String uniqueName, javax.sql.XAConnection xaConnection) throws SQLException {
        jdbcConnections.put(uniqueName, xaConnection);
        xaResources.put(uniqueName, xaConnection.getXAResource());
        remain.add(uniqueName);
    }

    void track(String uniqueName, javax.jms.XAConnection xaConnection) throws JMSException {
        jmsConnections.put(uniqueName, xaConnection);
        xaResources.put(uniqueName, xaConnection.createXASession().getXAResource());
        remain.add(uniqueName);
    }

    void putRecoveredXids(String uniqueName, Set<XAPlusXid> xids) {
        remain.remove(uniqueName);
        recoveredXids.put(uniqueName, xids);
    }

    void recoveryResourceFailed(String uniqueName) {
        remain.remove(uniqueName);
    }

    void putDanglingTransactions(Map<String, Map<XAPlusXid, Boolean>> danglingTransactions) {
        this.danglingTransactions.putAll(danglingTransactions);
    }

    void findDanglingTransactionsFailed() {
        this.failed = true;
    }

    boolean isFailed() {
        return failed;
    }

    boolean isRecovered() {
        return remain.isEmpty();
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

    void close() {
        for (String uniqueName : jdbcConnections.keySet()) {
            javax.sql.XAConnection connection = jdbcConnections.get(uniqueName);
            try {
                connection.close();
            } catch (SQLException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Close connection to {} failed, {}", uniqueName, e.getMessage());
                }
            }
        }
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
    }

    void reset() {
        started = false;
        jdbcConnections.clear();
        jmsConnections.clear();
        xaResources.clear();
        remain.clear();
        recoveredXids.clear();
        danglingTransactions.clear();
        failed = false;
    }
}
