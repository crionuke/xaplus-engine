package org.xaplus.engine;

import javax.jms.JMSException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class XAPlusRecoveryState {
    private boolean started;
    private boolean failed;
    private Map<String, XAConnection> jdbcConnections;
    private Map<String, javax.jms.XAConnection> jmsConnections;
    private Map<String, XAResource> xaResources;
    private Map<String, Set<XAPlusXid>> recoveredXids;
    private Set<String> recovering;
    private Map<String, Map<XAPlusXid, Boolean>> danglingTransactions;

    XAPlusRecoveryState() {
        started = false;
        failed = false;
        jdbcConnections = new HashMap<>();
        jmsConnections = new HashMap<>();
        xaResources = new HashMap<>();
        recoveredXids = new HashMap<>();
        recovering = new HashSet<>();
        danglingTransactions = null;
    }

    void track(String uniqueName, javax.sql.XAConnection xaConnection) throws SQLException {
        jdbcConnections.put(uniqueName, xaConnection);
        xaResources.put(uniqueName, xaConnection.getXAResource());
        recovering.add(uniqueName);
    }

    void track(String uniqueName, javax.jms.XAConnection xaConnection) throws JMSException {
        jmsConnections.put(uniqueName, xaConnection);
        xaResources.put(uniqueName, xaConnection.createXASession().getXAResource());
        recovering.add(uniqueName);
    }

    void setStarted() {
        this.started = true;
    }

    void setFailed() {
        this.failed = true;
    }

    void setRecoveredXids(String uniqueName, Set<XAPlusXid> xids) {
        recovering.remove(uniqueName);
        recoveredXids.put(uniqueName, xids);
    }

    void setRecoveryFailed(String uniqueName) {
        recovering.remove(uniqueName);
    }

    boolean isStarted() {
        return started;
    }

    boolean isFailed() {
        return failed;
    }

    boolean isRecovered() {
        return recovering.isEmpty();
    }

    boolean isReady() {
        return recovering.isEmpty() && danglingTransactions != null;
    }

    Map<String, Set<XAPlusXid>> getRecoveredXids() {
        return recoveredXids;
    }

    Map<String, Map<XAPlusXid, Boolean>> getDanglingTransactions() {
        return danglingTransactions;
    }

    void setDanglingTransactions(Map<String, Map<XAPlusXid, Boolean>> danglingTransactions) {
        this.danglingTransactions = danglingTransactions;
    }

    XAResource getXAResource(String uniqueName) {
        return xaResources.get(uniqueName);
    }

    void reset() {
        started = false;
        //TODO: close all connections
        jdbcConnections.clear();
        jmsConnections.clear();
        xaResources.clear();
        recoveredXids.clear();
        recovering.clear();
        danglingTransactions.clear();
    }
}
