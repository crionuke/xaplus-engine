package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

import javax.transaction.xa.XAResource;
import java.util.Map;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryPreparedEvent extends Event<XAPlusRecoveryPreparedEvent.Handler> {

    private final Map<String, javax.sql.XAConnection> jdbcConnections;
    private final Map<String, javax.jms.XAJMSContext> jmsContexts;
    private final Map<String, XAResource> xaResources;
    private final Map<String, Set<XAPlusXid>> recoveredXids;
    private final Map<String, Map<XAPlusXid, Boolean>> danglingTransactions;

    public XAPlusRecoveryPreparedEvent(Map<String, javax.sql.XAConnection> jdbcConnections,
                                       Map<String, javax.jms.XAJMSContext> jmsContexts,
                                       Map<String, XAResource> xaResources,
                                       Map<String, Set<XAPlusXid>> recoveredXids,
                                       Map<String, Map<XAPlusXid, Boolean>> danglingTransactions) {
        super();
        if (jdbcConnections == null) {
            throw new NullPointerException("jdbcConnections is null");
        }
        if (jmsContexts == null) {
            throw new NullPointerException("jmsContexts is null");
        }
        if (xaResources == null) {
            throw new NullPointerException("xaResources is null");
        }
        if (recoveredXids == null) {
            throw new NullPointerException("recoveredXids is null");
        }
        if (danglingTransactions == null) {
            throw new NullPointerException("danglingTransactions is null");
        }
        this.jdbcConnections = jdbcConnections;
        this.jmsContexts = jmsContexts;
        this.xaResources = xaResources;
        this.recoveredXids = recoveredXids;
        this.danglingTransactions = danglingTransactions;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryPrepared(this);
    }

    public Map<String, javax.sql.XAConnection> getJdbcConnections() {
        return jdbcConnections;
    }

    public Map<String, javax.jms.XAJMSContext> getJmsContexts() {
        return jmsContexts;
    }

    public Map<String, XAResource> getXaResources() {
        return xaResources;
    }

    public Map<String, Set<XAPlusXid>> getRecoveredXids() {
        return recoveredXids;
    }

    public Map<String, Map<XAPlusXid, Boolean>> getDanglingTransactions() {
        return danglingTransactions;
    }

    public interface Handler {
        void handleRecoveryPrepared(XAPlusRecoveryPreparedEvent event) throws InterruptedException;
    }
}