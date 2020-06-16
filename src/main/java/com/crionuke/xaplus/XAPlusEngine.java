package com.crionuke.xaplus;

import com.crionuke.xaplus.events.XAPlusRecoveryRequestEvent;
import com.crionuke.xaplus.events.XAPlusUserCommitRequestEvent;
import com.crionuke.xaplus.events.XAPlusUserRollbackRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Component
public final class XAPlusEngine {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusEngine.class);

    private final XAPlusProperties properties;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusUidGenerator uidGenerator;
    private final XAPlusThreadOfControl threadOfControl;
    private volatile DataSource tlogDataSource;

    XAPlusEngine(XAPlusProperties properties, XAPlusDispatcher dispatcher,
                 XAPlusResources resources, XAPlusUidGenerator uidGenerator,
                 XAPlusThreadOfControl threadOfControl) {
        this.properties = properties;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.uidGenerator = uidGenerator;
        this.threadOfControl = threadOfControl;
    }

    public void setTLogDataSource(DataSource dataSource) {
        this.tlogDataSource = dataSource;
        if (logger.isTraceEnabled()) {
            logger.trace("Set tlog dataSource={}", dataSource);
        }
    }

    public void recovery() throws InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug("User start recovery");
        }
        dispatcher.dispatch(new XAPlusRecoveryRequestEvent());
    }

    /**
     * Register JDBC resource
     *
     * @param dataSource {@link javax.sql.XAConnection} factory
     * @param uniqueName unique name for JDBC resource
     */
    public void register(XADataSource dataSource, String uniqueName) {
        resources.register(dataSource, uniqueName);
        if (logger.isDebugEnabled()) {
            logger.debug("Resource with uniqueName={}, dataSource={} registered", uniqueName, dataSource);
        }
    }

    /**
     * Register JMS resource
     *
     * @param connectionFactory {@link javax.jms.XAConnection} factory
     * @param uniqueName        unique name for JMS resource
     */
    public void register(XAConnectionFactory connectionFactory, String uniqueName) {
        resources.register(connectionFactory, uniqueName);
        if (logger.isDebugEnabled()) {
            logger.debug("Resource with uniqueName={}, connectionFactory={} registered", uniqueName, connectionFactory);
        }
    }

    /**
     * Register XAPlus resource
     *
     * @param factory  {@link XAPlusResource} factory
     * @param serverId unique name for XAPlus resource
     */
    public void register(XAPlusFactory factory, String serverId) {
        resources.register(factory, serverId);
        if (logger.isDebugEnabled()) {
            logger.debug("Resource with serverId={}, factory={} registered", serverId, factory);
        }
    }

    /**
     * Begin new XA/XA+ transaction
     */
    public void begin() {
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (context.hasTransaction()) {
            throw new IllegalStateException("Nested transactions not supported");
        }
        XAPlusXid xid = new XAPlusXid(uidGenerator.generateUid(properties.getServerId()),
                uidGenerator.generateUid(properties.getServerId()));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, properties.getDefaultTimeoutInSeconds(),
                properties.getServerId());
        context.setTransaction(transaction);
        if (logger.isDebugEnabled()) {
            logger.debug("User begin transaction with generated xid={}", xid);
        }
    }

    /**
     * Begin XA transaction as part of global XA+ transaction
     *
     * @param xidString xid in string representation like "grtid:bqual" (in hex)
     */
    public void join(String xidString) {
        if (xidString == null) {
            throw new NullPointerException("xidString is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (context.hasTransaction()) {
            throw new IllegalStateException("Nested transactions not supported");
        }
        if (xidString == null) {
            throw new NullPointerException("xid is null");
        }
        XAPlusXid xid = new XAPlusXid(xidString);
        XAPlusTransaction transaction = new XAPlusTransaction(xid, properties.getDefaultTimeoutInSeconds(),
                properties.getServerId());
        context.setTransaction(transaction);
        if (logger.isDebugEnabled()) {
            logger.debug("Joined to transaction with xid={}", xid);
        }
    }

    /**
     * Enlist JDBC resource
     *
     * @param uniqueName name of resource
     * @return {@link javax.sql.XAConnection} instance
     * @throws SQLException access resource failed
     * @throws XAException  start XA resource failed
     */
    public Connection enlistJdbc(String uniqueName) throws SQLException, XAException {
        if (uniqueName == null) {
            throw new NullPointerException("Unique name is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        if (logger.isTraceEnabled()) {
            logger.trace("Enlisting resource with uniqueName={}, xid={}", uniqueName, transaction.getXid());
        }
        XAPlusResources.XADataSourceWrapper wrapper =
                (XAPlusResources.XADataSourceWrapper) resources.get(uniqueName);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown resource name=" + uniqueName);
        }
        javax.sql.XAConnection connection = wrapper.get();
        XAPlusXid branchXid = createAndStartBranch(uniqueName, connection.getXAResource());
        if (logger.isDebugEnabled()) {
            logger.debug("Resource with uniqueName={}, branchXid={}, xid={} enlisted",
                    uniqueName, branchXid, transaction.getXid());
        }
        return connection.getConnection();
    }

    /**
     * Enlist JMS resource
     *
     * @param uniqueName name of resource
     * @return {@link Session} instance
     * @throws JMSException access resource failed
     * @throws XAException  start XA resource failed
     */
    public Session enlistJms(String uniqueName) throws JMSException, XAException {
        if (uniqueName == null) {
            throw new NullPointerException("Unique name is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        if (logger.isTraceEnabled()) {
            logger.trace("Enlisting resource with uniqueName={}, xid={}", uniqueName, transaction.getXid());
        }
        XAPlusResources.XAConnectionFactoryWrapper wrapper =
                (XAPlusResources.XAConnectionFactoryWrapper) resources.get(uniqueName);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown resource name=" + uniqueName);
        }
        javax.jms.XAConnection connection = wrapper.get();
        XASession session = connection.createXASession();
        XAPlusXid branchXid = createAndStartBranch(uniqueName, session.getXAResource());
        if (logger.isDebugEnabled()) {
            logger.debug("Resource with uniqueName={}, branchXid={}, xid={} enlisted",
                    uniqueName, branchXid, transaction.getXid());
        }
        return session.getSession();
    }

    /**
     * Enlist XAPlus resource
     *
     * @param serverId name of resource
     * @return {@link XAPlusXid} in string representation
     * @throws XAException access resource failed or start XA resource failed
     */
    public String enlistXAPlus(String serverId) throws XAException {
        if (serverId == null) {
            throw new NullPointerException("serverId is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        if (!transaction.isSuperior()) {
            throw new IllegalStateException("Only superior has the right to enlist XA+ resources");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Enlisting resource with serverId={}, xid={}", serverId, transaction.getXid());
        }
        XAPlusResources.XAPlusFactoryWrapper wrapper =
                (XAPlusResources.XAPlusFactoryWrapper) resources.get(serverId);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown serverId=" + serverId);
        }
        XAPlusResource resource = wrapper.get();
        XAPlusXid branchXid = createAndStartBranch(serverId, resource);
        if (logger.isDebugEnabled()) {
            logger.debug("Resource with uniqueName={}, branchXid={}, xid={} enlisted",
                    serverId, branchXid, transaction.getXid());
        }
        return branchXid.toString();
    }

    /**
     * Start 2pc protocol
     *
     * @throws InterruptedException commit operation was interrupted
     */
    public XAPlusFuture commit() throws InterruptedException {
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        if (logger.isDebugEnabled()) {
            logger.debug("User commit transaction with xid={}", context.getTransaction().getXid());
        }
        return transaction.getFuture();
    }

    /**
     * Start rollback protocol
     *
     * @throws InterruptedException rollback operation was interrupted
     */
    public XAPlusFuture rollback() throws InterruptedException {
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        if (logger.isDebugEnabled()) {
            logger.debug("User rollback transaction with xid={}", context.getTransaction().getXid());
        }
        return transaction.getFuture();
    }

    /**
     * Get serverId
     *
     * @return serverId
     */
    public String getServerId() {
        return properties.getServerId();
    }

    DataSource getTlogDataSource() {
        return tlogDataSource;
    }

    private XAPlusXid createAndStartBranch(String uniqueName, XAResource resource) throws XAException {
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        XAPlusXid branchXid = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(branchXid, uniqueName, resource);
        if (logger.isTraceEnabled()) {
            logger.trace("Starting branch with branchXid={}, resource={}", branchXid, resource);
        }
        resource.start(branchXid, XAResource.TMNOFLAGS);
        if (logger.isDebugEnabled()) {
            logger.debug("Branch with branchXid={}, resource={} started", branchXid, resource);
        }
        return branchXid;
    }

    private XAPlusXid createAndStartBranch(String serverId, XAPlusResource resource) throws XAException {
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        XAPlusXid branchXid = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), serverId);
        transaction.enlist(branchXid, serverId, resource);
        if (logger.isTraceEnabled()) {
            logger.trace("Starting branch with branchXid={}, resource={}", branchXid, resource);
        }
        resource.start(branchXid, XAResource.TMNOFLAGS);
        if (logger.isDebugEnabled()) {
            logger.debug("Branch with branchXid={}, resource={} started", branchXid, resource);
        }
        return branchXid;
    }
}
