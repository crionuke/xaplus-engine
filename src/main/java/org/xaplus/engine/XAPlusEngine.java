package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.XAPlusRecoveryRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;

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
            logger.trace("Set journal dataSource={}", dataSource);
        }
    }

    public void recovery() throws InterruptedException {
        if (logger.isInfoEnabled()) {
            logger.info("User start recovery");
        }
        dispatcher.dispatch(new XAPlusRecoveryRequestEvent());
    }

    /**
     * Register JDBC xaResource
     *
     * @param dataSource {@link javax.sql.XAConnection} factory
     * @param uniqueName unique name for JDBC xaResource
     */
    public void register(XADataSource dataSource, String uniqueName) {
        resources.register(dataSource, uniqueName);
        if (logger.isInfoEnabled()) {
            logger.info("Resource registered, uniqueName={}, dataSource={}", uniqueName, dataSource);
        }
    }

    /**
     * Register JMS xaResource
     *
     * @param connectionFactory {@link javax.jms.XAConnection} factory
     * @param uniqueName        unique name for JMS xaResource
     */
    public void register(XAConnectionFactory connectionFactory, String uniqueName) {
        resources.register(connectionFactory, uniqueName);
        if (logger.isInfoEnabled()) {
            logger.info("Resource registered, uniqueName={}, connectionFactory={}", uniqueName, connectionFactory);
        }
    }

    /**
     * Register XAPlus xaResource
     *
     * @param factory  {@link XAPlusResource} factory
     * @param serverId unique name for XAPlus xaResource
     */
    public void register(XAPlusFactory factory, String serverId) {
        resources.register(factory, serverId);
        if (logger.isInfoEnabled()) {
            logger.info("Resource registered, with serverId={}, factory={}", serverId, factory);
        }
    }

    /**
     * Begin new XA/XA+ getTransaction
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
        if (logger.isInfoEnabled()) {
            logger.info("User begin transaction with new xid={}", xid);
        }
    }

    /**
     * Begin XA transaction as part of global XA+ getTransaction
     *
     * @param xid xa+ transaction id
     */
    public void join(XAPlusXid xid) {
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (context.hasTransaction()) {
            throw new IllegalStateException("Nested transactions not supported");
        }
        XAPlusTransaction transaction = new XAPlusTransaction(xid, properties.getDefaultTimeoutInSeconds(),
                properties.getServerId());
        context.setTransaction(transaction);
        if (logger.isInfoEnabled()) {
            logger.info("Joined to transaction with xid={}", xid);
        }
    }

    /**
     * Enlist JDBC xaResource
     *
     * @param uniqueName name of xaResource
     * @return {@link javax.sql.XAConnection} instance
     * @throws SQLException access xaResource failed
     * @throws XAException  start XA xaResource failed
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
            logger.trace("Enlisting xaResource, uniqueName={}, xid={}", uniqueName, transaction.getXid());
        }
        XAPlusResources.XADataSourceWrapper wrapper =
                (XAPlusResources.XADataSourceWrapper) resources.get(uniqueName);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown xaResource, name=" + uniqueName);
        }
        javax.sql.XAConnection connection = wrapper.get();
        XAPlusXid branchXid = createAndStartBranch(uniqueName, connection.getXAResource());
        if (logger.isInfoEnabled()) {
            logger.info("Resource enlisted, uniqueName={}, branchXid={}, xid={}",
                    uniqueName, branchXid, transaction.getXid());
        }
        return connection.getConnection();
    }

    /**
     * Enlist JMS xaResource
     *
     * @param uniqueName name of xaResource
     * @return {@link Session} instance
     * @throws JMSException access xaResource failed
     * @throws XAException  start XA xaResource failed
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
            logger.trace("Enlisting xaResource, uniqueName={}, xid={}", uniqueName, transaction.getXid());
        }
        XAPlusResources.XAConnectionFactoryWrapper wrapper =
                (XAPlusResources.XAConnectionFactoryWrapper) resources.get(uniqueName);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown xaResource name=" + uniqueName);
        }
        javax.jms.XAConnection connection = wrapper.get();
        XASession session = connection.createXASession();
        XAPlusXid branchXid = createAndStartBranch(uniqueName, session.getXAResource());
        if (logger.isInfoEnabled()) {
            logger.info("Resource enlisted, uniqueName={}, branchXid={}, xid={}",
                    uniqueName, branchXid, transaction.getXid());
        }
        return session.getSession();
    }

    /**
     * Enlist XAPlus xaResource
     *
     * @param serverId name of xaResource
     * @return {@link XAPlusXid} in string representation
     * @throws XAException access xaResource failed or start XA xaResource failed
     */
    public XAPlusXid enlistXAPlus(String serverId) throws XAException {
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
            logger.trace("Enlisting xaResource, serverId={}, xid={}", serverId, transaction.getXid());
        }
        XAPlusResources.XAPlusFactoryWrapper wrapper =
                (XAPlusResources.XAPlusFactoryWrapper) resources.get(serverId);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown serverId=" + serverId);
        }
        XAPlusResource resource = wrapper.get();
        XAPlusXid branchXid = createAndStartBranch(serverId, resource);
        if (logger.isInfoEnabled()) {
            logger.info("Resource enlisted, uniqueName={}, branchXid={}, xid={}", serverId, branchXid,
                    transaction.getXid());
        }
        return branchXid;
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
        if (logger.isInfoEnabled()) {
            logger.info("User commit getTransaction, xid={}", context.getTransaction().getXid());
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
        if (logger.isInfoEnabled()) {
            logger.info("User rollback getTransaction, xid={}", context.getTransaction().getXid());
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
            throw new NullPointerException("xaResource is null");
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
            logger.trace("Starting branch, branchXid={}, xaResource={}", branchXid, resource);
        }
        resource.start(branchXid, XAResource.TMNOFLAGS);
        if (logger.isDebugEnabled()) {
            logger.debug("Branch started, branchXid={}, xaResource={}", branchXid, resource);
        }
        return branchXid;
    }

    private XAPlusXid createAndStartBranch(String serverId, XAPlusResource resource) throws XAException {
        if (resource == null) {
            throw new NullPointerException("xaResource is null");
        }
        XAPlusThreadContext context = threadOfControl.getThreadContext();
        if (!context.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = context.getTransaction();
        XAPlusXid branchXid = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), serverId);
        transaction.enlist(branchXid, serverId, resource);
        if (logger.isTraceEnabled()) {
            logger.trace("Starting branch, branchXid={}, xaResource={}", branchXid, resource);
        }
        resource.start(branchXid, XAResource.TMNOFLAGS);
        if (logger.isDebugEnabled()) {
            logger.debug("Branch started, branchXid={}, xaResource={}", branchXid, resource);
        }
        return branchXid;
    }
}
