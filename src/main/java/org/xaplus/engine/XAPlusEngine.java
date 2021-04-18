package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.XAPlusStartRecoveryRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.XAConnectionFactory;
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
    private final XAPlusThreadOfControl threadOfControl;
    private volatile DataSource tLogDataSource;

    XAPlusEngine(XAPlusProperties properties, XAPlusDispatcher dispatcher,
                 XAPlusResources resources, XAPlusThreadOfControl threadOfControl) {
        this.properties = properties;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.threadOfControl = threadOfControl;
    }

    /**
     * Register JDBC resource
     *
     * @param dataSource {@link javax.sql.XAConnection} factory
     * @param uniqueName unique name for JDBC resource
     */
    public void register(XADataSource dataSource, String uniqueName) {
        resources.register(dataSource, uniqueName);
        if (logger.isInfoEnabled()) {
            logger.info("Resource registered, uniqueName={}, dataSource={}", uniqueName, dataSource);
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
        if (logger.isInfoEnabled()) {
            logger.info("Resource registered, uniqueName={}, connectionFactory={}", uniqueName, connectionFactory);
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
        if (logger.isInfoEnabled()) {
            logger.info("Resource registered, with serverId={}, factory={}", serverId, factory);
        }
    }

    /**
     * Begin new XA/XA+ transaction
     */
    public void begin() throws InterruptedException {
        if (tLogDataSource == null) {
            throw new IllegalStateException("Transaction log data source undefined");
        }
        XAPlusThreadContext threadContext = threadOfControl.getThreadContext();
        if (threadContext.hasTransaction()) {
            throw new IllegalStateException("Nested transactions not supported");
        }
        XAPlusXid xid = new XAPlusXid(new XAPlusUid(properties.getServerId()), new XAPlusUid(properties.getServerId()));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, properties.getTransactionsTimeoutInSeconds(),
                properties.getServerId());
        threadContext.setTransaction(transaction);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        if (logger.isInfoEnabled()) {
            logger.info("User begin transaction with new xid={}", xid);
        }
    }

    /**
     * Begin XA transaction as part of global XA+ transaction
     *
     * @param xid xa+ transaction id
     */
    public void join(XAPlusXid xid) throws InterruptedException {
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (tLogDataSource == null) {
            throw new IllegalStateException("Transaction log data source undefined");
        }
        XAPlusThreadContext threadContext = threadOfControl.getThreadContext();
        if (threadContext.hasTransaction()) {
            throw new IllegalStateException("Nested transactions not supported");
        }
        XAPlusTransaction transaction = new XAPlusTransaction(xid, properties.getTransactionsTimeoutInSeconds(),
                properties.getServerId());
        threadContext.setTransaction(transaction);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        if (logger.isInfoEnabled()) {
            logger.info("Joined to transaction with xid={}", xid);
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
        XAPlusThreadContext threadContext = threadOfControl.getThreadContext();
        if (!threadContext.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = threadContext.getTransaction();
        if (logger.isTraceEnabled()) {
            logger.trace("Enlisting XA resource, uniqueName={}, xid={}", uniqueName, transaction.getXid());
        }
        XAPlusResources.XADataSourceWrapper wrapper =
                (XAPlusResources.XADataSourceWrapper) resources.get(uniqueName);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown xaResource, name=" + uniqueName);
        }
        javax.sql.XAConnection connection = wrapper.get();
        XAPlusXid branchXid = createAndStartBranch(uniqueName, connection);
        if (logger.isInfoEnabled()) {
            logger.info("XA resource enlisted, uniqueName={}, branchXid={}, xid={}",
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
    public JMSContext enlistJms(String uniqueName) throws JMSException, XAException {
        if (uniqueName == null) {
            throw new NullPointerException("Unique name is null");
        }
        XAPlusThreadContext threadContext = threadOfControl.getThreadContext();
        if (!threadContext.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = threadContext.getTransaction();
        if (logger.isTraceEnabled()) {
            logger.trace("Enlisting XA resource, uniqueName={}, xid={}", uniqueName, transaction.getXid());
        }
        XAPlusResources.XAConnectionFactoryWrapper wrapper =
                (XAPlusResources.XAConnectionFactoryWrapper) resources.get(uniqueName);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown xaResource name=" + uniqueName);
        }
        javax.jms.XAJMSContext context = wrapper.get();
        XAPlusXid branchXid = createAndStartBranch(uniqueName, context);
        if (logger.isInfoEnabled()) {
            logger.info("XA resource enlisted, uniqueName={}, branchXid={}, xid={}",
                    uniqueName, branchXid, transaction.getXid());
        }
        return context.getContext();
    }

    /**
     * Enlist XA+ resource
     *
     * @param serverId name of XA+ resource
     * @return {@link XAPlusXid} transaction xid
     * @throws XAException access resource failed or start XA resource failed
     */
    public XAPlusXid enlistXAPlus(String serverId) throws XAException {
        if (serverId == null) {
            throw new NullPointerException("serverId is null");
        }
        XAPlusThreadContext threadContext = threadOfControl.getThreadContext();
        if (!threadContext.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = threadContext.getTransaction();
        if (!transaction.isSuperior()) {
            throw new IllegalStateException("Only superior has the right to enlistXAPlus XA+ resources");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Enlisting XA+ resource, serverId={}, xid={}", serverId, transaction.getXid());
        }
        XAPlusResources.XAPlusFactoryWrapper wrapper =
                (XAPlusResources.XAPlusFactoryWrapper) resources.get(serverId);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown serverId=" + serverId);
        }
        XAPlusResource resource = wrapper.get();
        XAPlusXid branchXid = createAndStartBranch(serverId, resource);
        if (logger.isInfoEnabled()) {
            logger.info("XA+ resource enlisted, uniqueName={}, branchXid={}, xid={}", serverId, branchXid,
                    transaction.getXid());
        }
        return branchXid;
    }

    /**
     * Commit transaction
     *
     * @return future to get result
     * @throws InterruptedException commit operation was interrupted
     */
    public XAPlusFuture commit() throws InterruptedException {
        XAPlusThreadContext threadContext = threadOfControl.getThreadContext();
        if (!threadContext.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = threadContext.getTransaction();
        threadContext.removeTransaction();
        if (logger.isInfoEnabled()) {
            logger.info("User commit transaction, {}", transaction);
        }
        // TODO: handle case when transaction just local
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        return transaction.getFuture();
    }

    /**
     * Rollback transaction
     *
     * @return future to get result
     * @throws InterruptedException rollback operation was interrupted
     */
    public XAPlusFuture rollback() throws InterruptedException {
        XAPlusThreadContext threadContext = threadOfControl.getThreadContext();
        if (!threadContext.hasTransaction()) {
            throw new IllegalStateException("No transaction on this thread");
        }
        XAPlusTransaction transaction = threadContext.getTransaction();
        threadContext.removeTransaction();
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        if (logger.isInfoEnabled()) {
            logger.info("User rollback transaction, {}", transaction);
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

    // TODO: start recovery on start and by timer
    void startRecovery() throws InterruptedException {
        if (logger.isInfoEnabled()) {
            logger.info("Start recovery");
        }
        dispatcher.dispatch(new XAPlusStartRecoveryRequestEvent());
    }

    DataSource getTLogDataSource() {
        return tLogDataSource;
    }

    public void setTLogDataSource(DataSource dataSource) {
        if (dataSource == null) {
            throw new NullPointerException("dataSource is null");
        }
        this.tLogDataSource = dataSource;
        if (logger.isTraceEnabled()) {
            logger.trace("Set journal dataSource={}", dataSource);
        }
    }

    private XAPlusXid createAndStartBranch(String uniqueName, javax.sql.XAConnection connection)
            throws SQLException, XAException {
        XAPlusTransaction transaction = threadOfControl.getThreadContext().getTransaction();
        XAPlusXid branchXid = new XAPlusXid(transaction.getXid().getGtrid(), properties.getServerId());
        transaction.enlist(branchXid, uniqueName, connection);
        return branchXid;
    }

    private XAPlusXid createAndStartBranch(String uniqueName, javax.jms.XAJMSContext jmsContext)
            throws XAException {
        XAPlusTransaction transaction = threadOfControl.getThreadContext().getTransaction();
        XAPlusXid branchXid = new XAPlusXid(transaction.getXid().getGtrid(), properties.getServerId());
        transaction.enlist(branchXid, uniqueName, jmsContext);
        return branchXid;
    }

    private XAPlusXid createAndStartBranch(String serverId, XAPlusResource resource) throws XAException {
        XAPlusTransaction transaction = threadOfControl.getThreadContext().getTransaction();
        XAPlusXid branchXid = new XAPlusXid(transaction.getXid().getGtrid(), serverId);
        transaction.enlist(branchXid, serverId, resource);
        if (logger.isTraceEnabled()) {
            logger.trace("Starting branch, branchXid={}, resource={}", branchXid, resource);
        }
        resource.start(branchXid, XAResource.TMNOFLAGS);
        if (logger.isDebugEnabled()) {
            logger.debug("Branch started, branchXid={}, resource={}", branchXid, resource);
        }
        return branchXid;
    }
}
