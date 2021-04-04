package org.xaplus.engine;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public interface XAPlusResource extends XAResource {

    /**
     * Report to superior that a transaction branch {@code xid} has been prepared by subordinate
     *
     * @param xid a transaction branch identifier
     * @throws XAPlusException an error has occurred
     */
    void readied(Xid xid) throws XAPlusException;

    /**
     * Report to superior that a transaction branch {@code xid} has been failed by subordinate
     *
     * @param xid a transaction branch identifier
     * @throws XAPlusException an error has occurred
     */
    void failed(Xid xid) throws XAPlusException;

    /**
     * Report to superior that a transaction branch {@code xid} has been committed or rolled back by subordinate
     *
     * @param xid a transaction branch identifier
     * @throws XAPlusException an error has occurred
     */
    void done(Xid xid) throws XAPlusException;

    /**
     * Request dangling transactions status from superior
     *
     * @param serverId this server uid
     * @throws XAPlusException an error has occured
     */
    void retry(String serverId) throws XAPlusException;
}
