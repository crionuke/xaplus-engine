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
    void ready(Xid xid) throws XAPlusException;

    /**
     * Report to superior that a transaction branch {@code xid} has been failed by subordinate
     *
     * @param xid a transaction branch identifier
     * @throws XAPlusException an error has occurred
     */
    void failed(Xid xid) throws XAPlusException;

    /**
     * Request transaction status from superior
     *
     * @param xid a transaction branch identifier
     * @throws XAPlusException an error has occurred
     */
    void retry(XAPlusXid xid) throws XAPlusException;
}
