package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusResource;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusReportTransactionStatusRequestEvent extends Event<XAPlusReportTransactionStatusRequestEvent.Handler> {

    private final XAPlusResource xaPlusResource;
    private final XAPlusXid xid;

    public XAPlusReportTransactionStatusRequestEvent(XAPlusResource xaPlusResource, XAPlusXid xid) {
        super();
        if (xaPlusResource == null) {
            throw new NullPointerException("xaPlusResource is null");
        }
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xaPlusResource = xaPlusResource;
        this.xid = xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleReportTransactionStatusRequest(this);
    }

    public XAPlusResource getXaPlusResource() {
        return xaPlusResource;
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public interface Handler {
        void handleReportTransactionStatusRequest(XAPlusReportTransactionStatusRequestEvent event) throws InterruptedException;
    }
}