package org.xaplus.engine.events.xaplus;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusResource;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusReportReadyStatusRequestEvent extends Event<XAPlusReportReadyStatusRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusResource resource;

    public XAPlusReportReadyStatusRequestEvent(XAPlusXid xid, XAPlusResource resource) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.xid = xid;
        this.resource = resource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleReportReadyStatusRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(xid=" + xid + ")";
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleReportReadyStatusRequest(XAPlusReportReadyStatusRequestEvent event) throws InterruptedException;
    }
}