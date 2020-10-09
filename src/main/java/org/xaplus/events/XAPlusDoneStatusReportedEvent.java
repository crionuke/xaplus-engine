package org.xaplus.events;

import com.crionuke.bolts.Event;
import org.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDoneStatusReportedEvent extends Event<XAPlusDoneStatusReportedEvent.Handler> {

    private final XAPlusXid xid;

    public XAPlusDoneStatusReportedEvent(XAPlusXid xid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDoneStatusReported(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public interface Handler {
        void handleDoneStatusReported(XAPlusDoneStatusReportedEvent event) throws InterruptedException;
    }
}