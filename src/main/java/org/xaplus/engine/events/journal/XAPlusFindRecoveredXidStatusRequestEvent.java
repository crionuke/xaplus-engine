package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusFindRecoveredXidStatusRequestEvent extends Event<XAPlusFindRecoveredXidStatusRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusRecoveredResource recoveredResource;

    public XAPlusFindRecoveredXidStatusRequestEvent(XAPlusXid xid, XAPlusRecoveredResource recoveredResource) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (recoveredResource == null) {
            throw new NullPointerException("recoveredResource is null");
        }
        this.xid = xid;
        this.recoveredResource = recoveredResource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleFindRecoveredXidStatusRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(xid=" + xid + ")";
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusRecoveredResource getRecoveredResource() {
        return recoveredResource;
    }

    public interface Handler {
        void handleFindRecoveredXidStatusRequest(XAPlusFindRecoveredXidStatusRequestEvent event) throws InterruptedException;
    }
}