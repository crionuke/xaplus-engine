package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusFindRecoveredXidStatusFailedEvent extends Event<XAPlusFindRecoveredXidStatusFailedEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusRecoveredResource recoveredResource;
    private final Exception exception;

    public XAPlusFindRecoveredXidStatusFailedEvent(XAPlusXid xid, XAPlusRecoveredResource recoveredResource,
                                                   Exception exception) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (recoveredResource == null) {
            throw new NullPointerException("recoveredResource is null");
        }
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.xid = xid;
        this.recoveredResource = recoveredResource;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleFindRecoveredXidStatusFailed(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(xid=" + xid + ", exception=" + exception + ")";
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusRecoveredResource getRecoveredResource() {
        return recoveredResource;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleFindRecoveredXidStatusFailed(XAPlusFindRecoveredXidStatusFailedEvent event) throws InterruptedException;
    }
}