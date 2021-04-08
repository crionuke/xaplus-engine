package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveredXidStatusFoundEvent extends Event<XAPlusRecoveredXidStatusFoundEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusRecoveredResource recoveredResource;
    private final boolean status;

    public XAPlusRecoveredXidStatusFoundEvent(XAPlusXid xid, XAPlusRecoveredResource recoveredResource,
                                              boolean status) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (recoveredResource == null) {
            throw new NullPointerException("recoveredResource is null");
        }
        this.xid = xid;
        this.recoveredResource = recoveredResource;
        this.status = status;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveredXidStatusFound(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(xid=" + xid + ", status=" + status + ")";
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusRecoveredResource getRecoveredResource() {
        return recoveredResource;
    }

    public boolean getStatus() {
        return status;
    }

    public interface Handler {
        void handleRecoveredXidStatusFound(XAPlusRecoveredXidStatusFoundEvent event) throws InterruptedException;
    }
}