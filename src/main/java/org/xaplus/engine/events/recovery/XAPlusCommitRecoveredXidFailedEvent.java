package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusCommitRecoveredXidFailedEvent extends Event<XAPlusCommitRecoveredXidFailedEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusRecoveredResource resource;

    public XAPlusCommitRecoveredXidFailedEvent(XAPlusXid xid, XAPlusRecoveredResource resource) {
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
        handler.handleCommitRecoveredXidFailed(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(xid=" + xid + ", resource=" + resource + ")";
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusRecoveredResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleCommitRecoveredXidFailed(XAPlusCommitRecoveredXidFailedEvent event) throws InterruptedException;
    }
}