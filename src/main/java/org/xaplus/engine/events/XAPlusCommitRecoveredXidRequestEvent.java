package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusCommitRecoveredXidRequestEvent extends Event<XAPlusCommitRecoveredXidRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final String uniqueName;
    private final XAResource resource;

    public XAPlusCommitRecoveredXidRequestEvent(XAPlusXid xid, XAResource resource, String uniqueName) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName is null");
        }
        this.xid = xid;
        this.resource = resource;
        this.uniqueName = uniqueName;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleCommitRecoveredXidRequest(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAResource getResource() {
        return resource;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public interface Handler {
        void handleCommitRecoveredXidRequest(XAPlusCommitRecoveredXidRequestEvent event) throws InterruptedException;
    }
}