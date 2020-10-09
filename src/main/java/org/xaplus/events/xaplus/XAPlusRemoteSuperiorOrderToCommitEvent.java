package org.xaplus.events.xaplus;

import com.crionuke.bolts.Event;
import org.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRemoteSuperiorOrderToCommitEvent extends Event<XAPlusRemoteSuperiorOrderToCommitEvent.Handler> {

    private final XAPlusXid xid;

    public XAPlusRemoteSuperiorOrderToCommitEvent(XAPlusXid xid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRemoteSuperiorOrderToCommit(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public interface Handler {
        void handleRemoteSuperiorOrderToCommit(XAPlusRemoteSuperiorOrderToCommitEvent event) throws InterruptedException;
    }
}
