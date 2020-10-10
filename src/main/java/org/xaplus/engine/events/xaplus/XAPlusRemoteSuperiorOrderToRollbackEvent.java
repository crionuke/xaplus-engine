package org.xaplus.engine.events.xaplus;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRemoteSuperiorOrderToRollbackEvent extends Event<XAPlusRemoteSuperiorOrderToRollbackEvent.Handler> {

    private final XAPlusXid xid;

    public XAPlusRemoteSuperiorOrderToRollbackEvent(XAPlusXid xid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRemoteSuperiorOrderToRollback(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public interface Handler {
        void handleRemoteSuperiorOrderToRollback(XAPlusRemoteSuperiorOrderToRollbackEvent event) throws InterruptedException;
    }
}
