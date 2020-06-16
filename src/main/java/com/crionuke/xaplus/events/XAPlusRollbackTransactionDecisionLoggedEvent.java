package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackTransactionDecisionLoggedEvent extends Event<XAPlusRollbackTransactionDecisionLoggedEvent.Handler> {

    private final XAPlusXid xid;

    public XAPlusRollbackTransactionDecisionLoggedEvent(XAPlusXid xid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackTransactionDecisionLogged(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public interface Handler {
        void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event) throws InterruptedException;
    }
}