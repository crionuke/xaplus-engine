package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLogCommitTransactionDecisionEvent extends Event<XAPlusLogCommitTransactionDecisionEvent.Handler> {

    private final XAPlusXid xid;
    private final Map<XAPlusXid, String> uniqueNames;

    public XAPlusLogCommitTransactionDecisionEvent(XAPlusXid xid, Map<XAPlusXid, String> uniqueNames) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (uniqueNames == null) {
            throw new NullPointerException("uniqueNames is null");
        }
        this.xid = xid;
        this.uniqueNames = uniqueNames;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLogCommitTransactionDecision(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public Map<XAPlusXid, String> getUniqueNames() {
        return uniqueNames;
    }

    public interface Handler {
        void handleLogCommitTransactionDecision(XAPlusLogCommitTransactionDecisionEvent event) throws InterruptedException;
    }
}