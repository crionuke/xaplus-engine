package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackRecoveredXidDecisionLoggedEvent extends Event<XAPlusRollbackRecoveredXidDecisionLoggedEvent.Handler> {

    private final XAPlusXid xid;
    private final String uniqueName;

    public XAPlusRollbackRecoveredXidDecisionLoggedEvent(XAPlusXid xid, String uniqueName) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName is null");
        }
        this.xid = xid;
        this.uniqueName = uniqueName;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackRecoveredXidDecisionLogged(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public interface Handler {
        void handleRollbackRecoveredXidDecisionLogged(XAPlusRollbackRecoveredXidDecisionLoggedEvent event) throws InterruptedException;
    }
}