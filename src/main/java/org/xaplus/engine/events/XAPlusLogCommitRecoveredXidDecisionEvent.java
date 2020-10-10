package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLogCommitRecoveredXidDecisionEvent extends Event<XAPlusLogCommitRecoveredXidDecisionEvent.Handler> {

    private final XAPlusXid xid;
    private final String uniqueName;

    public XAPlusLogCommitRecoveredXidDecisionEvent(XAPlusXid xid, String uniqueName) {
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
        handler.handleLogCommitRecoveredXidDecision(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public interface Handler {
        void handleLogCommitRecoveredXidDecision(XAPlusLogCommitRecoveredXidDecisionEvent event) throws InterruptedException;
    }
}