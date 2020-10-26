package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveredXidForgottenEvent extends Event<XAPlusRecoveredXidForgottenEvent.Handler> {

    private final XAPlusXid xid;

    public XAPlusRecoveredXidForgottenEvent(XAPlusXid xid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDanglingTransactionForgot(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(xid=" + xid + ")";
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public interface Handler {
        void handleDanglingTransactionForgot(XAPlusRecoveredXidForgottenEvent event) throws InterruptedException;
    }
}