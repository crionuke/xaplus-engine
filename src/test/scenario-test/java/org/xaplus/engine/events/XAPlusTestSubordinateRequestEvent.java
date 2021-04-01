package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTestSubordinateRequestEvent extends Event<XAPlusTestSubordinateRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final long value;
    private final boolean beforeCommitException;

    public XAPlusTestSubordinateRequestEvent(XAPlusXid xid, long value, boolean beforeCommitException) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
        this.value = value;
        this.beforeCommitException = beforeCommitException;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTestSubordinateRequest(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public long getValue() {
        return value;
    }

    public boolean isBeforeCommitException() {
        return beforeCommitException;
    }

    public interface Handler {
        void handleTestSubordinateRequest(XAPlusTestSubordinateRequestEvent event) throws InterruptedException;
    }
}