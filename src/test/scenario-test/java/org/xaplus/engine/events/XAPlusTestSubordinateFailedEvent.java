package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTestSubordinateFailedEvent extends Event<XAPlusTestSubordinateFailedEvent.Handler> {

    private final XAPlusXid xid;
    private final long value;
    private final Exception exception;

    public XAPlusTestSubordinateFailedEvent(XAPlusXid xid, long value, Exception exception) {
        super();
        this.xid = xid;
        this.value = value;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTestSubordinateFailed(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public long getValue() {
        return value;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleTestSubordinateFailed(XAPlusTestSubordinateFailedEvent event) throws InterruptedException;
    }
}