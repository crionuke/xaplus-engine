package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTestSubordinateFinishedEvent extends
        Event<XAPlusTestSubordinateFinishedEvent.Handler> {

    private final XAPlusXid xid;
    private final boolean status;
    private final long value;

    public XAPlusTestSubordinateFinishedEvent(XAPlusXid xid, boolean status, long value) {
        super();
        this.xid = xid;
        this.status = status;
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTestSubordinateFinished(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public boolean getStatus() {
        return status;
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleTestSubordinateFinished(XAPlusTestSubordinateFinishedEvent event)
                throws InterruptedException;
    }
}