package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTestSubordinateFinishedEvent extends
        Event<XAPlusTestSubordinateFinishedEvent.Handler> {

    private final boolean status;
    private final long value;

    public XAPlusTestSubordinateFinishedEvent(boolean status, long value) {
        super();
        this.status = status;
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTestSubordinateFinished(this);
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