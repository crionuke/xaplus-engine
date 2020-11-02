package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioSubordinateFinishedEvent extends
        Event<XAPlusScenarioSubordinateFinishedEvent.Handler> {

    private final boolean status;
    private final long value;

    public XAPlusScenarioSubordinateFinishedEvent(boolean status, long value) {
        super();
        this.status = status;
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioSubordinateFinished(this);
    }

    public boolean getStatus() {
        return status;
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleScenarioSubordinateFinished(XAPlusScenarioSubordinateFinishedEvent event)
                throws InterruptedException;
    }
}