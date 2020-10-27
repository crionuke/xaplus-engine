package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioInitialRequestEvent extends Event<XAPlusScenarioInitialRequestEvent.Handler> {

    private final long value;
    private final boolean beforeRequestException;
    private final boolean beforeCommitException;

    public XAPlusScenarioInitialRequestEvent(long value, boolean beforeRequestException,
                                             boolean beforeCommitException) {
        super();
        this.value = value;
        this.beforeRequestException = beforeRequestException;
        this.beforeCommitException = beforeCommitException;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public boolean isBeforeRequestException() {
        return beforeRequestException;
    }

    public boolean isBeforeCommitException() {
        return beforeCommitException;
    }

    public interface Handler {
        void handleScenarioInitialRequest(XAPlusScenarioInitialRequestEvent event) throws InterruptedException;
    }
}