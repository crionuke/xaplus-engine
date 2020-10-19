package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryResourceFailedEvent extends Event<XAPlusRecoveryResourceFailedEvent.Handler> {

    private final String uniqueName;
    private final Exception exception;

    public XAPlusRecoveryResourceFailedEvent(String uniqueName, Exception exception) {
        super();
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName is null");
        }
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.uniqueName = uniqueName;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryResourceFailed(this);
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleRecoveryResourceFailed(XAPlusRecoveryResourceFailedEvent event) throws InterruptedException;
    }
}