package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryResourceFailedEvent extends Event<XAPlusRecoveryResourceFailedEvent.Handler> {

    private final XAPlusRecoveredResource recoveredResource;
    private final Exception exception;

    public XAPlusRecoveryResourceFailedEvent(XAPlusRecoveredResource recoveredResource, Exception exception) {
        super();
        if (recoveredResource == null) {
            throw new NullPointerException("recoveredResource is null");
        }
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.recoveredResource = recoveredResource;
        this.exception = exception;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(recoveredResource=" + recoveredResource +
                ", exception=" + exception + ")";
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryResourceFailed(this);
    }

    public XAPlusRecoveredResource getRecoveredResource() {
        return recoveredResource;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleRecoveryResourceFailed(XAPlusRecoveryResourceFailedEvent event) throws InterruptedException;
    }
}