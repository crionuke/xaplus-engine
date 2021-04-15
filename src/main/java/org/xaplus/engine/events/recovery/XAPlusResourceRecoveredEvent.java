package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusResourceRecoveredEvent extends Event<XAPlusResourceRecoveredEvent.Handler> {

    private final XAPlusRecoveredResource recoveredResource;

    public XAPlusResourceRecoveredEvent(XAPlusRecoveredResource recoveredResource) {
        super();
        if (recoveredResource == null) {
            throw new NullPointerException("recoveredResource is null");
        }
        this.recoveredResource = recoveredResource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleResourceRecovered(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(recoveredResource=" + recoveredResource + ")";
    }

    public XAPlusRecoveredResource getRecoveredResource() {
        return recoveredResource;
    }

    public interface Handler {
        void handleResourceRecovered(XAPlusResourceRecoveredEvent event) throws InterruptedException;
    }
}