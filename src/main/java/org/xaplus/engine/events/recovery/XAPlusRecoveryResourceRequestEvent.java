package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryResourceRequestEvent extends Event<XAPlusRecoveryResourceRequestEvent.Handler> {

    private final XAPlusRecoveredResource recoveredResource;

    public XAPlusRecoveryResourceRequestEvent(XAPlusRecoveredResource recoveredResource) {
        super();
        if (recoveredResource == null) {
            throw new NullPointerException("recoveredResource is null");
        }
        this.recoveredResource = recoveredResource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryResourceRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(recoveredResource=" + recoveredResource + ")";
    }

    public XAPlusRecoveredResource getRecoveredResource() {
        return recoveredResource;
    }

    public interface Handler {
        void handleRecoveryResourceRequest(XAPlusRecoveryResourceRequestEvent event) throws InterruptedException;
    }
}