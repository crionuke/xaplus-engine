package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryResourceRequestEvent extends Event<XAPlusRecoveryResourceRequestEvent.Handler> {

    private final String uniqueName;
    private final XAResource resource;

    public XAPlusRecoveryResourceRequestEvent(String uniqueName, XAResource resource) {
        super();
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.resource = resource;
        this.uniqueName = uniqueName;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryResourceRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(uniqueName=" + uniqueName + ")";
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public XAResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleRecoveryResourceRequest(XAPlusRecoveryResourceRequestEvent event) throws InterruptedException;
    }
}