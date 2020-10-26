package org.xaplus.engine.events.xaplus;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRemoteSubordinateRetryRequestEvent extends Event<XAPlusRemoteSubordinateRetryRequestEvent.Handler> {

    private final String subordinateServerId;

    public XAPlusRemoteSubordinateRetryRequestEvent(String subordinateServerId) {
        super();
        if (subordinateServerId == null) {
            throw new NullPointerException("subordinateServerId is null");
        }
        this.subordinateServerId = subordinateServerId;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRemoteSubordinateRetryRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(subordinateServerId=" + subordinateServerId + ")";
    }

    public String getSubordinateServerId() {
        return subordinateServerId;
    }

    public interface Handler {
        void handleRemoteSubordinateRetryRequest(XAPlusRemoteSubordinateRetryRequestEvent event) throws InterruptedException;
    }
}