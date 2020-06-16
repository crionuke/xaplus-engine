package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRetryRequestFromSubordinateEvent extends Event<XAPlusRetryRequestFromSubordinateEvent.Handler> {

    private final String subordinateServerId;

    public XAPlusRetryRequestFromSubordinateEvent(String subordinateServerId) {
        super();
        if (subordinateServerId == null) {
            throw new NullPointerException("subordinateServerId is null");
        }
        this.subordinateServerId = subordinateServerId;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRetryRequestFromSubordinate(this);
    }

    public String getSubordinateServerId() {
        return subordinateServerId;
    }

    public interface Handler {
        void handleRetryRequestFromSubordinate(XAPlusRetryRequestFromSubordinateEvent event) throws InterruptedException;
    }
}