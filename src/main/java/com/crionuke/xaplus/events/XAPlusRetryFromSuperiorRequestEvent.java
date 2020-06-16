package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRetryFromSuperiorRequestEvent extends Event<XAPlusRetryFromSuperiorRequestEvent.Handler> {

    private final XAPlusResource resource;

    public XAPlusRetryFromSuperiorRequestEvent(XAPlusResource resource) {
        super();
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.resource = resource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRetryFromSuperiorRequest(this);
    }

    public XAPlusResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleRetryFromSuperiorRequest(XAPlusRetryFromSuperiorRequestEvent event) throws InterruptedException;
    }
}