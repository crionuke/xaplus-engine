package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRetryFromSuperiorRequestEvent extends Event<XAPlusRetryFromSuperiorRequestEvent.Handler> {

    private final String serverId;
    private final XAPlusResource resource;

    public XAPlusRetryFromSuperiorRequestEvent(String serverId, XAPlusResource resource) {
        super();
        if (serverId == null) {
            throw new NullPointerException("serverId is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.serverId = serverId;
        this.resource = resource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRetryFromSuperiorRequest(this);
    }

    public String getServerId() {
        return serverId;
    }

    public XAPlusResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleRetryFromSuperiorRequest(XAPlusRetryFromSuperiorRequestEvent event) throws InterruptedException;
    }
}