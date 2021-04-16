package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLocalTransactionInitialRequestEvent extends Event<XAPlusLocalTransactionInitialRequestEvent.Handler> {

    private final long value;
    private final boolean beforeCommitException;

    public XAPlusLocalTransactionInitialRequestEvent(long value, boolean beforeCommitException) {
        super();
        this.value = value;
        this.beforeCommitException = beforeCommitException;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLocalTransactionInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public boolean isBeforeCommitException() {
        return beforeCommitException;
    }

    public interface Handler {
        void handleLocalTransactionInitialRequest(XAPlusLocalTransactionInitialRequestEvent event)
                throws InterruptedException;
    }
}