package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDistributedTransactionInitialRequestEvent extends Event<XAPlusDistributedTransactionInitialRequestEvent.Handler> {

    private final long value;
    private final boolean beforeCommitException;

    public XAPlusDistributedTransactionInitialRequestEvent(long value, boolean beforeCommitException) {
        super();
        this.value = value;
        this.beforeCommitException = beforeCommitException;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDistributedTransactionInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public boolean isBeforeCommitException() {
        return beforeCommitException;
    }

    public interface Handler {
        void handleDistributedTransactionInitialRequest(XAPlusDistributedTransactionInitialRequestEvent event)
                throws InterruptedException;
    }
}