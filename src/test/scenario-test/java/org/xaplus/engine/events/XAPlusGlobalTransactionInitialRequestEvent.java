package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusGlobalTransactionInitialRequestEvent extends Event<XAPlusGlobalTransactionInitialRequestEvent.Handler> {

    private final long value;
    private final boolean superiorBeforeRequestException;
    private final boolean superiorBeforeCommitException;
    private final boolean subordinateBeforeCommitException;

    public XAPlusGlobalTransactionInitialRequestEvent(long value,
                                                      boolean superiorBeforeRequestException,
                                                      boolean superiorBeforeCommitException,
                                                      boolean subordinateBeforeCommitException) {
        super();
        this.value = value;
        this.superiorBeforeRequestException = superiorBeforeRequestException;
        this.superiorBeforeCommitException = superiorBeforeCommitException;
        this.subordinateBeforeCommitException = subordinateBeforeCommitException;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleGlobalTransactionInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public boolean isSuperiorBeforeRequestException() {
        return superiorBeforeRequestException;
    }

    public boolean isSuperiorBeforeCommitException() {
        return superiorBeforeCommitException;
    }

    public boolean isSubordinateBeforeCommitException() {
        return subordinateBeforeCommitException;
    }

    public interface Handler {
        void handleGlobalTransactionInitialRequest(XAPlusGlobalTransactionInitialRequestEvent event) throws InterruptedException;
    }
}