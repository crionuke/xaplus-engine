package org.xaplus.engine;

import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusFuture {

    private final BlockingQueue<XAPlusResult> container;

    XAPlusFuture() {
        container = new ArrayBlockingQueue<>(1);
    }

    public boolean get()
            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusResult result = container.take();
        return result.get();
    }

    public boolean get(long timeout, TimeUnit unit)
            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusResult result = container.poll(timeout, unit);
        return result.get();
    }

    void put(XAPlusResult result) throws InterruptedException {
        container.put(result);
    }
}
