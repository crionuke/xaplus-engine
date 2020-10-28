package org.xaplus.engine;

import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusResult {

    private final boolean status;
    private final XAPlusRollbackException rollbackException;
    private final XAPlusTimeoutException timeoutException;

    XAPlusResult(boolean status) {
        this.status = status;
        rollbackException = null;
        timeoutException = null;
    }

    XAPlusResult(XAPlusRollbackException rollbackException) {
        this.status = false;
        this.rollbackException = rollbackException;
        timeoutException = null;
    }

    XAPlusResult(XAPlusTimeoutException timeoutException) {
        this.status = false;
        rollbackException = null;
        this.timeoutException = timeoutException;
    }

    boolean get() throws XAPlusRollbackException, XAPlusTimeoutException {
        if (rollbackException != null) {
            throw rollbackException;
        } else if (timeoutException != null) {
            throw timeoutException;
        } else {
            return status;
        }
    }
}
