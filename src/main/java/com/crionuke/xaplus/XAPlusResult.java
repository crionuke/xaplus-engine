package com.crionuke.xaplus;

import com.crionuke.xaplus.exceptions.XAPlusCommitException;
import com.crionuke.xaplus.exceptions.XAPlusRollbackException;
import com.crionuke.xaplus.exceptions.XAPlusTimeoutException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusResult {

    private final XAPlusCommitException commitException;
    private final XAPlusRollbackException rollbackException;
    private final XAPlusTimeoutException timeoutException;

    XAPlusResult() {
        commitException = null;
        rollbackException = null;
        timeoutException = null;
    }

    XAPlusResult(XAPlusCommitException commitException) {
        this.commitException = commitException;
        rollbackException = null;
        timeoutException = null;
    }

    XAPlusResult(XAPlusRollbackException rollbackException) {
        commitException = null;
        this.rollbackException = rollbackException;
        timeoutException = null;
    }

    XAPlusResult(XAPlusTimeoutException timeoutException) {
        commitException = null;
        rollbackException = null;
        this.timeoutException = timeoutException;
    }

    boolean get() throws XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        if (commitException != null) {
            throw commitException;
        } else if (rollbackException != null) {
            throw rollbackException;
        } else if (timeoutException != null) {
            throw timeoutException;
        } else {
            return true;
        }
    }
}
