package org.xaplus.engine;

class XAPlusGlobalTransactionScenarioExceptions {
    volatile boolean readyException;
    volatile boolean failedException;
    volatile boolean commitException;
    volatile boolean rollbackException;
    volatile boolean retryException;

    XAPlusGlobalTransactionScenarioExceptions() {
        reset();
    }

    void reset() {
        readyException = false;
        failedException = false;
        commitException = false;
        rollbackException = false;
        retryException = false;
    }
}
