package org.xaplus.engine;

class XAPlusScenarioExceptions {
    volatile boolean readyException;
    volatile boolean failedException;
    volatile boolean commitException;
    volatile boolean rollbackException;
    volatile boolean retryException;

    XAPlusScenarioExceptions() {
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
