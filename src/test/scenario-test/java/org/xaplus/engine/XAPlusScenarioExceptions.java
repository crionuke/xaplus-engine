package org.xaplus.engine;

class XAPlusScenarioExceptions {
    volatile boolean readiedException;
    volatile boolean failedException;
    volatile boolean commitException;
    volatile boolean rollbackException;
    volatile boolean retryException;

    XAPlusScenarioExceptions() {
        reset();
    }

    void reset() {
        readiedException = false;
        failedException = false;
        commitException = false;
        rollbackException = false;
        retryException = false;
    }
}
