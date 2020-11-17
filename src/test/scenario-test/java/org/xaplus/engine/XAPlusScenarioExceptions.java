package org.xaplus.engine;

class XAPlusScenarioExceptions {
    volatile boolean cancelledException;
    volatile boolean readiedException;
    volatile boolean failedException;
    volatile boolean commitException;
    volatile boolean rollbackException;
    volatile boolean doneException;
    volatile boolean retryException;

    XAPlusScenarioExceptions() {
        reset();
    }

    void reset() {
        cancelledException = false;
        readiedException = false;
        failedException = false;
        commitException = false;
        rollbackException = false;
        doneException = false;
        retryException = false;
    }
}
