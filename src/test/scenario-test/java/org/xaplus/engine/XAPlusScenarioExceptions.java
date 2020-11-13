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
        this.cancelledException = false;
        this.readiedException = false;
        this.failedException = false;
        this.commitException = false;
        this.rollbackException = false;
        this.doneException = false;
        this.retryException = false;
    }
}
