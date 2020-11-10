package org.xaplus.engine;

class XAPlusTestScenario {
    volatile boolean cancelledException;
    volatile boolean readiedException;
    volatile boolean failedException;
    volatile boolean commitException;
    volatile boolean rollbackException;
    volatile boolean doneException;
    volatile boolean retryException;

    XAPlusTestScenario() {
        this.cancelledException = false;
        this.readiedException = false;
        this.failedException = false;
        this.commitException = false;
        this.rollbackException = false;
        this.doneException = false;
        this.retryException = false;
    }
}
