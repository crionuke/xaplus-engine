package org.xaplus.engine;

class XAPlusTestScenario {
    volatile boolean prepareException;
    volatile boolean readyException;
    volatile boolean commitException;
    volatile boolean rollbackException;
    volatile boolean doneException;
    volatile boolean retryException;
    volatile boolean absentException;

    XAPlusTestScenario() {
        this.prepareException = false;
        this.readyException = false;
        this.commitException = false;
        this.rollbackException = false;
        this.doneException = false;
        this.retryException = false;
        this.absentException = false;
    }
}
