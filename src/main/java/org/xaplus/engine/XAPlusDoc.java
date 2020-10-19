package org.xaplus.engine;

public class XAPlusDoc {
    private XAPlusTickService tickService;
    private XAPlusTimerService timerService;

    private XAPlusManagerService managerService;

    private XAPlusPrepareOrderWaiterService prepareOrderWaiterService;
    private XAPlusPreparerService preparerService;
    private XAPlusCommitOrderWaiterService commitOrderWaiterService;
    private XAPlusCommitterService committerService;

    private XAPlusRollbackService rollbackService;

    private XAPlusRecoveryPreparerService recoveryPreparerService;
    private XAPlusRecoveryCommitterService recoveryCommitterService;

    private XAPlusJournalService journalService;

    private XAPlusService xaPlusService;
}
