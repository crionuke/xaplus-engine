package org.xaplus.engine;

public class XAPlus {

    final XAPlusProperties properties;
    final XAPlusThreadPool threadPool;
    final XAPlusDispatcher dispatcher;
    final XAPlusResources resources;
    final XAPlusEngine engine;

    final XAPlusTickService tickService;
    final XAPlusTimerService timerService;
    final XAPlusManagerService managerService;
    final XAPlusPrepareOrderWaiterService prepareOrderWaiterService;
    final XAPlusPreparerService preparerService;
    final XAPlusCommitOrderWaiterService commitOrderWaiterService;
    final XAPlusCommitterService committerService;
    final XAPlusRollbackService rollbackService;
    final XAPlusRecoveryPreparerService recoveryPreparerService;
    final XAPlusRecoveryCommitterService recoveryCommitterService;
    final XAPlusJournalService journalService;
    final XAPlusService xaPlusService;

    public XAPlus(String serverId, int defaultTimeoutInSeconds) {
        properties = new XAPlusProperties(serverId, 128, defaultTimeoutInSeconds);
        threadPool = new XAPlusThreadPool();
        dispatcher = new XAPlusDispatcher();
        resources = new XAPlusResources();
        engine = new XAPlusEngine(properties, dispatcher, resources, new XAPlusUidGenerator(),
                new XAPlusThreadOfControl());
        tickService = new XAPlusTickService(properties, threadPool, dispatcher);
        timerService = new XAPlusTimerService(properties, threadPool, dispatcher);
        managerService = new XAPlusManagerService(properties, threadPool, dispatcher, resources);
        prepareOrderWaiterService =
                new XAPlusPrepareOrderWaiterService(properties, threadPool, dispatcher, new XAPlusTracker());
        preparerService = new XAPlusPreparerService(properties, threadPool, dispatcher, new XAPlusTracker());
        commitOrderWaiterService = new XAPlusCommitOrderWaiterService(properties, threadPool, dispatcher, resources);
        committerService = new XAPlusCommitterService(properties, threadPool, dispatcher, new XAPlusTracker());
        rollbackService = new XAPlusRollbackService(properties, threadPool, dispatcher, new XAPlusTracker());
        recoveryPreparerService = new XAPlusRecoveryPreparerService(properties, threadPool, dispatcher, resources,
                new XAPlusRecoveryPreparerTracker());
        recoveryCommitterService = new XAPlusRecoveryCommitterService(properties, threadPool, dispatcher, resources,
                new XAPlusRecoveryCommitterTracker(), new XAPlusRecoveryOrdersTracker(),
                new XAPlusRecoveryRetriesTracker());
        journalService = new XAPlusJournalService(properties, threadPool, dispatcher, new XAPlusTLog(serverId, engine));
        xaPlusService = new XAPlusService(properties, threadPool, dispatcher);
    }

    public void start() {
        tickService.postConstruct();
        timerService.postConstruct();
        managerService.postConstruct();
        prepareOrderWaiterService.postConstruct();
        preparerService.postConstruct();
        commitOrderWaiterService.postConstruct();
        committerService.postConstruct();
        rollbackService.postConstruct();
        recoveryPreparerService.postConstruct();
        recoveryCommitterService.postConstruct();
        journalService.postConstruct();
        xaPlusService.postConstruct();
    }

    public XAPlusEngine getEngine() {
        return engine;
    }
}
