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

    final XAPlusSubordinateCommitterService subordinateCommitterService;
    final XAPlusSubordinateCompleterService subordinateCompleterService;
    final XAPlusSubordinatePreparerService subordinatePreparerService;
    final XAPlusSubordinateRollbackService subordinateRollbackService;

    final XAPlusSuperiorCommitterService superiorCommitterService;
    final XAPlusSuperiorCompleterService superiorCompleterService;
    final XAPlusSuperiorPreparerService superiorPreparerService;
    final XAPlusSuperiorRollbackService superiorRollbackService;

    final XAPlusRecoveryPreparerService recoveryPreparerService;
    final XAPlusRecoveryCommitterService recoveryCommitterService;
    final XAPlusJournalService journalService;
    final XAPlusService xaPlusService;

    public XAPlus(String serverId, int defaultTimeoutInSeconds) {
        properties = new XAPlusProperties(serverId, 128, defaultTimeoutInSeconds);
        threadPool = new XAPlusThreadPool();
        dispatcher = new XAPlusDispatcher();
        resources = new XAPlusResources();
        engine = new XAPlusEngine(properties, dispatcher, resources, new XAPlusThreadOfControl());
        tickService = new XAPlusTickService(properties, threadPool, dispatcher);
        timerService = new XAPlusTimerService(properties, threadPool, dispatcher, new XAPlusTimerState());
        managerService = new XAPlusManagerService(properties, threadPool, dispatcher, resources);

        subordinateCommitterService = new XAPlusSubordinateCommitterService(properties, threadPool, dispatcher,
                resources, new XAPlusTracker());
        subordinateCompleterService = new XAPlusSubordinateCompleterService(properties, threadPool, dispatcher,
                resources, new XAPlusTracker());
        subordinatePreparerService = new XAPlusSubordinatePreparerService(properties, threadPool, dispatcher,
                resources, new XAPlusTracker());
        subordinateRollbackService = new XAPlusSubordinateRollbackService(properties, threadPool, dispatcher,
                new XAPlusTracker());
        superiorCommitterService = new XAPlusSuperiorCommitterService(properties, threadPool, dispatcher,
                resources, new XAPlusTracker());
        superiorCompleterService = new XAPlusSuperiorCompleterService(properties, threadPool, dispatcher);
        superiorPreparerService = new XAPlusSuperiorPreparerService(properties, threadPool, dispatcher,
                resources, new XAPlusTracker());
        superiorRollbackService = new XAPlusSuperiorRollbackService(properties, threadPool, dispatcher,
                new XAPlusTracker());
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
        subordinateCommitterService.postConstruct();
        subordinateCompleterService.postConstruct();
        subordinatePreparerService.postConstruct();
        subordinateRollbackService.postConstruct();
        superiorCommitterService.postConstruct();
        superiorCompleterService.postConstruct();
        superiorPreparerService.postConstruct();
        superiorRollbackService.postConstruct();
        recoveryPreparerService.postConstruct();
        recoveryCommitterService.postConstruct();
        journalService.postConstruct();
        xaPlusService.postConstruct();
    }

    public XAPlusEngine getEngine() {
        return engine;
    }
}
