package org.xaplus.engine;

public class XAPlus {

    private final static int MAX_SERVER_ID_LENGTH = 51;

    final XAPlusProperties properties;
    final XAPlusThreadPool threadPool;
    final XAPlusDispatcher dispatcher;
    final XAPlusResources resources;
    final XAPlusEngine engine;

    final XAPlusTickService tickService;
    final XAPlusTimerService timerService;
    final XAPlusManagerService managerService;

    final XAPlusSubordinateCommitterService subordinateCommitterService;
    final XAPlusSubordinatePreparerService subordinatePreparerService;
    final XAPlusSubordinateRollbackService subordinateRollbackService;

    final XAPlusSuperiorCommitterService superiorCommitterService;
    final XAPlusSuperiorPreparerService superiorPreparerService;
    final XAPlusSuperiorRollbackService superiorRollbackService;

    final XAPlusRecoveryPreparerService recoveryPreparerService;
    final XAPlusRecoveryCommitterService recoveryCommitterService;
    final XAPlusJournalService journalService;
    final XAPlusService xaPlusService;

    private boolean constructed;

    public XAPlus(String serverId, int defaultTimeoutInSeconds) {
        if (serverId == null) {
            throw new NullPointerException("serverId is null");
        }
        if (serverId.length() > MAX_SERVER_ID_LENGTH) {
            throw new IllegalArgumentException("too long serverId, limited by " + MAX_SERVER_ID_LENGTH + " bytes, " +
                    "serverId=" + serverId);
        }
        if (defaultTimeoutInSeconds <= 0) {
            throw new IllegalArgumentException("default timeout must be greater zero, " +
                    "defaultTimeoutInSeconds=" + defaultTimeoutInSeconds);
        }
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
        subordinatePreparerService = new XAPlusSubordinatePreparerService(properties, threadPool, dispatcher,
                resources, new XAPlusTracker());
        subordinateRollbackService = new XAPlusSubordinateRollbackService(properties, threadPool, dispatcher,
                resources, new XAPlusTracker());
        superiorCommitterService = new XAPlusSuperiorCommitterService(properties, threadPool, dispatcher,
                new XAPlusTracker());
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

        constructed = false;
    }

    public synchronized XAPlusEngine construct() {
        if (constructed) {
            throw new IllegalStateException("XAPlusEngine already constructed");
        } else {
            constructed = true;
        }
        tickService.postConstruct();
        timerService.postConstruct();
        managerService.postConstruct();
        subordinateCommitterService.postConstruct();
        subordinatePreparerService.postConstruct();
        subordinateRollbackService.postConstruct();
        superiorCommitterService.postConstruct();
        superiorPreparerService.postConstruct();
        superiorRollbackService.postConstruct();
        recoveryPreparerService.postConstruct();
        recoveryCommitterService.postConstruct();
        journalService.postConstruct();
        xaPlusService.postConstruct();
        return engine;
    }
}
