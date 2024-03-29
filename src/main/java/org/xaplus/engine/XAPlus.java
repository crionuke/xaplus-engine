package org.xaplus.engine;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public class XAPlus {

    final XAPlusProperties properties;
    final XAPlusThreadPool threadPool;
    final XAPlusDispatcher dispatcher;
    final XAPlusResources resources;
    final XAPlusEngine engine;

    final XAPlusTickService tickService;
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

    public XAPlus(String serverId, int transactionsTimeoutInSeconds, int recoveryTimeoutInSeconds,
                  int recoveryPeriodInSeconds) {
        properties = new XAPlusProperties(serverId, 128,
                transactionsTimeoutInSeconds, recoveryTimeoutInSeconds, recoveryPeriodInSeconds);
        threadPool = new XAPlusThreadPool();
        dispatcher = new XAPlusDispatcher();
        resources = new XAPlusResources();
        engine = new XAPlusEngine(properties, dispatcher, resources, new XAPlusThreadOfControl());
        tickService = new XAPlusTickService(properties, threadPool, dispatcher);
        managerService = new XAPlusManagerService(properties, threadPool, dispatcher);

        subordinateCommitterService = new XAPlusSubordinateCommitterService(properties, threadPool, dispatcher);
        subordinatePreparerService =
                new XAPlusSubordinatePreparerService(properties, threadPool, dispatcher, resources);
        subordinateRollbackService = new XAPlusSubordinateRollbackService(properties, threadPool, dispatcher);
        superiorCommitterService = new XAPlusSuperiorCommitterService(properties, threadPool, dispatcher);
        superiorPreparerService = new XAPlusSuperiorPreparerService(properties, threadPool, dispatcher);
        superiorRollbackService = new XAPlusSuperiorRollbackService(properties, threadPool, dispatcher);
        recoveryPreparerService = new XAPlusRecoveryPreparerService(properties, threadPool, dispatcher, resources);
        recoveryCommitterService = new XAPlusRecoveryCommitterService(properties, threadPool, dispatcher, resources);
        journalService = new XAPlusJournalService(properties, threadPool, dispatcher, resources,
                new XAPlusTLog(serverId, engine));
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
