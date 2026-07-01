/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.shutdown_terminated_transaction_wait_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_database_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.shutdown_transaction_end_timeout;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.MULTI_VERSIONED;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LeaseExpired;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.kernel.impl.api.transaction.serial.DatabaseSerialGuard.EMPTY_GUARD;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.neo4j.collection.Dependencies;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.collection.factory.OnHeapCollectionsFactory;
import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.TransactionFailureHelper;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.DatabaseAccessMode;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseMonitors;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.database.PrivilegeDatabaseReference;
import org.neo4j.kernel.database.PrivilegeDatabaseReferenceImpl;
import org.neo4j.kernel.impl.api.chunk.TransactionRollbackProcess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitoringRecord;
import org.neo4j.kernel.impl.api.transaction.serial.DatabaseSerialGuard;
import org.neo4j.kernel.impl.api.transaction.serial.MultiVersionDatabaseSerialGuard;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.ExceptionHandlerService;
import org.neo4j.resources.CpuClock;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.ElementIdMapper;

/**
 * Central source of transactions in the database.
 * <p>
 * This class maintains references to all transactions, a pool of passive kernel transactions, and provides
 * capabilities
 * for enumerating all running transactions. During normal operation, acquiring new transactions and enumerating live
 * ones requires no synchronization (although the live list is not guaranteed to be exact).
 */
public class KernelTransactions extends LifecycleAdapter
        implements TransactionRegistry, Supplier<IdController.TransactionSnapshot>, IdController.IdFreeCondition {
    public static final long SYSTEM_TRANSACTION_ID = 0;
    private final LockManager lockManager;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final TransactionCommitProcess transactionCommitProcess;
    private final TransactionRollbackProcess rollbackProcess;
    private final DatabaseTransactionEventListeners eventListeners;
    private final TransactionMonitor transactionMonitor;
    private final GlobalMemoryGroupTracker transactionsMemoryPool;
    private final TransactionExecutionMonitor transactionExecutionMonitor;
    private final AvailabilityGuard databaseAvailabilityGuard;
    private final StorageEngine storageEngine;
    private final GlobalProcedures globalProcedures;
    private final DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider;
    private final TransactionIdStore transactionIdStore;
    private final KernelVersionProvider kernelVersionProvider;
    private final ServerIdentity serverIdentity;
    private final AtomicReference<CpuClock> cpuClockRef;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final SystemNanoClock clock;
    private final CursorContextFactory contextFactory;
    private final ReentrantReadWriteLock newTransactionsLock = new ReentrantReadWriteLock();
    private final TransactionIdSequence transactionIdSequence;
    private final TokenHolders tokenHolders;
    private final ElementIdMapper elementIdMapper;
    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;
    private final IdController.IdFreeCondition externalIdReuseCondition;
    private final TransactionCommitmentFactory commitmentFactory;
    private final TransactionIdGenerator transactionIdGenerator;
    private final DatabaseHealth databaseHealth;
    private final TransactionValidatorFactory transactionValidatorFactory;
    private final LogProvider internalLogProvider;
    private final NamedDatabaseId namedDatabaseId;
    private final IndexingService indexingService;
    private final IndexStatisticsStore indexStatisticsStore;
    private final Dependencies databaseDependencies;
    private final Config config;
    private final SchemaState schemaState;
    private final LeaseService leaseService;
    private final ExceptionHandlerService exceptionHandlerService;

    /**
     * Used to enumerate all transactions in the system, active and idle ones.
     * <p>
     * This data structure is *only* updated when brand-new transactions are created, or when transactions are disposed
     * of. During normal operation (where all transactions come from and are returned to the pool), this will be left
     * in peace, working solely as a collection of references to all transaction objects (idle and active) in the
     * database.
     * <p>
     * As such, it provides a good mechanism for listing all transactions without requiring synchronization when
     * starting and committing transactions.
     */
    private final Set<KernelTransactionImplementation> allTransactions = ConcurrentHashMap.newKeySet();

    private final MonitoredTransactionPool txPool;
    private final ConstraintSemantics constraintSemantics;
    private final AtomicInteger activeTransactionCounter = new AtomicInteger();
    private final ApplyEnrichmentStrategy enrichmentStrategy;
    private final DatabaseReferenceRepository databaseReferenceRepository;
    private final AbstractSecurityLog securityLog;
    private final boolean multiVersioned;
    private final TopologyGraphDbmsModel.HostedOnMode mode;
    private final DatabaseSerialGuard databaseSerialGuard;
    private final TransactionStateBehaviour transactionStateBehaviour;
    private final Log log;
    private final DatabaseMonitors databaseMonitors;
    private ScopedMemoryPool transactionMemoryPool;

    /**
     * Kernel transactions component status. True when stopped, false when started.
     * Will not allow to start new transaction by stopped instance of kernel transactions.
     * Should simplify tracking of stopped component usage by up the stack components.
     */
    private volatile boolean stopped = true;

    public KernelTransactions(
            Config config,
            LockManager lockManager,
            ConstraintIndexCreator constraintIndexCreator,
            TransactionCommitProcess transactionCommitProcess,
            TransactionRollbackProcess rollbackProcess,
            DatabaseTransactionEventListeners eventListeners,
            TransactionMonitor transactionMonitor,
            AvailabilityGuard databaseAvailabilityGuard,
            StorageEngine storageEngine,
            GlobalProcedures globalProcedures,
            DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider,
            TransactionIdStore transactionIdStore,
            KernelVersionProvider kernelVersionProvider,
            ServerIdentity serverIdentity,
            SystemNanoClock clock,
            AtomicReference<CpuClock> cpuClockRef,
            AccessCapabilityFactory accessCapabilityFactory,
            CursorContextFactory contextFactory,
            ConstraintSemantics constraintSemantics,
            SchemaState schemaState,
            TokenHolders tokenHolders,
            ElementIdMapper elementIdMapper,
            NamedDatabaseId namedDatabaseId,
            IndexingService indexingService,
            IndexStatisticsStore indexStatisticsStore,
            Dependencies databaseDependencies,
            DatabaseTracers tracers,
            LeaseService leaseService,
            GlobalMemoryGroupTracker transactionsMemoryPool,
            DatabaseReadOnlyChecker readOnlyDatabaseChecker,
            TransactionExecutionMonitor transactionExecutionMonitor,
            IdController.IdFreeCondition externalIdReuseCondition,
            TransactionCommitmentFactory commitmentFactory,
            TransactionIdSequence transactionIdSequence,
            TransactionIdGenerator transactionIdGenerator,
            DatabaseHealth databaseHealth,
            TransactionValidatorFactory transactionValidatorFactory,
            ExceptionHandlerService exceptionHandlerService,
            LogProvider internalLogProvider,
            TopologyGraphDbmsModel.HostedOnMode mode,
            DatabaseMonitors databaseMonitors) {
        this.config = config;
        this.lockManager = lockManager;
        this.constraintIndexCreator = constraintIndexCreator;
        this.transactionCommitProcess = transactionCommitProcess;
        this.rollbackProcess = rollbackProcess;
        this.eventListeners = eventListeners;
        this.transactionMonitor = transactionMonitor;
        this.transactionsMemoryPool = transactionsMemoryPool;
        this.transactionExecutionMonitor = transactionExecutionMonitor;
        this.databaseAvailabilityGuard = databaseAvailabilityGuard;
        this.storageEngine = storageEngine;
        this.globalProcedures = globalProcedures;
        this.dbmsRuntimeVersionProvider = dbmsRuntimeVersionProvider;
        this.transactionIdStore = transactionIdStore;
        this.kernelVersionProvider = kernelVersionProvider;
        this.serverIdentity = serverIdentity;
        this.cpuClockRef = cpuClockRef;
        this.accessCapabilityFactory = accessCapabilityFactory;
        this.tokenHolders = tokenHolders;
        this.elementIdMapper = elementIdMapper;
        this.readOnlyDatabaseChecker = readOnlyDatabaseChecker;
        this.externalIdReuseCondition = externalIdReuseCondition;
        this.commitmentFactory = commitmentFactory;
        this.transactionIdGenerator = transactionIdGenerator;
        this.databaseHealth = databaseHealth;
        this.transactionValidatorFactory = transactionValidatorFactory;
        this.exceptionHandlerService = exceptionHandlerService;
        this.internalLogProvider = internalLogProvider;
        this.namedDatabaseId = namedDatabaseId;
        this.indexingService = indexingService;
        this.indexStatisticsStore = indexStatisticsStore;
        this.databaseDependencies = databaseDependencies;
        this.contextFactory = contextFactory;
        this.clock = clock;
        this.constraintSemantics = constraintSemantics;
        this.schemaState = schemaState;
        this.leaseService = leaseService;
        this.transactionIdSequence = transactionIdSequence;
        this.multiVersioned = storageEngine.getOpenOptions().contains(MULTI_VERSIONED);
        this.mode = mode;
        this.log = internalLogProvider.getLog(KernelTransactions.class);
        this.databaseMonitors = databaseMonitors;
        this.txPool = new MonitoredTransactionPool(
                new GlobalKernelTransactionPool(allTransactions, new TransactionFactory(allTransactions, tracers)),
                activeTransactionCounter,
                config);
        this.enrichmentStrategy = this.databaseDependencies.resolveDependency(ApplyEnrichmentStrategy.class);
        this.databaseReferenceRepository = databaseDependencies.resolveDependency(DatabaseReferenceRepository.class);
        this.transactionStateBehaviour = new KernelTransactionsStateBehaviour(storageEngine, enrichmentStrategy);
        this.securityLog = this.databaseDependencies.resolveDependency(AbstractSecurityLog.class);
        this.databaseSerialGuard = multiVersioned ? new MultiVersionDatabaseSerialGuard(allTransactions) : EMPTY_GUARD;

        doBlockNewTransactions();
    }

    public KernelTransaction newInstance(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            TransactionTimeout timeout) {
        assertCurrentThreadIsNotBlockingNewTransactions();
        ProcedureView procedureView = globalProcedures.getCurrentView();
        BooleanSupplier isStale = () -> !globalProcedures.getCurrentView().equals(procedureView);
        PrivilegeDatabaseReference sessionDatabase = getDatabaseReference(loginContext);
        long startTimeMillis = clock.millis();
        SecurityContext securityContext = loginContext.authorize(
                new TokenHoldersIdLookup(tokenHolders, procedureView, isStale),
                sessionDatabase,
                securityLog,
                startTimeMillis);
        var tx = newKernelTransaction(type, clientInfo, timeout, securityContext, procedureView, startTimeMillis);
        databaseSerialGuard.acquireSerialLock(tx);
        return tx;
    }

    private PrivilegeDatabaseReference getDatabaseReference(LoginContext loginContext) {
        if (namedDatabaseId.isSystemDatabase()) {
            // avoid recursive update to databaseReferenceRepository
            return new DatabaseReferenceImpl.Internal(
                    new NormalizedDatabaseName(namedDatabaseId.name()), namedDatabaseId, true);
        } else if (LoginContext.AUTH_DISABLED.equals(loginContext)
                || (loginContext instanceof SecurityContext sc
                        && DatabaseAccessMode.FULL.equals(sc.databaseAccessMode()))) {
            // the repository does not contain databases until after they are stared.
            return new PrivilegeDatabaseReferenceImpl(namedDatabaseId.name(), null);
        }
        return databaseReferenceRepository.getByAlias(namedDatabaseId.name()).orElseThrow();
    }

    private KernelTransaction newKernelTransaction(
            KernelTransaction.Type type,
            ClientConnectionInfo clientInfo,
            TransactionTimeout timeout,
            SecurityContext securityContext,
            ProcedureView procedureView,
            long startTimeMillis) {
        try {
            while (!newTransactionsLock.readLock().tryLock(1, TimeUnit.SECONDS)) {
                assertRunning();
            }
            try {
                assertRunning();
                TransactionId lastCommittedTransaction = transactionIdStore.getLastCommittedTransaction();
                var tx = txPool.acquire();
                tx.initialize(
                        lastCommittedTransaction.id(),
                        type,
                        securityContext,
                        timeout,
                        transactionIdSequence.next(),
                        clientInfo,
                        procedureView,
                        startTimeMillis);
                return tx;
            } finally {
                newTransactionsLock.readLock().unlock();
            }
        } catch (InterruptedException ie) {
            Thread.interrupted();
            throw TransactionFailureHelper.failToStartTransaction(ie);
        }
    }

    /**
     * Give an approximate set of all transactions currently running.
     * This is not guaranteed to be exact, as transactions may stop and start while this set is gathered.
     *
     * @return the (approximate) set of open transactions.
     */
    @Override
    public Set<KernelTransactionHandle> activeTransactions() {
        return allTransactions.stream()
                .map(this::createHandle)
                .filter(KernelTransactionHandle::isOpen)
                .collect(toSet());
    }

    public long oldestActiveTransactionSequenceNumber() {
        return oldestActiveTransaction(true);
    }

    public long earliestTransactionSequenceNumber() {
        return oldestActiveTransaction(false);
    }

    private long oldestActiveTransaction(boolean filterTransactions) {
        long oldestTransactionSequenceNumber = Long.MAX_VALUE;
        for (KernelTransactionImplementation transaction : allTransactions) {
            if (filterTransactions && (transaction.isTerminated() || !transaction.isOpen())) {
                continue;
            }
            long tsn = transaction.getTransactionSequenceNumber();
            if (tsn != TransactionIdSequence.TRANSACTION_SEQUENCE_INITIAL_VALUE) {
                oldestTransactionSequenceNumber = Math.min(oldestTransactionSequenceNumber, tsn);
            }
        }
        return oldestTransactionSequenceNumber;
    }

    public long startTimeOfOldestExecutingTransaction() {
        long startTime = Long.MAX_VALUE;
        for (KernelTransactionImplementation transaction : allTransactions) {
            if (transaction.isOpen() || transaction.isClosing()) {
                startTime = Math.min(startTime, transaction.startTime());
            }
        }
        return startTime;
    }

    /**
     * Give an approximate set of all transactions currently executing. In contrast to {@link #activeTransactions}, this also includes transactions in the
     * closing state, e.g. committing or rolling back. This is not guaranteed to be exact, as transactions may stop and start while this set is gathered.
     *
     * @return the (approximate) set of executing transactions.
     */
    @Override
    public Set<KernelTransactionHandle> executingTransactions() {
        return allTransactions.stream()
                .map(this::createHandle)
                .filter(h -> h.isOpen() || h.isClosing())
                .collect(toSet());
    }

    public Iterable<TransactionMonitoringRecord> allTransactions() {
        return allTransactions.stream().map(this::createMonitoringRecord).toList();
    }

    @Override
    public void terminateTransactions() {
        // we mark all transactions for termination since we want to make sure these transactions
        // won't be reused, ever. Each transaction has, among other things, a Locks.Client and we
        // certainly want to keep that from being reused from this point.
        allTransactions.forEach(tx -> tx.markForTermination(Status.General.DatabaseUnavailable));
    }

    /**
     * Terminate all transactions that are not associated with the provided current lease id.
     */
    public void terminateOldLeaseTransactions(long currentLeaseId) {
        for (KernelTransactionImplementation tx : allTransactions) {
            if (tx.isDataTransaction() && isOldLease(tx, currentLeaseId)) {
                tx.markForTermination(LeaseExpired);
            }
        }
    }

    private static boolean isOldLease(KernelTransactionImplementation tx, long currentLeaseId) {
        int txLeaseId = tx.getLeaseId();
        return txLeaseId != NO_LEASE && currentLeaseId != txLeaseId;
    }

    @Override
    public boolean haveClosingTransaction() {
        return allTransactions.stream().anyMatch(KernelTransactionImplementation::isClosing);
    }

    @Override
    public boolean haveActiveTransaction() {
        return allTransactions.stream().anyMatch(KernelTransactionImplementation::isOpen);
    }

    @Override
    public void init() throws Exception {
        if (namedDatabaseId.equals(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)) {
            this.transactionMemoryPool = transactionsMemoryPool.newSystemDatabasePool(
                    namedDatabaseId.name(),
                    config.get(memory_transaction_database_max_size),
                    memory_transaction_database_max_size.name());
        } else {
            this.transactionMemoryPool = transactionsMemoryPool.newDatabasePool(
                    namedDatabaseId.name(),
                    config.get(memory_transaction_database_max_size),
                    memory_transaction_database_max_size.name());
        }
        config.addListener(
                memory_transaction_database_max_size, (before, after) -> transactionMemoryPool.setSize(after));
    }

    @Override
    public void start() {
        stopped = false;
        unblockNewTransactions();
    }

    @Override
    public void stop() {
        blockNewTransactions();
        stopped = true;
        waitAndTerminateRunningTransactions();
    }

    private void waitAndTerminateRunningTransactions() {
        log.info("Waiting for closing transactions.");

        var transactionShutdownMonitor = databaseMonitors.newMonitor(ShutdownTransactionMonitor.class);

        if (!newTransactionsLock.isWriteLocked()) {
            throw new IllegalStateException("Ability to start new transactions should be disabled.");
        }

        // give time for transactions to complete without termination
        if (haveActiveTransaction()) {
            transactionShutdownMonitor.awaitActiveTransactionClose();
            long completionDeadline = clock.millis()
                    + config.get(shutdown_transaction_end_timeout).toMillis();
            while (haveActiveTransaction() && clock.millis() < completionDeadline) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(10));
            }
        }

        // terminate transactions
        terminateTransactions();

        // Give transactions a short time to detect they are terminated
        if (haveActiveTransaction()) {
            transactionShutdownMonitor.awaitTerminatedTransactionClose();
            long waitTime =
                    config.get(shutdown_terminated_transaction_wait_timeout).toMillis();
            long deadline = clock.millis() + waitTime;
            while (haveActiveTransaction() && clock.millis() < deadline) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(10));
            }
        }

        if (haveClosingTransaction()) {
            transactionShutdownMonitor.awaitClosingTransactionClose();
            while (haveClosingTransaction()) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(10));
            }
        }

        if (haveActiveTransaction()) {
            log.warn("Failed to close all transactions. Shutdown may be unclean.");
        } else {
            log.info("All transactions are closed.");
        }
    }

    @Override
    public void shutdown() {
        // All transaction should be terminated/awaited to complete on stop
        try (var tempMemoryPool = transactionMemoryPool;
                var tempTxPool = txPool) {
        } finally {
            unblockNewTransactions(); // Release the lock before we discard this object
        }
    }

    @Override
    public IdController.TransactionSnapshot get() {
        return new IdController.TransactionSnapshot(
                transactionIdSequence.currentValue(),
                clock.millis(),
                transactionIdStore.getLastCommittedTransactionId(),
                transactionIdStore.getClosedTransactionSnapshot());
    }

    @Override
    public boolean eligibleForFreeing(IdController.TransactionSnapshot snapshot) {
        return externalIdReuseCondition.eligibleForFreeing(snapshot)
                && (snapshot.currentSequenceNumber() < oldestActiveTransactionSequenceNumber());
    }

    /**
     * Do not allow new transactions to start until {@link #unblockNewTransactions()} is called. Current thread have
     * responsibility of doing so.
     * <p>
     * Blocking call.
     */
    public void blockNewTransactions() {
        doBlockNewTransactions();
    }

    /**
     * This is private since it's called from the constructor.
     */
    private void doBlockNewTransactions() {
        newTransactionsLock.writeLock().lock();
    }

    /**
     * Allow new transactions to be started again if current thread is the one who called
     * {@link #blockNewTransactions()}.
     *
     * @throws IllegalStateException if current thread is not the one that called {@link #blockNewTransactions()}.
     */
    public void unblockNewTransactions() {
        if (!newTransactionsLock.writeLock().isHeldByCurrentThread()) {
            throw new IllegalStateException("This thread did not block transactions previously");
        }
        newTransactionsLock.writeLock().unlock();
    }

    public int getNumberOfActiveTransactions() {
        return activeTransactionCounter.get();
    }

    /**
     * Create new handle for the given transaction.
     * @param tx transaction to wrap.
     * @return transaction handle.
     */
    @VisibleForTesting
    KernelTransactionHandle createHandle(KernelTransactionImplementation tx) {
        return new KernelTransactionImplementationHandle(tx, clock);
    }

    TransactionMonitoringRecord createMonitoringRecord(KernelTransactionImplementation tx) {
        return new TransactionMonitoringRecord(tx, lookupInitializedContext(tx));
    }

    private static CursorContext lookupInitializedContext(KernelTransactionImplementation tx) {
        CursorContext cursorContext;
        do {
            cursorContext = tx.concurrentCursorContextLookup();
        } while (cursorContext == CursorContext.INITIALIZATION_SENTINEL_CONTEXT);
        return cursorContext;
    }

    private void assertRunning() {
        if (!databaseAvailabilityGuard.isAvailable()) {
            throw DatabaseShutdownException.databaseUnavailable(namedDatabaseId.name());
        }
        if (stopped) {
            throw new IllegalStateException("Can't start new transaction with stopped " + getClass());
        }
    }

    private void assertCurrentThreadIsNotBlockingNewTransactions() {
        if (newTransactionsLock.isWriteLockedByCurrentThread()) {
            throw new IllegalStateException(
                    "Thread that is blocking new transactions from starting can't start new transaction");
        }
    }

    private class TransactionFactory implements Factory<KernelTransactionImplementation> {
        private final Set<KernelTransactionImplementation> transactions;
        private final DatabaseTracers tracers;

        TransactionFactory(Set<KernelTransactionImplementation> transactions, DatabaseTracers tracers) {
            this.transactions = transactions;
            this.tracers = tracers;
        }

        @Override
        public KernelTransactionImplementation newInstance() {
            KernelTransactionImplementation tx = ktiFactory()
                    .createTransactionImplementation(
                            config,
                            eventListeners,
                            constraintIndexCreator,
                            transactionCommitProcess,
                            rollbackProcess,
                            transactionMonitor,
                            txPool,
                            clock,
                            cpuClockRef,
                            tracers,
                            storageEngine,
                            accessCapabilityFactory,
                            contextFactory,
                            OnHeapCollectionsFactory.INSTANCE,
                            constraintSemantics,
                            schemaState,
                            tokenHolders,
                            elementIdMapper,
                            indexingService,
                            indexStatisticsStore,
                            databaseDependencies,
                            namedDatabaseId,
                            leaseService,
                            transactionMemoryPool,
                            readOnlyDatabaseChecker,
                            transactionExecutionMonitor,
                            securityLog,
                            lockManager,
                            commitmentFactory,
                            KernelTransactions.this,
                            transactionIdGenerator,
                            dbmsRuntimeVersionProvider,
                            kernelVersionProvider,
                            serverIdentity,
                            enrichmentStrategy,
                            transactionStateBehaviour,
                            databaseHealth,
                            internalLogProvider,
                            transactionValidatorFactory,
                            databaseSerialGuard,
                            multiVersioned,
                            exceptionHandlerService,
                            mode,
                            databaseAvailabilityGuard);
            this.transactions.add(tx);
            return tx;
        }
    }

    protected KernelTransactionImplementationFactory ktiFactory() {
        return KernelTransactionImplementation::new;
    }

    private static class GlobalKernelTransactionPool extends LinkedQueuePool<KernelTransactionImplementation> {
        private final Set<KernelTransactionImplementation> transactions;

        GlobalKernelTransactionPool(
                Set<KernelTransactionImplementation> transactions, Factory<KernelTransactionImplementation> factory) {
            super(8, factory);
            this.transactions = transactions;
        }

        @Override
        public void dispose(KernelTransactionImplementation tx) {
            transactions.remove(tx);
            tx.dispose();
            super.dispose(tx);
        }
    }

    static class MonitoredTransactionPool implements Pool<KernelTransactionImplementation> {
        private final AtomicInteger activeTransactionCounter;
        private final GlobalKernelTransactionPool delegate;
        private volatile int maxNumberOfTransaction;

        MonitoredTransactionPool(
                GlobalKernelTransactionPool delegate, AtomicInteger activeTransactionCounter, Config config) {
            this.delegate = delegate;
            this.activeTransactionCounter = activeTransactionCounter;
            this.maxNumberOfTransaction = config.get(GraphDatabaseSettings.max_concurrent_transactions);
            config.addListener(
                    GraphDatabaseSettings.max_concurrent_transactions,
                    (oldValue, newValue) -> maxNumberOfTransaction = newValue);
        }

        @Override
        public KernelTransactionImplementation acquire() {
            verifyTransactionsLimit();
            return delegate.acquire();
        }

        @Override
        public void release(KernelTransactionImplementation txn) {
            activeTransactionCounter.decrementAndGet();
            delegate.release(txn);
        }

        @Override
        public void dispose(KernelTransactionImplementation txn) {
            activeTransactionCounter.decrementAndGet();
            delegate.dispose(txn);
        }

        @Override
        public void close() {
            delegate.close();
        }

        private void verifyTransactionsLimit() {
            int activeTransactions;
            do {
                activeTransactions = activeTransactionCounter.get();
                int localTransactionMaximum = maxNumberOfTransaction;
                if (localTransactionMaximum != 0 && activeTransactions >= localTransactionMaximum) {
                    throw MaximumTransactionLimitExceededException.maximumNumberOfTransactionsExceeded();
                }
            } while (!activeTransactionCounter.weakCompareAndSetAcquire(activeTransactions, activeTransactions + 1));
        }
    }

    protected interface KernelTransactionImplementationFactory {
        KernelTransactionImplementation createTransactionImplementation(
                Config externalConfig,
                DatabaseTransactionEventListeners transactionEventListeners,
                ConstraintIndexCreator constraintIndexCreator,
                TransactionCommitProcess commitProcess,
                TransactionRollbackProcess rollbackProcess,
                TransactionMonitor transactionMonitor,
                Pool<KernelTransactionImplementation> pool,
                SystemNanoClock clock,
                AtomicReference<CpuClock> cpuClockRef,
                DatabaseTracers tracers,
                StorageEngine storageEngine,
                AccessCapabilityFactory accessCapabilityFactory,
                CursorContextFactory contextFactory,
                CollectionsFactory collectionsFactory,
                ConstraintSemantics constraintSemantics,
                SchemaState schemaState,
                TokenHolders tokenHolders,
                ElementIdMapper elementIdMapper,
                IndexingService indexingService,
                IndexStatisticsStore indexStatisticsStore,
                Dependencies dependencies,
                NamedDatabaseId namedDatabaseId,
                LeaseService leaseService,
                ScopedMemoryPool dbTransactionsPool,
                DatabaseReadOnlyChecker readOnlyDatabaseChecker,
                TransactionExecutionMonitor transactionExecutionMonitor,
                AbstractSecurityLog securityLog,
                LockManager lockManager,
                TransactionCommitmentFactory commitmentFactory,
                KernelTransactions kernelTransactions,
                TransactionIdGenerator transactionIdGenerator,
                DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider,
                KernelVersionProvider kernelVersionProvider,
                ServerIdentity serverIdentity,
                ApplyEnrichmentStrategy enrichmentStrategy,
                TransactionStateBehaviour transactionStateBehaviour,
                DatabaseHealth databaseHealth,
                LogProvider logProvider,
                TransactionValidatorFactory transactionValidatorFactory,
                DatabaseSerialGuard databaseSerialGuard,
                boolean multiVersioned,
                ExceptionHandlerService exceptionHandlerService,
                TopologyGraphDbmsModel.HostedOnMode mode,
                AvailabilityGuard availabilityGuard);
    }
}
