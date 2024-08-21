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

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_sampling_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.kernel.impl.api.transaction.trace.TraceProviderFactory.getTraceProvider;
import static org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace.NONE;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.neo4j.collection.Dependencies;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.LocalConfig;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnspecifiedKernelException;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.EntityLocks;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.AccessModeProvider;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.database.enrichment.TxEnrichmentVisitor;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.chunk.ChunkSink;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransactionSink;
import org.neo4j.kernel.impl.api.chunk.TransactionRollbackProcess;
import org.neo4j.kernel.impl.api.commit.ChunkCommitter;
import org.neo4j.kernel.impl.api.commit.DefaultCommitter;
import org.neo4j.kernel.impl.api.commit.TransactionCommitter;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextCursorTracer;
import org.neo4j.kernel.impl.api.parallel.ParallelAccessCheck;
import org.neo4j.kernel.impl.api.parallel.ThreadExecutionContext;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.transaction.serial.DatabaseSerialGuard;
import org.neo4j.kernel.impl.api.transaction.serial.SerialExecutionGuard;
import org.neo4j.kernel.impl.api.transaction.serial.TransactionSerialExecutionGuard;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors;
import org.neo4j.kernel.impl.newapi.IndexTxStateUpdater;
import org.neo4j.kernel.impl.newapi.KernelProcedures;
import org.neo4j.kernel.impl.newapi.KernelRead;
import org.neo4j.kernel.impl.newapi.KernelSchemaRead;
import org.neo4j.kernel.impl.newapi.KernelToken;
import org.neo4j.kernel.impl.newapi.KernelTokenRead;
import org.neo4j.kernel.impl.newapi.Operations;
import org.neo4j.kernel.impl.newapi.TransactionQueryContext;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.TransactionEventListeners;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.enrichment.CaptureMode;
import org.neo4j.storageengine.api.enrichment.EnrichmentMode;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor.Decorator;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;
import org.neo4j.storageengine.api.txstate.validation.ValidationLockDumper;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.ElementIdMapper;

public class KernelTransactionImplementation implements KernelTransaction, TxStateHolder, ExecutionStatistics {
    /*
     * IMPORTANT:
     * This class is pooled and re-used. If you add *any* state to it, you *must* make sure that:
     *   - the #initialize() method resets that state for re-use
     *   - the #release() method releases resources acquired in #initialize() or during the transaction's life time
     */

    // default values for not committed tx id and tx commit time
    private static final long NOT_COMMITTED_TRANSACTION_ID = -1;
    private static final long NOT_COMMITTED_TRANSACTION_COMMIT_TIME = -1;
    private static final String TRANSACTION_TAG = "transaction";
    private static final VarHandle CURSOR_CONTEXT_HANDLE;

    static {
        try {
            CURSOR_CONTEXT_HANDLE = MethodHandles.lookup()
                    .in(KernelTransactionImplementation.class)
                    .findVarHandle(KernelTransactionImplementation.class, "cursorContext", CursorContext.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final CollectionsFactory collectionsFactory;

    // Logic
    private final TransactionEventListeners transactionEventListeners;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final StorageEngine storageEngine;
    private final TransactionTracer transactionTracer;
    private final Pool<KernelTransactionImplementation> pool;

    // For committing
    private final TransactionCommitProcess commitProcess;
    private final TransactionRollbackProcess rollbackProcess;
    private final TransactionMonitor transactionMonitor;
    private final TransactionExecutionMonitor transactionExecutionMonitor;
    private final LeaseService leaseService;
    private final StorageReader storageReader;
    private final CommandCreationContext commandCreationContext;
    private final KernelVersionProvider kernelVersionProvider;
    private final ServerIdentity serverIdentity;
    private final NamedDatabaseId namedDatabaseId;
    private final TransactionClockContext clocks;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final ConstraintSemantics constraintSemantics;
    private final TransactionMemoryPool transactionMemoryPool;
    private final LogProvider logProvider;
    private final CursorContextFactory contextFactory;
    private final EntityLocks entityLocks;
    private final KernelProcedures.ForTransactionScope procedures;
    private final KernelSchemaRead schemaRead;
    private final KernelRead kernelRead;
    // For concurrent access by monitoring, jobs, etc CURSOR_CONTEXT_HANDLE should be used
    @SuppressWarnings("FieldMayBeFinal")
    private CursorContext cursorContext;

    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;
    private final TransactionIdGenerator transactionIdGenerator;
    private final ApplyEnrichmentStrategy enrichmentStrategy;
    private final DatabaseHealth databaseHealth;
    private final SecurityAuthorizationHandler securityAuthorizationHandler;

    // State that needs to be reset between uses. Most of these should be cleared or released in #release(),
    // whereas others, such as timestamp or txId when transaction starts, even locks, needs to be set in #initialize().
    private TxState txState;
    private volatile TransactionWriteState writeState;
    private AccessCapability accessCapability;
    private final KernelStatement currentStatement;
    private OverridableSecurityContext overridableSecurityContext;
    private final LockManager.Client lockClient;
    private volatile long transactionSequenceNumber;
    private LeaseClient leaseClient;
    private volatile boolean closing;
    private volatile boolean closed;
    private boolean commit;

    private volatile TerminationMark terminationMark;
    private long startTimeMillis;
    private volatile long startTimeNanos;
    private volatile TransactionTimeout timeout;
    private long lastTransactionIdWhenStarted;
    private final Statistics statistics;
    private TransactionEvent transactionEvent;
    private Type type;
    private volatile long transactionId;
    private volatile long commitTime;
    private volatile ClientConnectionInfo clientInfo;
    private volatile Map<String, Object> userMetaData;
    private volatile String statusDetails;
    private final QueryContext queryContext;
    private final Operations operations;
    private InternalTransaction internalTransaction;
    private volatile TraceProvider traceProvider;
    private volatile TransactionInitializationTrace initializationTrace;
    private final MemoryTracker memoryTracker;
    private final LocalConfig config;
    private volatile long transactionHeapBytesLimit;
    private final ExecutionContextFactory executionContextFactory;
    private ProcedureView procedureView;
    private boolean needsHighIdTracking;

    /**
     * Lock prevents transaction {@link #markForTermination(Status)}  transaction termination} from interfering with
     * {@link #close() transaction commit} and specifically with {@link #reset()}.
     * Termination can run concurrently with commit and we need to make sure that it terminates the right lock client
     * and the right transaction (with the right {@link #transactionSequenceNumber}) because {@link KernelTransactionImplementation}
     * instances are pooled.
     */
    private final Lock terminationReleaseLock = new ReentrantLock();

    private KernelTransactionMonitor kernelTransactionMonitor;
    private final StoreCursors transactionalCursors;

    private final KernelTransactions kernelTransactions;
    /**
     * This transaction's inner transactions' ids.
     */
    private volatile InnerTransactionHandlerImpl innerTransactionHandler;

    private final TransactionValidator transactionValidator;
    private final ValidationLockDumper validationLockDumper;
    private final TransactionCommitter committer;
    private final ChunkedTransactionSink txStateWriter;
    private final DatabaseSerialGuard databaseSerialGuard;
    private final SerialExecutionGuard serialExecutionGuard;
    private boolean failedCleanup = false;

    public KernelTransactionImplementation(
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
            CollectionsFactorySupplier collectionsFactorySupplier,
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
            DatabaseHealth databaseHealth,
            LogProvider logProvider,
            TransactionValidatorFactory transactionValidatorFactory,
            DatabaseSerialGuard databaseSerialGuard,
            boolean multiVersioned) {
        this.logProvider = logProvider;
        this.closed = true;
        this.timeout = TransactionTimeout.NO_TIMEOUT;
        this.config = new LocalConfig(externalConfig);
        this.accessCapabilityFactory = accessCapabilityFactory;
        this.contextFactory = contextFactory;
        this.cursorContext = CursorContext.NULL_CONTEXT;
        this.readOnlyDatabaseChecker = readOnlyDatabaseChecker;
        this.transactionIdGenerator = transactionIdGenerator;
        this.databaseHealth = databaseHealth;
        this.transactionMemoryPool = new TransactionMemoryPool(dbTransactionsPool, config, () -> !closed, logProvider);
        this.memoryTracker = transactionMemoryPool.getTransactionTracker();
        this.constraintIndexCreator = constraintIndexCreator;
        this.commitProcess = commitProcess;
        this.rollbackProcess = rollbackProcess;
        this.transactionMonitor = transactionMonitor;
        this.transactionExecutionMonitor = transactionExecutionMonitor;
        this.storageReader = storageEngine.newReader();
        this.commandCreationContext = storageEngine.newCommandCreationContext(multiVersioned);
        this.kernelVersionProvider = kernelVersionProvider;
        this.serverIdentity = serverIdentity;
        this.enrichmentStrategy = enrichmentStrategy;
        this.namedDatabaseId = namedDatabaseId;
        this.storageEngine = storageEngine;
        this.pool = pool;
        this.clocks = new TransactionClockContext(clock);
        this.transactionTracer = tracers.getDatabaseTracer();
        this.leaseService = leaseService;
        this.currentStatement =
                new KernelStatement(this, tracers.getLockTracer(), this.clocks, cpuClockRef, namedDatabaseId, config);
        this.statistics = new Statistics(
                this,
                cpuClockRef,
                config.get(GraphDatabaseInternalSettings.enable_transaction_heap_allocation_tracking));
        this.userMetaData = emptyMap();
        this.statusDetails = EMPTY;
        this.constraintSemantics = constraintSemantics;
        this.transactionalCursors = storageEngine.createStorageCursors(CursorContext.NULL_CONTEXT);
        this.lockClient = ParallelAccessCheck.maybeWrapLockClient(lockManager.newClient());
        StorageLocks storageLocks = storageEngine.createStorageLocks(lockClient);
        DefaultPooledCursors cursors = new DefaultPooledCursors(
                storageReader, transactionalCursors, config, storageEngine.indexingBehaviour(), multiVersioned);
        this.securityAuthorizationHandler = new SecurityAuthorizationHandler(securityLog);
        var kernelToken = new KernelToken(storageReader, commandCreationContext, this, tokenHolders);
        this.queryContext = new TransactionQueryContext(
                this::dataRead, cursors, this, this::cursorContext, memoryTracker, indexingService.getMonitor());
        this.entityLocks = new EntityLocks(
                storageLocks, currentStatement::lockTracer, lockClient, this::assertOpenWithParallelAccessCheck);
        this.procedures =
                new KernelProcedures.ForTransactionScope(this, dependencies, this::assertOpenWithParallelAccessCheck);
        AccessModeProvider accessModeProvider = () -> securityContext().mode();
        this.schemaRead = new KernelSchemaRead(
                schemaState,
                indexStatisticsStore,
                storageReader,
                entityLocks,
                this,
                indexingService,
                this::assertOpenWithParallelAccessCheck,
                accessModeProvider);
        this.kernelRead = new KernelRead(
                storageReader,
                kernelToken,
                cursors,
                transactionalCursors,
                entityLocks,
                queryContext,
                this,
                schemaRead,
                indexingService,
                memoryTracker,
                multiVersioned,
                this::assertOpenWithParallelAccessCheck,
                accessModeProvider,
                false);
        this.executionContextFactory = createExecutionContextFactory(
                contextFactory,
                storageEngine,
                config,
                lockManager,
                tokenHolders,
                schemaState,
                indexingService,
                indexStatisticsStore,
                tracers,
                leaseService,
                dependencies,
                securityAuthorizationHandler,
                elementIdMapper,
                multiVersioned);
        this.operations = new Operations(
                kernelRead,
                storageReader,
                new IndexTxStateUpdater(storageReader, indexingService, this),
                commandCreationContext,
                dbmsRuntimeVersionProvider,
                kernelVersionProvider,
                storageLocks,
                this,
                schemaRead,
                kernelToken,
                cursors,
                constraintIndexCreator,
                constraintSemantics,
                indexingService,
                config,
                memoryTracker,
                accessModeProvider);
        this.traceProvider = getTraceProvider(config);
        this.initializationTrace = NONE;
        this.transactionHeapBytesLimit = config.get(memory_transaction_max_size);
        this.collectionsFactory = collectionsFactorySupplier.create();
        this.kernelTransactions = kernelTransactions;
        this.databaseSerialGuard = databaseSerialGuard;
        this.transactionValidator =
                transactionValidatorFactory.createTransactionValidator(memoryTracker, transactionMonitor);
        this.validationLockDumper = transactionValidatorFactory.createValidationLockDumper();
        this.serialExecutionGuard = createSerialGuard(multiVersioned);
        this.committer = createCommitter(commitmentFactory, multiVersioned);
        this.transactionEventListeners = new TransactionEventListeners(transactionEventListeners, this, storageReader);
        this.txStateWriter = createChunkWriter(multiVersioned);
        registerConfigChangeListeners(config);
    }

    private void assertOpenWithParallelAccessCheck() {
        if (ParallelAccessCheck.shouldPerformCheck()) {
            ParallelAccessCheck.checkNotCypherWorkerThread();
        }
        assertOpen();
    }

    /**
     * Reset this transaction to a vanilla state, turning it into a logically new transaction.
     */
    public KernelTransactionImplementation initialize(
            long lastCommittedTx,
            Type type,
            SecurityContext frozenSecurityContext,
            TransactionTimeout transactionTimeout,
            long transactionSequenceNumber,
            ClientConnectionInfo clientInfo,
            ProcedureView procedureView) {
        assert transactionMemoryPool.usedHeap() == 0;
        assert transactionMemoryPool.usedNative() == 0;
        assert !failedCleanup : "This transaction should not be reused since it did not close properly";
        CURSOR_CONTEXT_HANDLE.setRelease(this, contextFactory.create(TRANSACTION_TAG));
        this.transactionalCursors.reset(cursorContext);
        this.accessCapability = accessCapabilityFactory.newAccessCapability(readOnlyDatabaseChecker);
        this.kernelTransactionMonitor = KernelTransaction.NO_MONITOR;
        this.type = type;
        this.leaseClient = leaseService.newClient();
        this.lockClient.initialize(leaseClient, transactionSequenceNumber, memoryTracker, config);
        this.terminationMark = null;
        this.commit = false;
        this.writeState = TransactionWriteState.NONE;
        this.startTimeMillis = clocks.systemClock().millis();
        this.startTimeNanos = clocks.systemClock().nanos();
        this.timeout = transactionTimeout;
        this.lastTransactionIdWhenStarted = lastCommittedTx;
        this.transactionEvent = transactionTracer.beginTransaction(cursorContext);
        this.overridableSecurityContext = new OverridableSecurityContext(frozenSecurityContext);
        this.transactionId = NOT_COMMITTED_TRANSACTION_ID;
        this.commitTime = NOT_COMMITTED_TRANSACTION_COMMIT_TIME;
        this.clientInfo = clientInfo;
        this.statistics.init(currentThread().getId());
        this.commandCreationContext.initialize(
                kernelVersionProvider,
                cursorContext,
                transactionalCursors,
                kernelTransactions::startTimeOfOldestActiveTransaction,
                lockClient,
                currentStatement::lockTracer);
        this.currentStatement.initialize(lockClient, cursorContext, startTimeMillis);
        this.operations.initialize(cursorContext);
        this.initializationTrace = traceProvider.getTraceInfo();
        this.transactionMemoryPool.setLimit(transactionHeapBytesLimit);
        this.innerTransactionHandler = new InnerTransactionHandlerImpl(kernelTransactions);
        this.procedureView = procedureView;
        this.procedures.initialize(procedureView);
        this.needsHighIdTracking = false;
        // keep these three at the bottom to have happens-before relationship with above volatile writes
        this.transactionSequenceNumber = transactionSequenceNumber;
        this.closing = false;
        this.closed = false;
        return this;
    }

    private static ExecutionContextFactory createExecutionContextFactory(
            CursorContextFactory contextFactory,
            StorageEngine storageEngine,
            Config config,
            LockManager lockManager,
            TokenHolders tokenHolders,
            SchemaState schemaState,
            IndexingService indexingService,
            IndexStatisticsStore indexStatisticsStore,
            DatabaseTracers tracers,
            LeaseService leaseService,
            Dependencies dependencies,
            SecurityAuthorizationHandler securityAuthorizationHandler,
            ElementIdMapper elementIdMapper,
            boolean multiVersioned) {
        return (securityContext,
                transactionId,
                transactionCursorContext,
                clockContextSupplier,
                kernelTransaction,
                procedureView) -> {
            var executionContextCursorTracer = new ExecutionContextCursorTracer(
                    PageCacheTracer.NULL, ExecutionContextCursorTracer.TRANSACTION_EXECUTION_TAG);
            var executionContextCursorContext = contextFactory.create(executionContextCursorTracer);
            StorageReader executionContextStorageReader = storageEngine.newReader();
            MemoryTracker executionContextMemoryTracker = kernelTransaction.createExecutionContextMemoryTracker();
            StoreCursors executionContextStoreCursors =
                    storageEngine.createStorageCursors(executionContextCursorContext);
            DefaultPooledCursors executionContextPooledCursors = new DefaultPooledCursors(
                    executionContextStorageReader,
                    executionContextStoreCursors,
                    config,
                    storageEngine.indexingBehaviour(),
                    multiVersioned);
            LockManager.Client executionContextLockClient = lockManager.newClient();
            executionContextLockClient.initialize(
                    leaseService.newClient(), transactionId, executionContextMemoryTracker, config);
            var overridableSecurityContext = new OverridableSecurityContext(securityContext);
            var executionContextTokenRead = new KernelTokenRead.ForThreadExecutionContextScope(
                    executionContextStorageReader, tokenHolders, overridableSecurityContext, kernelTransaction);

            return new ThreadExecutionContext(
                    executionContextPooledCursors,
                    executionContextCursorContext,
                    overridableSecurityContext,
                    executionContextCursorTracer,
                    transactionCursorContext,
                    executionContextTokenRead,
                    executionContextStoreCursors,
                    indexingService.getMonitor(),
                    executionContextMemoryTracker,
                    securityAuthorizationHandler,
                    executionContextStorageReader,
                    schemaState,
                    indexingService,
                    indexStatisticsStore,
                    dependencies,
                    storageEngine.createStorageLocks(executionContextLockClient),
                    executionContextLockClient,
                    tracers.getLockTracer(),
                    elementIdMapper,
                    kernelTransaction,
                    clockContextSupplier,
                    List.of(executionContextStorageReader, executionContextLockClient),
                    procedureView,
                    multiVersioned);
        };
    }

    @Override
    public void bindToUserTransaction(InternalTransaction internalTransaction) {
        this.internalTransaction = internalTransaction;
    }

    @Override
    public InternalTransaction internalTransaction() {
        return internalTransaction;
    }

    @Override
    public long startTime() {
        return startTimeMillis;
    }

    @Override
    public long startTimeNanos() {
        return startTimeNanos;
    }

    @Override
    public TransactionTimeout timeout() {
        return timeout;
    }

    @Override
    public Optional<TerminationMark> getTerminationMark() {
        return Optional.ofNullable(terminationMark);
    }

    private boolean canCommit() {
        return commit && terminationMark == null;
    }

    boolean markForTermination(long expectedTransactionSequenceNumber, Status reason) {
        terminationReleaseLock.lock();
        try {
            return expectedTransactionSequenceNumber == transactionSequenceNumber
                    && markForTerminationIfPossible(reason);
        } finally {
            terminationReleaseLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is guarded by {@link #terminationReleaseLock} to coordinate concurrent
     * {@link #close()} and {@link #reset()} calls.
     */
    @Override
    public void markForTermination(Status reason) {
        terminationReleaseLock.lock();
        try {
            markForTerminationIfPossible(reason);
        } finally {
            terminationReleaseLock.unlock();
        }
    }

    @Override
    public boolean isSchemaTransaction() {
        return TransactionWriteState.SCHEMA == writeState;
    }

    public boolean isDataTransaction() {
        return TransactionWriteState.DATA == writeState;
    }

    /**
     * Access method that should be used when cursor context is accessed from the <b>SAME</b> thread that is executing transaction or transaction itself
     */
    @Override
    public CursorContext cursorContext() {
        return cursorContext;
    }

    /**
     * Access method that should be used when cursor context is accessed from the thread that is not executing transaction (for example, monitoring)
     * For most of the cases this is not the proper method to use, see {@link #cursorContext()}
     */
    public CursorContext concurrentCursorContextLookup() {
        return (CursorContext) CURSOR_CONTEXT_HANDLE.getAcquire(this);
    }

    @Override
    public ExecutionContext createExecutionContext() {
        if (hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Execution context cannot be used for transactions with non-empty transaction state");
        }

        // Currently, the execution context is statement scoped and we rely on that by simply obtaining
        // the statement clock when it is created and the statement clock is immutable for the entire life of the
        // execution context.
        // For the same reason, execution context can be created only when there is an active statement.
        if (clocks.statementClock() == null) {
            throw new IllegalStateException("Execution context must be created when there is an active statement");
        }
        var statementClock =
                new ExecutionContextClock(clocks.systemClock(), clocks.transactionClock(), clocks.statementClock());

        return executionContextFactory.createNew(
                overridableSecurityContext.originalSecurityContext(),
                transactionSequenceNumber,
                cursorContext,
                () -> statementClock,
                this,
                this.procedureView);
    }

    @Override
    public MemoryTracker createExecutionContextMemoryTracker() {
        var grabSize = config.get(GraphDatabaseInternalSettings.initial_transaction_heap_grab_size_per_worker);
        var maxGrabSize = config.get(GraphDatabaseInternalSettings.max_transaction_heap_grab_size_per_worker);
        return transactionMemoryPool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
    }

    @Override
    public QueryContext queryContext() {
        return queryContext;
    }

    @Override
    public StoreCursors storeCursors() {
        return transactionalCursors;
    }

    @Override
    public MemoryTracker memoryTracker() {
        return memoryTracker;
    }

    private boolean markForTerminationIfPossible(Status reason) {
        if (canBeTerminated()) {
            var innerTransactionHandler = this.innerTransactionHandler;
            if (innerTransactionHandler != null) {
                innerTransactionHandler.terminateInnerTransactions(reason);
            }
            terminationMark = new TerminationMark(reason, clocks.systemClock().nanos());
            if (lockClient != null) {
                lockClient.stop();
            }
            transactionMonitor.transactionTerminated(hasTxState());

            var internalTransaction = this.internalTransaction;

            if (internalTransaction != null) {
                internalTransaction.terminate(reason);
            }

            return true;
        }
        return false;
    }

    @Override
    public boolean isOpen() {
        return !closed && !closing;
    }

    @Override
    public boolean isCommitting() {
        return closing && commit;
    }

    @Override
    public boolean isRollingback() {
        return closing && !commit;
    }

    @Override
    public SecurityAuthorizationHandler securityAuthorizationHandler() {
        return securityAuthorizationHandler;
    }

    @Override
    public SecurityContext securityContext() {
        // During the closing phase of the transaction, we have a brief window where the transaction
        // has already been marked as closed, but the security context has not been reset yet and might
        // be needed by the transactionExecutionMonitor (see afterCommit/afterRollback). So we only assert
        // if the transaction is fully closed when we're outside the closing phase.
        // If we're in the closing phase (independently if it has been marked as closed or not), we know
        // the security context object is still available to be used.
        if (!closing) {
            assertTransactionOpen();
        }
        return overridableSecurityContext.currentSecurityContext();
    }

    @Override
    public AuthSubject subjectOrAnonymous() {
        var ctx = overridableSecurityContext;
        if (ctx == null) {
            return AuthSubject.ANONYMOUS;
        }
        return ctx.currentSecurityContext().subject();
    }

    @Override
    public void setMetaData(Map<String, Object> data) {
        assertOpen();
        this.userMetaData = data;
    }

    @Override
    public Map<String, Object> getMetaData() {
        return userMetaData;
    }

    @Override
    public void setStatusDetails(String statusDetails) {
        assertOpen();
        this.statusDetails = statusDetails;
    }

    @Override
    public String statusDetails() {
        var details = statusDetails;
        return Objects.toString(details, EMPTY);
    }

    @Override
    public KernelStatement acquireStatement() {
        assertOpen();
        currentStatement.acquire();
        return currentStatement;
    }

    @Override
    public int aquireStatementCounter() {
        return currentStatement.aquireCounter();
    }

    @Override
    public ResourceMonitor resourceMonitor() {
        assert currentStatement.isAcquired();
        return currentStatement;
    }

    @Override
    public IndexDescriptor indexUniqueCreate(IndexPrototype prototype) {
        return operations.indexUniqueCreate(prototype);
    }

    @Override
    public long pageHits() {
        return cursorContext.getCursorTracer().hits();
    }

    @Override
    public long pageFaults() {
        return cursorContext.getCursorTracer().faults();
    }

    Optional<ExecutingQuery> executingQuery() {
        return currentStatement.executingQuery();
    }

    private void upgradeToDataWrites() throws InvalidTransactionTypeKernelException {
        writeState = writeState.upgradeToDataWrites();
    }

    private void upgradeToSchemaWrites() throws InvalidTransactionTypeKernelException {
        writeState = writeState.upgradeToSchemaWrites();
    }

    private void dropCreatedConstraintIndexes() throws TransactionFailureException {
        Iterator<IndexDescriptor> createdIndexIds = txState().constraintIndexesCreatedInTx();
        while (createdIndexIds.hasNext()) {
            IndexDescriptor createdIndex = createdIndexIds.next();
            constraintIndexCreator.dropUniquenessConstraintIndex(createdIndex);
        }
    }

    @Override
    public TransactionState txState() {
        if (txState == null) {
            leaseClient.ensureValid();
            readOnlyDatabaseChecker.check();
            transactionMonitor.upgradeToWriteTransaction();
            txStateWriter.initialize(
                    leaseClient,
                    cursorContext,
                    currentStatement::lockTracer,
                    startTimeMillis,
                    lastTransactionIdWhenStarted,
                    this::transactionApplicationMode);
            txState = new TxState(
                    collectionsFactory,
                    memoryTracker,
                    () -> enrichmentStrategy.check() != EnrichmentMode.OFF
                            || storageEngine.transactionStateBehaviour().keepMetaDataForDeletedRelationship(),
                    enrichmentStrategy,
                    txStateWriter,
                    transactionEvent);
        }
        return txState;
    }

    private boolean hasTxState() {
        return txState != null;
    }

    @Override
    public boolean hasTxStateWithChanges() {
        return hasTxState() && txState.hasChanges();
    }

    private boolean hasChanges() {
        return hasTxStateWithChanges();
    }

    private void markAsClosed() {
        assertTransactionOpen();
        closed = true;
        closeCurrentStatementIfAny();
    }

    private void closeCurrentStatementIfAny() {
        currentStatement.forceClose();
    }

    private void assertTransactionNotClosing() {
        if (closing) {
            throw new IllegalStateException("This transaction is already being closed.");
        }
    }

    private void assertTransactionOpen() {
        if (closed) {
            throw new NotInTransactionException("This transaction has already been closed.");
        }
    }

    /**
     * Used to make a note that this transaction, when it commits, will need to be applied with
     * highId tracking for ID generators.
     */
    public void needsHighIdTracking() {
        this.needsHighIdTracking = true;
    }

    @Override
    public void assertOpen() {
        var terminationMark = this.terminationMark;
        if (terminationMark != null) {
            throw new TransactionTerminatedException(terminationMark.getReason());
        }
        assertTransactionOpen();
    }

    @Override
    public long commit(KernelTransactionMonitor kernelTransactionMonitor) throws TransactionFailureException {
        commit = true;
        this.kernelTransactionMonitor = kernelTransactionMonitor;
        return closeTransaction();
    }

    @Override
    public void rollback() throws TransactionFailureException {
        // we need to allow multiple rollback calls since its possible that as result of query execution engine will
        // rollback the transaction
        // and will throw exception. For cases when users will do rollback as result of that as well we need to support
        // chain of rollback calls but
        // still fail on rollback, commit
        if (!isOpen()) {
            return;
        }
        closeTransaction();
    }

    @Override
    public long closeTransaction() throws TransactionFailureException {
        assertTransactionOpen();
        assertTransactionNotClosing();
        // we assume that inner transaction have been closed before closing the outer transaction
        assertNoInnerTransactions();
        closing = true;
        Throwable exception = null;
        long txId = ROLLBACK_ID;
        try {
            if (canCommit()) {
                txId = commitTransaction();
            } else {
                rollbackTransaction();
                failOnNonExplicitRollbackIfNeeded();
            }
        } catch (TransactionFailureException | RuntimeException | Error e) {
            exception = e;
        } catch (KernelException e) {
            exception = new TransactionFailureException(e.status(), e, "Unexpected kernel exception");
        } finally {
            try {
                closed();
            } catch (RuntimeException | Error e) {
                exception = Exceptions.chain(exception, e);
                failedCleanup = true;
            } finally {
                try {
                    reset();
                } catch (RuntimeException | Error e) {
                    exception = Exceptions.chain(exception, e);
                    failedCleanup = true;
                }
            }
        }
        if (exception == null) {
            return txId;
        }

        if (leaseClient.leaseId() != NO_LEASE) {
            try {
                leaseClient.ensureValid();
            } catch (RuntimeException | Error e) {
                exception = Exceptions.chain(exception, e);
            }
        }

        Exceptions.throwIfInstanceOf(exception, TransactionFailureException.class);
        Exceptions.throwIfUnchecked(exception);
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
                .withClassification(ErrorClassification.DATABASE_ERROR)
                .build();
        throw new TransactionFailureException(gql, Status.General.UnknownError, exception);
    }

    private void closed() {
        closed = true;
        closing = false;
        transactionEvent.setCommit(commit);
        transactionEvent.setRollback(!commit);
        transactionEvent.setTransactionWriteState(writeState.name());
        transactionEvent.setReadOnly(txState == null || !txState.hasChanges());
        transactionEvent.close();
    }

    @Override
    public void close() throws TransactionFailureException {
        try {
            if (isOpen()) {
                closeTransaction();
            }
        } finally {
            if (failedCleanup) {
                pool.dispose(this);
            } else {
                pool.release(this);
            }
        }
    }

    @Override
    public boolean isClosing() {
        return closing;
    }

    /**
     * Throws exception if this transaction was attempted to be committed but failed or was terminated.
     * <p>
     * This could happen when:
     * <ul>
     * <li>caller explicitly calls {@link #commit()} but transaction execution fails</li>
     * <li>caller explicitly calls {@link #commit()} but transaction is terminated</li>
     * </ul>
     * <p>
     *
     * @throws TransactionFailureException when execution failed
     * @throws TransactionTerminatedException when transaction was terminated
     */
    private void failOnNonExplicitRollbackIfNeeded() throws TransactionFailureException {
        if (commit) {
            if (isTerminated()) {
                throw new TransactionTerminatedException(terminationMark.getReason());
            }
            // Commit was called, but also failed which means that the client code using this
            // transaction passed through a happy path, but the transaction was rolled back
            // for one or more reasons. Tell the user that although it looked happy it
            // wasn't committed, but was instead rolled back.
            throw new TransactionFailureException(
                    Status.Transaction.TransactionMarkedAsFailed,
                    "Transaction rolled back even if marked as successful");
        }
    }

    private long commitTransaction() throws KernelException {
        Throwable exception = null;
        boolean success = false;
        long txId = READ_ONLY_ID;
        try (TransactionWriteEvent transactionWriteEvent = transactionEvent.beginCommitEvent()) {
            transactionEventListeners.beforeCommit(txState, true);

            // Convert changes into commands and commit
            if (hasChanges()) {
                schemaTransactionVersionReset();
                lockClient.prepareForCommit();

                long timeCommitted = clocks.systemClock().millis();
                txId = committer.commit(
                        transactionWriteEvent,
                        leaseClient,
                        cursorContext,
                        memoryTracker,
                        kernelTransactionMonitor,
                        lockTracer(),
                        timeCommitted,
                        startTimeMillis,
                        lastTransactionIdWhenStarted,
                        true,
                        transactionApplicationMode());
                commitTime = timeCommitted;
            }
            success = true;
        } catch (ConstraintValidationException | CreateConstraintFailureException e) {
            exception = new ConstraintViolationTransactionFailureException(e.getUserMessage(tokenRead()), e);
        } catch (Throwable e) {
            exception = e;
        } finally {
            try {
                if (!success) {
                    commit = false;
                    rollbackTransaction();
                } else {
                    transactionId = txId;
                    afterCommit();
                }
                transactionMonitor.addHeapTransactionSize(transactionMemoryPool.usedHeap());
                transactionMonitor.addNativeTransactionSize(transactionMemoryPool.usedNative());
            } catch (RuntimeException | Error e) {
                exception = Exceptions.chain(exception, e);
            }
        }
        if (exception == null) {
            return txId;
        }
        Exceptions.throwIfInstanceOf(exception, TransactionFailureException.class);
        Exceptions.throwIfUnchecked(exception);
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
                .withClassification(ErrorClassification.DATABASE_ERROR)
                .build();
        throw new TransactionFailureException(gql, Status.General.UnknownError, exception);
    }

    private TransactionApplicationMode transactionApplicationMode() {
        return needsHighIdTracking ? TransactionApplicationMode.EXTERNAL : TransactionApplicationMode.INTERNAL;
    }

    public List<StorageCommand> extractCommands(MemoryTracker commandsTracker) throws KernelException {
        final var commandDecorator = commandDecorator(commandsTracker);
        final var commands = storageEngine.createCommands(
                txState,
                storageReader,
                commandCreationContext,
                lockTracer(),
                commandDecorator,
                cursorContext,
                transactionalCursors,
                commandsTracker);
        return commandDecorator.transform(commands);
    }

    private CommandDecorator commandDecorator(MemoryTracker commandsTracker) {
        final var mode = txState.enrichmentMode();
        if (namedDatabaseId.isSystemDatabase() || mode == EnrichmentMode.OFF || !txState.hasDataChanges()) {
            return tx -> enforceConstraints(tx, commandsTracker);
        }

        return new CommandDecorator() {
            private TxEnrichmentVisitor enrichmentVisitor;

            @Override
            public TxStateVisitor apply(TxStateVisitor tx) {
                enrichmentVisitor = new TxEnrichmentVisitor(
                        enforceConstraints(tx, commandsTracker),
                        mode == EnrichmentMode.DIFF ? CaptureMode.DIFF : CaptureMode.FULL,
                        serverIdentity.serverId().shortName(),
                        kernelVersionProvider,
                        storageEngine::createEnrichmentCommand,
                        txState,
                        userMetaData,
                        lastTransactionIdWhenStarted,
                        storageReader,
                        cursorContext,
                        transactionalCursors,
                        commandsTracker);
                return enrichmentVisitor;
            }

            @Override
            public List<StorageCommand> transform(List<StorageCommand> storageCommands) {
                return (enrichmentVisitor == null) ? storageCommands : enrich(storageCommands);
            }

            private List<StorageCommand> enrich(List<StorageCommand> commands) {
                final var enrichment = enrichmentVisitor.command(overridableSecurityContext.currentSecurityContext());
                if (enrichment != null) {
                    commands.add(enrichment);
                }
                return commands;
            }
        };
    }

    // Because of current constraint creation dance we need to refresh context version to be able
    // to read schema records that were created in inner transactions
    private void schemaTransactionVersionReset() {
        if (isSchemaTransaction()) {
            cursorContext.getVersionContext().initRead();
        }
    }

    private void rollbackTransaction() throws KernelException {
        try {
            if (hasTxStateWithChanges()) {
                try (var rollbackEvent = transactionEvent.beginRollback()) {
                    committer.rollback(rollbackEvent);
                    if (!txState().hasConstraintIndexesCreatedInTx()) {
                        return;
                    }

                    try {
                        dropCreatedConstraintIndexes();
                    } catch (IllegalStateException | SecurityException e) {
                        throw new TransactionFailureException(
                                Status.Transaction.TransactionRollbackFailed,
                                e,
                                "Could not drop created constraint indexes");
                    }
                }
            }
        } catch (KernelException | RuntimeException | Error e) {
            throw e;
        } catch (Throwable throwable) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_40N01)
                    .withClassification(ErrorClassification.DATABASE_ERROR)
                    .build();
            throw new UnspecifiedKernelException(gql, Status.Transaction.TransactionRollbackFailed, throwable);
        } finally {
            afterRollback();
        }
    }

    @Override
    public Read dataRead() {
        return kernelRead;
    }

    @Override
    public Write dataWrite() throws InvalidTransactionTypeKernelException {
        accessCapability.assertCanWrite();
        upgradeToDataWrites();
        return operations;
    }

    @Override
    public TokenWrite tokenWrite() {
        accessCapability.assertCanWrite();
        return operations.token();
    }

    @Override
    public Token token() {
        accessCapability.assertCanWrite();
        return operations.token();
    }

    @Override
    public TokenRead tokenRead() {
        return operations.token();
    }

    @Override
    public SchemaRead schemaRead() {
        return schemaRead;
    }

    @Override
    public SchemaWrite schemaWrite() throws InvalidTransactionTypeKernelException {
        accessCapability.assertCanWrite();
        upgradeToSchemaWrites();
        return new RestrictedSchemaWrite(operations, securityContext(), securityAuthorizationHandler);
    }

    @Override
    public Upgrade upgrade() {
        accessCapability.assertCanWrite();
        return operations;
    }

    @Override
    public Locks locks() {
        return entityLocks;
    }

    public LockManager.Client lockClient() {
        assertOpen();
        return lockClient;
    }

    @Override
    public CursorFactory cursors() {
        return operations.cursors();
    }

    @Override
    public Procedures procedures() {
        return procedures;
    }

    @Override
    public ExecutionStatistics executionStatistics() {
        return this;
    }

    public LockTracer lockTracer() {
        return currentStatement.lockTracer();
    }

    private void afterCommit() {
        try {
            markAsClosed();
            transactionEventListeners.afterCommit();
            kernelTransactionMonitor.afterCommit(this);
        } finally {
            transactionMonitor.transactionFinished(true, hasTxState());
            transactionExecutionMonitor.commit(this);
        }
    }

    private void afterRollback() {
        try {
            markAsClosed();
            transactionEventListeners.afterRollback();
        } finally {
            transactionMonitor.transactionFinished(false, hasTxState());
            transactionExecutionMonitor.rollback(this, transactionEventListeners.failure());
        }
    }

    /**
     * Release resources for the current statement because it's being closed.
     */
    void releaseStatementResources() {
        kernelRead.release();
    }

    /**
     * Resets all internal states of the transaction so that it's ready to be reused.
     * This method is guarded by {@link #terminationReleaseLock} to coordinate concurrent
     * {@link #markForTermination(Status)} calls.
     */
    private void reset() {
        terminationReleaseLock.lock();
        Throwable error = null;
        try {
            try {
                lockClient.close();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                releaseStorageEngineResources();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            timeout = TransactionTimeout.NO_TIMEOUT;
            startTimeMillis = Long.MAX_VALUE;
            startTimeNanos = Long.MAX_VALUE;
            serialExecutionGuard.release();
            transactionEventListeners.reset();
            terminationMark = null;
            type = null;
            overridableSecurityContext = null;
            transactionEvent = null;
            txState = null;
            try {
                collectionsFactory.release();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            userMetaData = emptyMap();
            statusDetails = EMPTY;
            clientInfo = null;
            internalTransaction = null;
            transactionSequenceNumber = 0;
            try {
                statistics.reset();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                releaseStatementResources();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                procedures.reset();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            procedureView = null;
            try {
                operations.release();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                commandCreationContext.close();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                transactionalCursors.close();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                cursorContext.close();
                CURSOR_CONTEXT_HANDLE.setRelease(this, CursorContext.NULL_CONTEXT);
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            initializationTrace = NONE;
            try {
                committer.reset();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                transactionMemoryPool.reset();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            try {
                innerTransactionHandler.close();
            } catch (RuntimeException | Error e) {
                error = Exceptions.chain(error, e);
            }
            innerTransactionHandler = null;
        } finally {
            terminationReleaseLock.unlock();
        }
        if (error != null) {
            Exceptions.throwIfUnchecked(error);
            throw new ResourceCloseFailureException("Failed to close resources", error);
        }
    }

    @Override
    public void releaseStorageEngineResources() {
        if (txState != null) {
            storageEngine.release(txState, cursorContext, commandCreationContext, !commit);
        }
    }

    /**
     * Transaction can be terminated only when it is not closed and not already terminated.
     * Otherwise termination does not make sense.
     */
    private boolean canBeTerminated() {
        return !closed && !isTerminated();
    }

    @Override
    public boolean isTerminated() {
        return terminationMark != null;
    }

    @Override
    public Type transactionType() {
        return type;
    }

    @Override
    public long getTransactionId() {
        long txId = transactionId;
        if (txId == NOT_COMMITTED_TRANSACTION_ID) {
            throw new IllegalStateException(
                    "Transaction id is not assigned yet. " + "It will be assigned during transaction commit.");
        }
        return txId;
    }

    @Override
    public long getCommitTime() {
        long time = commitTime;
        if (time == NOT_COMMITTED_TRANSACTION_COMMIT_TIME) {
            throw new IllegalStateException(
                    "Transaction commit time is not assigned yet. " + "It will be assigned during transaction commit.");
        }
        return time;
    }

    @Override
    public Revertable overrideWith(SecurityContext context) {
        // During the closing phase of the transaction, we have a brief window where the transaction
        // has already been marked as closed, but the security context has not been reset yet and might
        // be needed by the transactionExecutionMonitor (see afterCommit/afterRollback). So we only assert
        // if the transaction is fully closed when we're outside the closing phase.
        // If we're in the closing phase (independently if it has been marked as closed or not), we know
        // the security context object is still available to be used.
        if (!closing) {
            assertTransactionOpen();
        }
        return overridableSecurityContext.overrideWith(context)::close;
    }

    @Override
    public String toString() {
        return format("KernelTransaction[lease:%d]", leaseClient.leaseId());
    }

    public void dispose() {
        storageReader.close();
        transactionMemoryPool.close();
        removeConfigChangeListeners(config);
    }

    /**
     * This method will be invoked by concurrent threads for inspecting the locks held by this transaction.
     * <p>
     * The fact that {@link #lockClient} is a volatile fields, grants us enough of a read barrier to get a good
     * enough snapshot of the lock state (as long as the underlying methods give us such guarantees).
     *
     * @return the locks held by this transaction.
     */
    public Collection<ActiveLock> activeLocks() {
        LockManager.Client locks = this.lockClient;
        return locks == null ? Collections.emptyList() : locks.activeLocks();
    }

    @Override
    public long getTransactionSequenceNumber() {
        return transactionSequenceNumber;
    }

    TransactionInitializationTrace getInitializationTrace() {
        return initializationTrace;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    private TxStateVisitor enforceConstraints(TxStateVisitor txStateVisitor, MemoryTracker memoryTracker) {
        return constraintSemantics.decorateTxStateVisitor(
                storageReader,
                kernelRead,
                operations.cursors(),
                txState,
                txStateVisitor,
                ((CursorContext) CURSOR_CONTEXT_HANDLE.get(this)),
                memoryTracker);
    }

    /**
     * @return transaction originator information.
     */
    @Override
    public ClientConnectionInfo clientInfo() {
        return clientInfo;
    }

    public StorageReader newStorageReader() {
        return storageEngine.newReader();
    }

    public void addIndexDoDropToTxState(IndexDescriptor index) {
        txState().indexDoDrop(index);
    }

    @Override
    public String getDatabaseName() {
        return namedDatabaseId.name();
    }

    @Override
    public UUID getDatabaseId() {
        return namedDatabaseId.databaseId().uuid();
    }

    @Override
    public InnerTransactionHandlerImpl getInnerTransactionHandler() {
        var handle = innerTransactionHandler;
        if (handle != null) {
            return this.innerTransactionHandler;
        }
        throw new IllegalStateException("Called getInnerTransactionHandler on inactive transaction");
    }

    private void assertNoInnerTransactions() throws TransactionFailureException {
        if (getInnerTransactionHandler().hasInnerTransaction()) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN01)
                    .withClassification(ErrorClassification.CLIENT_ERROR)
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN07)
                            .withClassification(ErrorClassification.CLIENT_ERROR)
                            .build())
                    .build();
            throw new TransactionFailureException(
                    gql,
                    TransactionCommitFailed,
                    "The transaction cannot be committed when it has open inner transactions.");
        }
    }

    private SerialExecutionGuard createSerialGuard(boolean multiVersioned) {
        return multiVersioned
                ? new TransactionSerialExecutionGuard(databaseSerialGuard, this)
                : SerialExecutionGuard.EMPTY_GUARD;
    }

    private ChunkedTransactionSink createChunkWriter(boolean multiVersioned) {
        return multiVersioned
                ? new ChunkSink(committer, transactionEventListeners, clocks, config)
                : ChunkedTransactionSink.EMPTY;
    }

    private TransactionCommitter createCommitter(
            TransactionCommitmentFactory commitmentFactory, boolean multiVersioned) {
        return multiVersioned
                ? new ChunkCommitter(
                        this,
                        commitmentFactory,
                        kernelVersionProvider,
                        transactionalCursors,
                        transactionIdGenerator,
                        commitProcess,
                        databaseHealth,
                        clocks,
                        rollbackProcess,
                        transactionValidator,
                        validationLockDumper,
                        serialExecutionGuard,
                        logProvider)
                : new DefaultCommitter(
                        this,
                        commitmentFactory,
                        kernelVersionProvider,
                        transactionalCursors,
                        transactionIdGenerator,
                        commitProcess);
    }

    public static class Statistics {
        private volatile long cpuTimeNanosWhenQueryStarted;
        private volatile long heapAllocatedBytesWhenQueryStarted;
        private volatile long waitingTimeNanos;
        private volatile long transactionThreadId;
        private final KernelTransactionImplementation transaction;
        private final AtomicReference<CpuClock> cpuClockRef;
        private CpuClock cpuClock;
        private final HeapAllocation heapAllocation;

        public Statistics(
                KernelTransactionImplementation transaction,
                AtomicReference<CpuClock> cpuClockRef,
                boolean heapAllocationTracking) {
            this.transaction = transaction;
            this.cpuClockRef = cpuClockRef;
            this.heapAllocation =
                    heapAllocationTracking ? HeapAllocation.HEAP_ALLOCATION : HeapAllocation.NOT_AVAILABLE;
        }

        protected void init(long threadId) {
            this.cpuClock = cpuClockRef.get();
            this.transactionThreadId = threadId;
            this.cpuTimeNanosWhenQueryStarted = cpuClock.cpuTimeNanos(transactionThreadId);
            this.heapAllocatedBytesWhenQueryStarted = heapAllocation.allocatedBytes(transactionThreadId);
        }

        /**
         * Returns number of allocated bytes by current transaction.
         * @return number of allocated bytes by the thread.
         */
        long heapAllocatedBytes() {
            long allocatedBytes = heapAllocation.allocatedBytes(transactionThreadId);
            return (allocatedBytes < 0) ? -1 : allocatedBytes - heapAllocatedBytesWhenQueryStarted;
        }

        /**
         * @return estimated amount of used heap memory
         */
        long estimatedHeapMemory() {
            return transaction.transactionMemoryPool.usedHeap();
        }

        /**
         * @return amount of native memory
         */
        long usedNativeMemory() {
            return transaction.transactionMemoryPool.usedNative();
        }

        /**
         * Return CPU time used by current transaction in milliseconds
         * @return the current CPU time used by the transaction, in milliseconds.
         */
        public long cpuTimeMillis() {
            long cpuTimeNanos = cpuClock.cpuTimeNanos(transactionThreadId);
            return (cpuTimeNanos < 0) ? -1 : NANOSECONDS.toMillis(cpuTimeNanos - cpuTimeNanosWhenQueryStarted);
        }

        /**
         * Report how long any particular query was waiting during it's execution
         * @param waitTimeNanos query waiting time in nanoseconds
         */
        @SuppressWarnings("NonAtomicOperationOnVolatileField")
        void addWaitingTime(long waitTimeNanos) {
            waitingTimeNanos += waitTimeNanos;
        }

        /**
         * Accumulated transaction waiting time that includes waiting time of all already executed queries
         * plus waiting time of currently executed query.
         * @return accumulated transaction waiting time
         * @param nowNanos current moment in nanoseconds
         */
        long getWaitingTimeNanos(long nowNanos) {
            Optional<ExecutingQuery> query = transaction.executingQuery();
            long waitingTime = waitingTimeNanos;
            if (query.isPresent()) {
                long latestQueryWaitingNanos = query.get().totalWaitingTimeNanos(nowNanos);
                waitingTime = waitingTime + latestQueryWaitingNanos;
            }
            return waitingTime;
        }

        void reset() {
            cpuTimeNanosWhenQueryStarted = 0;
            heapAllocatedBytesWhenQueryStarted = 0;
            waitingTimeNanos = 0;
            transactionThreadId = -1;
        }
    }

    public boolean isCommitted() {
        return commit;
    }

    @Override
    public TransactionClockContext clocks() {
        return clocks;
    }

    @Override
    public NodeCursor ambientNodeCursor() {
        return operations.nodeCursor();
    }

    @Override
    public RelationshipScanCursor ambientRelationshipCursor() {
        return operations.relationshipCursor();
    }

    @Override
    public PropertyCursor ambientPropertyCursor() {
        return operations.propertyCursor();
    }

    public void ensureValid() throws LeaseException {
        leaseClient.ensureValid();
    }

    private void registerConfigChangeListeners(LocalConfig config) {
        config.addListener(transaction_tracing_level, (before, after) -> traceProvider = getTraceProvider(config));
        config.addListener(
                transaction_sampling_percentage, (before, after) -> traceProvider = getTraceProvider(config));
        config.addListener(memory_transaction_max_size, (before, after) -> transactionHeapBytesLimit = after);
    }

    private void removeConfigChangeListeners(LocalConfig config) {
        config.removeAllLocalListeners();
    }

    /**
     * It is not allowed for the same transaction to perform database writes as well as schema writes.
     * This enum tracks the current write transactionStatus of the transaction, allowing it to transition from
     * no writes (NONE) to data writes (DATA) or schema writes (SCHEMA), but it cannot transition between
     * DATA and SCHEMA without throwing an InvalidTransactionTypeKernelException. Note that this behavior
     * is orthogonal to the SecurityContext which manages what the transaction or statement is allowed to do
     * based on authorization.
     */
    private enum TransactionWriteState {
        NONE,
        DATA {
            @Override
            TransactionWriteState upgradeToSchemaWrites() throws InvalidTransactionTypeKernelException {
                throw new InvalidTransactionTypeKernelException(
                        "Cannot perform schema updates in a transaction that has performed data updates.");
            }
        },
        SCHEMA {
            @Override
            TransactionWriteState upgradeToDataWrites() throws InvalidTransactionTypeKernelException {
                throw new InvalidTransactionTypeKernelException(
                        "Cannot perform data updates in a transaction that has performed schema updates.");
            }
        };

        TransactionWriteState upgradeToDataWrites() throws InvalidTransactionTypeKernelException {
            return DATA;
        }

        TransactionWriteState upgradeToSchemaWrites() throws InvalidTransactionTypeKernelException {
            return SCHEMA;
        }
    }

    @FunctionalInterface
    private interface ExecutionContextFactory {

        ExecutionContext createNew(
                SecurityContext securityContext,
                long transactionId,
                CursorContext transactionCursorContext,
                Supplier<ClockContext> clockContextSupplier,
                KernelTransaction ktx,
                ProcedureView procedureView);
    }

    private interface CommandDecorator extends Decorator {
        default List<StorageCommand> transform(List<StorageCommand> storageCommands) {
            // no enrichment is occurring
            return storageCommands;
        }
    }

    private record ExecutionContextClock(Clock systemClock, Clock transactionClock, Clock statementClock)
            implements ClockContext {

        @Override
        public Clock systemClock() {
            return systemClock;
        }

        @Override
        public Clock transactionClock() {
            return transactionClock;
        }

        @Override
        public Clock statementClock() {
            return statementClock;
        }
    }
}
