/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.pool.Pool;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.LocksNotFrozenException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.FrozenLockClient;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors;
import org.neo4j.kernel.impl.newapi.IndexTxStateUpdater;
import org.neo4j.kernel.impl.newapi.KernelToken;
import org.neo4j.kernel.impl.newapi.Operations;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.TransactionListenersState;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.LimitedMemoryTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;

import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_sampling_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.impl.api.transaction.trace.TraceProviderFactory.getTraceProvider;
import static org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace.NONE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

public class KernelTransactionImplementation implements KernelTransaction, TxStateHolder, ExecutionStatistics
{
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

    private final CollectionsFactory collectionsFactory;

    // Logic
    private final DatabaseTransactionEventListeners eventListeners;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final StorageEngine storageEngine;
    private final TransactionTracer transactionTracer;
    private final PageCacheTracer pageCacheTracer;
    private final Pool<KernelTransactionImplementation> pool;

    // For committing
    private final TransactionCommitProcess commitProcess;
    private final TransactionMonitor transactionMonitor;
    private final TransactionExecutionMonitor transactionExecutionMonitor;
    private final VersionContextSupplier versionContextSupplier;
    private final LeaseService leaseService;
    private final StorageReader storageReader;
    private final CommandCreationContext commandCreationContext;
    private final NamedDatabaseId namedDatabaseId;
    private final ClockContext clocks;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final ConstraintSemantics constraintSemantics;
    private final StorageLocks storageLocks;
    private CursorContext cursorContext;
    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;
    private final SecurityAuthorizationHandler securityAuthorizationHandler;

    // State that needs to be reset between uses. Most of these should be cleared or released in #release(),
    // whereas others, such as timestamp or txId when transaction starts, even locks, needs to be set in #initialize().
    private TxState txState;
    private volatile TransactionWriteState writeState;
    private AccessCapability accessCapability;
    private final KernelStatement currentStatement;
    private SecurityContext securityContext;
    private volatile Locks.Client lockClient;
    private volatile long userTransactionId;
    private LeaseClient leaseClient;
    private volatile boolean closing;
    private volatile boolean closed;
    private boolean failure;
    private boolean success;
    private volatile Status terminationReason;
    private long startTimeMillis;
    private volatile long startTimeNanos;
    private volatile long timeoutMillis;
    private long lastTransactionIdWhenStarted;
    private volatile long lastTransactionTimestampWhenStarted;
    private final Statistics statistics;
    private TransactionEvent transactionEvent;
    private Type type;
    private long transactionId;
    private long commitTime;
    private volatile ClientConnectionInfo clientInfo;
    private volatile int reuseCount;
    private volatile Map<String,Object> userMetaData;
    private volatile String statusDetails;
    private final AllStoreHolder allStoreHolder;
    private final Operations operations;
    private InternalTransaction internalTransaction;
    private volatile TraceProvider traceProvider;
    private volatile TransactionInitializationTrace initializationTrace;
    private final LimitedMemoryTracker memoryTracker;
    private final Config config;
    private volatile long transactionHeapBytesLimit;

    /**
     * Lock prevents transaction {@link #markForTermination(Status)}  transaction termination} from interfering with
     * {@link #close() transaction commit} and specifically with {@link #reset()}.
     * Termination can run concurrently with commit and we need to make sure that it terminates the right lock client
     * and the right transaction (with the right {@link #reuseCount}) because {@link KernelTransactionImplementation}
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

    public KernelTransactionImplementation( Config config,
            DatabaseTransactionEventListeners eventListeners, ConstraintIndexCreator constraintIndexCreator, GlobalProcedures globalProcedures,
            TransactionCommitProcess commitProcess, TransactionMonitor transactionMonitor,
            Pool<KernelTransactionImplementation> pool, SystemNanoClock clock,
            AtomicReference<CpuClock> cpuClockRef, DatabaseTracers tracers,
            StorageEngine storageEngine, AccessCapabilityFactory accessCapabilityFactory,
            VersionContextSupplier versionContextSupplier, CollectionsFactorySupplier collectionsFactorySupplier,
            ConstraintSemantics constraintSemantics, SchemaState schemaState, TokenHolders tokenHolders, IndexingService indexingService,
            IndexStatisticsStore indexStatisticsStore, Dependencies dependencies,
            NamedDatabaseId namedDatabaseId, LeaseService leaseService, ScopedMemoryPool transactionMemoryPool,
            DatabaseReadOnlyChecker readOnlyDatabaseChecker, TransactionExecutionMonitor transactionExecutionMonitor,
            AbstractSecurityLog securityLog, KernelVersionRepository kernelVersionRepository, DbmsRuntimeRepository dbmsRuntimeRepository,
            Locks.Client lockClient, KernelTransactions kernelTransactions )
    {
        this.accessCapabilityFactory = accessCapabilityFactory;
        this.readOnlyDatabaseChecker = readOnlyDatabaseChecker;
        long heapGrabSize = config.get( GraphDatabaseInternalSettings.initial_transaction_heap_grab_size );
        this.memoryTracker = config.get( memory_tracking ) ? new LocalMemoryTracker( transactionMemoryPool, 0, heapGrabSize,
                                     memory_transaction_max_size.name(), () -> !closed ) : EmptyMemoryTracker.INSTANCE;
        this.eventListeners = eventListeners;
        this.constraintIndexCreator = constraintIndexCreator;
        this.commitProcess = commitProcess;
        this.transactionMonitor = transactionMonitor;
        this.transactionExecutionMonitor = transactionExecutionMonitor;
        this.storageReader = storageEngine.newReader();
        this.commandCreationContext = storageEngine.newCommandCreationContext( memoryTracker );
        this.namedDatabaseId = namedDatabaseId;
        this.storageEngine = storageEngine;
        this.pool = pool;
        this.clocks = new ClockContext( clock );
        this.transactionTracer = tracers.getDatabaseTracer();
        this.pageCacheTracer = tracers.getPageCacheTracer();
        this.versionContextSupplier = versionContextSupplier;
        this.leaseService = leaseService;
        this.currentStatement = new KernelStatement( this, tracers.getLockTracer(), this.clocks, cpuClockRef, namedDatabaseId, config );
        this.statistics = new Statistics( this, cpuClockRef, config.get( GraphDatabaseInternalSettings.enable_transaction_heap_allocation_tracking ) );
        this.userMetaData = emptyMap();
        this.statusDetails = EMPTY;
        this.constraintSemantics = constraintSemantics;
        this.transactionalCursors = storageEngine.createStorageCursors( CursorContext.NULL );
        this.storageLocks = storageEngine.createStorageLocks( lockClient );
        DefaultPooledCursors cursors = new DefaultPooledCursors( storageReader, transactionalCursors, config );
        this.securityAuthorizationHandler = new SecurityAuthorizationHandler( securityLog );
        this.allStoreHolder = new AllStoreHolder( storageReader, this, storageLocks, cursors, globalProcedures, schemaState,
                indexingService, indexStatisticsStore, dependencies, memoryTracker );
        this.operations =
                new Operations(
                        allStoreHolder,
                        storageReader,
                        new IndexTxStateUpdater( storageReader, allStoreHolder, indexingService ),
                        commandCreationContext,
                        storageLocks,
                        this,
                        new KernelToken( storageReader, commandCreationContext, this, tokenHolders ),
                        cursors,
                        constraintIndexCreator,
                        constraintSemantics,
                        indexingService,
                        config, memoryTracker,
                        kernelVersionRepository,
                        dbmsRuntimeRepository
                );
        traceProvider = getTraceProvider( config );
        transactionHeapBytesLimit = config.get( memory_transaction_max_size );
        registerConfigChangeListeners( config );
        this.config = config;
        this.collectionsFactory = collectionsFactorySupplier.create();
        this.lockClient = lockClient;
        this.kernelTransactions = kernelTransactions;
    }

    /**
     * Reset this transaction to a vanilla state, turning it into a logically new transaction.
     */
    public KernelTransactionImplementation initialize( long lastCommittedTx, long lastTimeStamp, Type type,
            SecurityContext frozenSecurityContext, long transactionTimeout, long userTransactionId, ClientConnectionInfo clientInfo )
    {
        assert memoryTracker.estimatedHeapMemory() == 0;
        assert memoryTracker.usedNativeMemory() == 0;
        this.cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( TRANSACTION_TAG ), versionContextSupplier.createVersionContext() );
        this.transactionalCursors.reset( cursorContext );
        this.accessCapability = accessCapabilityFactory.newAccessCapability( readOnlyDatabaseChecker );
        this.kernelTransactionMonitor = KernelTransaction.NO_MONITOR;
        this.type = type;
        this.userTransactionId = userTransactionId;
        this.leaseClient = leaseService.newClient();
        this.lockClient.initialize( leaseClient, userTransactionId, memoryTracker, config );
        this.terminationReason = null;
        this.closing = false;
        this.closed = false;
        this.failure = false;
        this.success = false;
        this.writeState = TransactionWriteState.NONE;
        this.startTimeMillis = clocks.systemClock().millis();
        this.startTimeNanos = clocks.systemClock().nanos();
        this.timeoutMillis = transactionTimeout;
        this.lastTransactionIdWhenStarted = lastCommittedTx;
        this.lastTransactionTimestampWhenStarted = lastTimeStamp;
        this.transactionEvent = transactionTracer.beginTransaction( cursorContext );
        this.securityContext = frozenSecurityContext;
        this.transactionId = NOT_COMMITTED_TRANSACTION_ID;
        this.commitTime = NOT_COMMITTED_TRANSACTION_COMMIT_TIME;
        this.clientInfo = clientInfo;
        this.statistics.init( currentThread().getId(), cursorContext );
        this.commandCreationContext.initialize( cursorContext, transactionalCursors );
        this.currentStatement.initialize( lockClient, cursorContext, startTimeMillis );
        this.operations.initialize( cursorContext );
        this.initializationTrace = traceProvider.getTraceInfo();
        this.memoryTracker.setLimit( transactionHeapBytesLimit );
        this.innerTransactionHandler = new InnerTransactionHandlerImpl( kernelTransactions );
        return this;
    }

    @Override
    public void bindToUserTransaction( InternalTransaction internalTransaction )
    {
        this.internalTransaction = internalTransaction;
    }

    @Override
    public InternalTransaction internalTransaction()
    {
        return internalTransaction;
    }

    int getReuseCount()
    {
        return reuseCount;
    }

    @Override
    public long startTime()
    {
        return startTimeMillis;
    }

    @Override
    public long startTimeNanos()
    {
        return startTimeNanos;
    }

    @Override
    public long timeout()
    {
        return timeoutMillis;
    }

    @Override
    public long lastTransactionIdWhenStarted()
    {
        return lastTransactionIdWhenStarted;
    }

    public void success()
    {
        this.success = true;
    }

    boolean isSuccess()
    {
        return success;
    }

    @Override
    public boolean canCommit()
    {
        return success && !failure && terminationReason == null;
    }

    public void failure()
    {
        failure = true;
    }

    @Override
    public Optional<Status> getReasonIfTerminated()
    {
        return Optional.ofNullable( terminationReason );
    }

    boolean markForTermination( long expectedReuseCount, Status reason )
    {
        terminationReleaseLock.lock();
        try
        {
            return expectedReuseCount == reuseCount && markForTerminationIfPossible( reason );
        }
        finally
        {
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
    public void markForTermination( Status reason )
    {
        terminationReleaseLock.lock();
        try
        {
            markForTerminationIfPossible( reason );
        }
        finally
        {
            terminationReleaseLock.unlock();
        }
    }

    @Override
    public boolean isSchemaTransaction()
    {
        return writeState == TransactionWriteState.SCHEMA;
    }

    @Override
    public CursorContext cursorContext()
    {
        return cursorContext;
    }

    @Override
    public ExecutionContext createExecutionContext()
    {
        return new ThreadExecutionContext();
    }

    @Override
    public QueryContext queryContext()
    {
        return operations.queryContext();
    }

    @Override
    public StoreCursors storeCursors()
    {
        return transactionalCursors;
    }

    @Override
    public MemoryTracker memoryTracker()
    {
        return memoryTracker;
    }

    private void mergeExecutionContext( ExecutionContext executionContext )
    {
        cursorContext.merge( executionContext.cursorContext() );
    }

    private boolean markForTerminationIfPossible( Status reason )
    {
        if ( canBeTerminated() )
        {
            var innerTransactionHandler = this.innerTransactionHandler;
            if ( innerTransactionHandler != null )
            {
                innerTransactionHandler.terminateInnerTransactions( reason );
            }
            failure = true;
            terminationReason = reason;
            if ( lockClient != null )
            {
                lockClient.stop();
            }
            transactionMonitor.transactionTerminated( hasTxStateWithChanges() );

            var internalTransaction = this.internalTransaction;

            if ( internalTransaction != null )
            {
                internalTransaction.terminate( reason );
            }

            return true;
        }
        return false;
    }

    @Override
    public boolean isOpen()
    {
        return !closed && !closing;
    }

    @Override
    public SecurityAuthorizationHandler securityAuthorizationHandler()
    {
        return securityAuthorizationHandler;
    }

    @Override
    public SecurityContext securityContext()
    {
        if ( securityContext == null )
        {
            throw new NotInTransactionException();
        }
        return securityContext;
    }

    @Override
    public AuthSubject subjectOrAnonymous()
    {
        SecurityContext context = this.securityContext;
        return context == null ? AuthSubject.ANONYMOUS : context.subject();
    }

    @Override
    public void setMetaData( Map<String, Object> data )
    {
        assertOpen();
        this.userMetaData = data;
    }

    @Override
    public Map<String, Object> getMetaData()
    {
        return userMetaData;
    }

    @Override
    public void setStatusDetails( String statusDetails )
    {
        assertOpen();
        this.statusDetails = statusDetails;
    }

    @Override
    public String statusDetails()
    {
        var details = statusDetails;
        return defaultString( details, EMPTY );
    }

    @Override
    public KernelStatement acquireStatement()
    {
        assertOpen();
        currentStatement.acquire();
        return currentStatement;
    }

    @Override
    public IndexDescriptor indexUniqueCreate( IndexPrototype prototype )
    {
        return operations.indexUniqueCreate( prototype );
    }

    @Override
    public long pageHits()
    {
        return cursorContext.getCursorTracer().hits();
    }

    @Override
    public long pageFaults()
    {
        return cursorContext.getCursorTracer().faults();
    }

    Optional<ExecutingQuery> executingQuery()
    {
        return currentStatement.executingQuery();
    }

    private void upgradeToDataWrites() throws InvalidTransactionTypeKernelException
    {
        writeState = writeState.upgradeToDataWrites();
    }

    private void upgradeToSchemaWrites() throws InvalidTransactionTypeKernelException
    {
        writeState = writeState.upgradeToSchemaWrites();
    }

    private void dropCreatedConstraintIndexes() throws TransactionFailureException
    {
        if ( hasTxStateWithChanges() )
        {
            Iterator<IndexDescriptor> createdIndexIds = txState().constraintIndexesCreatedInTx();
            while ( createdIndexIds.hasNext() )
            {
                IndexDescriptor createdIndex = createdIndexIds.next();
                constraintIndexCreator.dropUniquenessConstraintIndex( createdIndex );
            }
        }
    }

    @Override
    public TransactionState txState()
    {
        if ( txState == null )
        {
            leaseClient.ensureValid();
            readOnlyDatabaseChecker.check();
            transactionMonitor.upgradeToWriteTransaction();
            txState = new TxState( collectionsFactory, memoryTracker, storageEngine.transactionStateBehaviour() );
        }
        return txState;
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return txState != null && txState.hasChanges();
    }

    private void markAsClosed()
    {
        assertTransactionOpen();
        closed = true;
        closeCurrentStatementIfAny();
    }

    private void closeCurrentStatementIfAny()
    {
        currentStatement.forceClose();
    }

    private void assertTransactionNotClosing()
    {
        if ( closing )
        {
            throw new IllegalStateException( "This transaction is already being closed." );
        }
    }

    private void assertTransactionOpen()
    {
        if ( closed )
        {
            throw new NotInTransactionException( "This transaction has already been closed." );
        }
    }

    @Override
    public void assertOpen()
    {
        Status reason = this.terminationReason;
        if ( reason != null )
        {
            throw new TransactionTerminatedException( reason );
        }
        assertTransactionOpen();
    }

    private boolean hasChanges()
    {
        return hasTxStateWithChanges();
    }

    @Override
    public long commit( KernelTransactionMonitor kernelTransactionMonitor ) throws TransactionFailureException
    {
        success();
        this.kernelTransactionMonitor = kernelTransactionMonitor;
        return closeTransaction();
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        // we need to allow multiple rollback calls since its possible that as result of query execution engine will rollback the transaction
        // and will throw exception. For cases when users will do rollback as result of that as well we need to support chain of rollback calls but
        // still fail on rollback, commit
        if ( !isOpen() && failure )
        {
            return;
        }
        failure();
        closeTransaction();
    }

    @Override
    public long closeTransaction() throws TransactionFailureException
    {
        assertTransactionOpen();
        assertTransactionNotClosing();
        // we assume that inner transaction have been closed before closing the outer transaction
        assertNoInnerTransactions();
        closing = true;
        try
        {
            if ( canCommit() )
            {
                return commitTransaction();
            }
            else
            {
                rollback( null );
                failOnNonExplicitRollbackIfNeeded();
                return ROLLBACK_ID;
            }
        }
        catch ( TransactionFailureException e )
        {
            throw e;
        }
        catch ( KernelException e )
        {
            throw new TransactionFailureException( e.status(), e, "Unexpected kernel exception" );
        }
        finally
        {
            try
            {
                closed = true;
                closing = false;
                transactionEvent.setSuccess( success );
                transactionEvent.setFailure( failure );
                transactionEvent.setTransactionWriteState( writeState.name() );
                transactionEvent.setReadOnly( txState == null || !txState.hasChanges() );
                transactionEvent.close();
            }
            finally
            {
                reset();
            }
        }
    }

    @Override
    public void close() throws TransactionFailureException
    {
        if ( isOpen() )
        {
            closeTransaction();
        }
        pool.release( this );
    }

    @Override
    public boolean isClosing()
    {
        return closing;
    }

    /**
     * Throws exception if this transaction was marked as successful but failure flag has also been set to true.
     * <p>
     * This could happen when:
     * <ul>
     * <li>caller explicitly calls both {@link #success()} and {@link #failure()}</li>
     * <li>caller explicitly calls {@link #success()} but transaction execution fails</li>
     * <li>caller explicitly calls {@link #success()} but transaction is terminated</li>
     * </ul>
     * <p>
     *
     * @throws TransactionFailureException when execution failed
     * @throws TransactionTerminatedException when transaction was terminated
     */
    private void failOnNonExplicitRollbackIfNeeded() throws TransactionFailureException
    {
        if ( success && isTerminated() )
        {
            throw new TransactionTerminatedException( terminationReason );
        }
        if ( success )
        {
            // Success was called, but also failure which means that the client code using this
            // transaction passed through a happy path, but the transaction was still marked as
            // failed for one or more reasons. Tell the user that although it looked happy it
            // wasn't committed, but was instead rolled back.
            throw new TransactionFailureException( Status.Transaction.TransactionMarkedAsFailed,
                    "Transaction rolled back even if marked as successful" );
        }
    }

    private long commitTransaction() throws KernelException
    {
        boolean success = false;
        long txId = READ_ONLY_ID;
        TransactionListenersState listenersState = null;
        try ( CommitEvent commitEvent = transactionEvent.beginCommitEvent() )
        {
            listenersState = eventListeners.beforeCommit( txState, this, storageReader );
            if ( listenersState != null && listenersState.isFailed() )
            {
                Throwable cause = listenersState.failure();
                if ( cause instanceof TransientFailureException )
                {
                    throw (TransientFailureException) cause;
                }
                if ( cause instanceof Status.HasStatus )
                {
                    throw new TransactionFailureException( ((Status.HasStatus) cause).status(), cause, cause.getMessage() );
                }
                throw new TransactionFailureException( Status.Transaction.TransactionHookFailed, cause, cause.getMessage() );
            }

            // Convert changes into commands and commit
            if ( hasChanges() )
            {
                forceThawLocks();
                lockClient.prepareForCommit();

                // Gather up commands from the various sources
                HeapTrackingArrayList<StorageCommand> extractedCommands = HeapTrackingCollections.newArrayList( memoryTracker );
                storageEngine.createCommands(
                        extractedCommands,
                        txState,
                        storageReader,
                        commandCreationContext,
                        lockClient,
                        lockTracer(),
                        lastTransactionIdWhenStarted,
                        this::enforceConstraints,
                        cursorContext,
                        transactionalCursors,
                        memoryTracker );

                /* Here's the deal: we track a quick-to-access hasChanges in transaction state which is true
                 * if there are any changes imposed by this transaction. Some changes made inside a transaction undo
                 * previously made changes in that same transaction, and so at some point a transaction may have
                 * changes and at another point, after more changes seemingly,
                 * the transaction may not have any changes.
                 * However, to track that "undoing" of the changes is a bit tedious, intrusive and hard to maintain
                 * and get right.... So to really make sure the transaction has changes we re-check by looking if we
                 * have produced any commands to add to the logical log.
                 */
                if ( !extractedCommands.isEmpty() )
                {
                    // Finish up the whole transaction representation
                    PhysicalTransactionRepresentation transactionRepresentation =
                            new PhysicalTransactionRepresentation( extractedCommands );
                    long timeCommitted = clocks.systemClock().millis();
                    transactionRepresentation.setHeader(
                            EMPTY_BYTE_ARRAY, startTimeMillis, lastTransactionIdWhenStarted, timeCommitted, leaseClient.leaseId(), securityContext.subject() );

                    // Commit the transaction
                    success = true;
                    TransactionToApply batch = new TransactionToApply( transactionRepresentation, cursorContext, transactionalCursors );
                    kernelTransactionMonitor.beforeApply();
                    txId = commitProcess.commit( batch, commitEvent, INTERNAL );
                    commitTime = timeCommitted;
                }
            }
            success = true;
            return txId;
        }
        catch ( ConstraintValidationException | CreateConstraintFailureException e )
        {
            throw new ConstraintViolationTransactionFailureException( e.getUserMessage( tokenRead() ), e );
        }
        finally
        {
            if ( !success )
            {
                rollback( listenersState );
            }
            else
            {
                transactionId = txId;
                afterCommit( listenersState );
            }
            transactionMonitor.addHeapTransactionSize( memoryTracker.heapHighWaterMark() );
            transactionMonitor.addNativeTransactionSize( memoryTracker.usedNativeMemory() );
        }
    }

    private void rollback( TransactionListenersState listenersState ) throws KernelException
    {
        try
        {
            try
            {
                dropCreatedConstraintIndexes();
            }
            catch ( IllegalStateException | SecurityException e )
            {
                throw new TransactionFailureException( Status.Transaction.TransactionRollbackFailed, e,
                        "Could not drop created constraint indexes" );
            }
        }
        finally
        {
            afterRollback( listenersState );
        }
    }

    @Override
    public Read dataRead()
    {
        return operations.dataRead();
    }

    @Override
    public Write dataWrite() throws InvalidTransactionTypeKernelException
    {
        accessCapability.assertCanWrite();
        upgradeToDataWrites();
        return operations;
    }

    @Override
    public TokenWrite tokenWrite()
    {
        accessCapability.assertCanWrite();
        return operations.token();
    }

    @Override
    public Token token()
    {
        accessCapability.assertCanWrite();
        return operations.token();
    }

    @Override
    public TokenRead tokenRead()
    {
        return operations.token();
    }

    @Override
    public SchemaRead schemaRead()
    {
        return operations.schemaRead();
    }

    @Override
    public SchemaWrite schemaWrite() throws InvalidTransactionTypeKernelException
    {
        accessCapability.assertCanWrite();
        //TODO: Consider removing this since we re-check with fine graned a few lines below
        securityAuthorizationHandler.assertAllowsSchemaWrites( securityContext() );

        upgradeToSchemaWrites();
        return new RestrictedSchemaWrite( operations, securityContext(), securityAuthorizationHandler );
    }

    @Override
    public org.neo4j.internal.kernel.api.Locks locks()
    {
       return operations.locks();
    }

    @Override
    public void freezeLocks()
    {
        Locks.Client locks = lockClient;
        if ( !(locks instanceof FrozenLockClient) )
        {
            this.lockClient = new FrozenLockClient( locks );
        }
        else
        {
            ((FrozenLockClient)locks).freeze();
        }
    }

    @Override
    public void thawLocks() throws LocksNotFrozenException
    {
        Locks.Client locks = lockClient;
        if ( locks instanceof FrozenLockClient )
        {
            FrozenLockClient frozenLocks = (FrozenLockClient) locks;
            if ( frozenLocks.thaw() )
            {
                lockClient = frozenLocks.getRealLockClient();
            }
        }
        else
        {
            throw new LocksNotFrozenException();
        }
    }

    private void forceThawLocks()
    {
        Locks.Client locks = lockClient;
        if ( locks instanceof FrozenLockClient )
        {
            lockClient = ((FrozenLockClient) locks).getRealLockClient();
        }
    }

    public Locks.Client lockClient()
    {
        assertOpen();
        return lockClient;
    }

    @Override
    public CursorFactory cursors()
    {
        return operations.cursors();
    }

    @Override
    public org.neo4j.internal.kernel.api.Procedures procedures()
    {
        return operations.procedures();
    }

    @Override
    public ExecutionStatistics executionStatistics()
    {
        return this;
    }

    public LockTracer lockTracer()
    {
        return currentStatement.lockTracer();
    }

    private void afterCommit( TransactionListenersState listenersState )
    {
        try
        {
            markAsClosed();
            eventListeners.afterCommit( listenersState );
        }
        finally
        {
            transactionMonitor.transactionFinished( true, hasTxStateWithChanges() );
            transactionExecutionMonitor.commit( this );
        }
    }

    private void afterRollback( TransactionListenersState listenersState )
    {
        try
        {
            markAsClosed();
            eventListeners.afterRollback( listenersState );
        }
        finally
        {
            transactionMonitor.transactionFinished( false, hasTxStateWithChanges() );
            if ( listenersState == null || listenersState.failure() == null )
            {
                transactionExecutionMonitor.rollback( this );
            }
            else
            {
                transactionExecutionMonitor.rollback( this, listenersState.failure() );
            }
        }
    }

    /**
     * Release resources for the current statement because it's being closed.
     */
    void releaseStatementResources()
    {
        allStoreHolder.release();
    }

    /**
     * Resets all internal states of the transaction so that it's ready to be reused.
     * This method is guarded by {@link #terminationReleaseLock} to coordinate concurrent
     * {@link #markForTermination(Status)} calls.
     */
    private void reset()
    {
        terminationReleaseLock.lock();
        try
        {
            forceThawLocks();
            lockClient.close();
            terminationReason = null;
            type = null;
            securityContext = null;
            transactionEvent = null;
            txState = null;
            collectionsFactory.release();
            reuseCount++;
            userMetaData = emptyMap();
            statusDetails = EMPTY;
            clientInfo = null;
            internalTransaction = null;
            userTransactionId = 0;
            statistics.reset();
            releaseStatementResources();
            operations.release();
            commandCreationContext.close();
            transactionalCursors.close();
            cursorContext.close();
            initializationTrace = NONE;
            memoryTracker.reset();
            innerTransactionHandler.close();
            innerTransactionHandler = null;
        }
        finally
        {
            terminationReleaseLock.unlock();
        }
    }

    /**
     * Transaction can be terminated only when it is not closed and not already terminated.
     * Otherwise termination does not make sense.
     */
    private boolean canBeTerminated()
    {
        return !closed && !isTerminated();
    }

    @Override
    public boolean isTerminated()
    {
        return terminationReason != null;
    }

    @Override
    public long lastTransactionTimestampWhenStarted()
    {
        return lastTransactionTimestampWhenStarted;
    }

    @Override
    public Type transactionType()
    {
        return type;
    }

    @Override
    public long getTransactionId()
    {
        if ( transactionId == NOT_COMMITTED_TRANSACTION_ID )
        {
            throw new IllegalStateException( "Transaction id is not assigned yet. " +
                                             "It will be assigned during transaction commit." );
        }
        return transactionId;
    }

    @Override
    public long getCommitTime()
    {
        if ( commitTime == NOT_COMMITTED_TRANSACTION_COMMIT_TIME )
        {
            throw new IllegalStateException( "Transaction commit time is not assigned yet. " +
                                             "It will be assigned during transaction commit." );
        }
        return commitTime;
    }

    @Override
    public Revertable overrideWith( SecurityContext context )
    {
        SecurityContext oldContext = this.securityContext;
        this.securityContext = context;
        return () -> this.securityContext = oldContext;
    }

    @Override
    public String toString()
    {
        return String.format( "KernelTransaction[lease:%d]", leaseClient.leaseId() );
    }

    public void dispose()
    {
        storageReader.close();
    }

    /**
     * This method will be invoked by concurrent threads for inspecting the locks held by this transaction.
     * <p>
     * The fact that {@link #lockClient} is a volatile fields, grants us enough of a read barrier to get a good
     * enough snapshot of the lock state (as long as the underlying methods give us such guarantees).
     *
     * @return the locks held by this transaction.
     */
    public Stream<ActiveLock> activeLocks()
    {
        Locks.Client locks = this.lockClient;
        return locks == null ? Stream.empty() : locks.activeLocks();
    }

    @Override
    public long getUserTransactionId()
    {
        return userTransactionId;
    }

    TransactionInitializationTrace getInitializationTrace()
    {
        return initializationTrace;
    }

    public Statistics getStatistics()
    {
        return statistics;
    }

    private TxStateVisitor enforceConstraints( TxStateVisitor txStateVisitor )
    {
        return constraintSemantics.decorateTxStateVisitor( storageReader, operations.dataRead(), operations.cursors(), txState, txStateVisitor,
                cursorContext, memoryTracker );
    }

    /**
     * @return transaction originator information.
     */
    @Override
    public ClientConnectionInfo clientInfo()
    {
        return clientInfo;
    }

    public StorageReader newStorageReader()
    {
        return storageEngine.newReader();
    }

    public void addIndexDoDropToTxState( IndexDescriptor index )
    {
        txState().indexDoDrop( index );
    }

    @Override
    public String getDatabaseName()
    {
        return namedDatabaseId.name();
    }

    public UUID getDatabaseId()
    {
        return namedDatabaseId.databaseId().uuid();
    }

    /**
     * WARNING! This method should only be called from DatabaseUpgradeTransactionHandler
     */
    @Override
    public InjectedNLIUpgradeCallback injectedNLIUpgradeCallback()
    {
        return id ->
        {
            if ( txState.indexChanges().getRemoved().contains( IndexDescriptor.INJECTED_NLI ) )
            {
                txState.indexDoUnRemove( IndexDescriptor.INJECTED_NLI );
                txState.indexDoDrop( IndexDescriptor.NLI_PROTOTYPE.materialise( id ) );
            }
        };
    }

    @Override
    public InnerTransactionHandlerImpl getInnerTransactionHandler()
    {
        if ( innerTransactionHandler != null )
        {
            return this.innerTransactionHandler;
        }
        throw new IllegalStateException( "Called getInnerTransactionHandler on inactive transaction" );
    }

    private void assertNoInnerTransactions() throws TransactionFailureException
    {
        if ( getInnerTransactionHandler().hasInnerTransaction() )
        {
            throw new TransactionFailureException( TransactionCommitFailed, "The transaction cannot be committed when it has open inner transactions." );
        }
    }

    public static class Statistics
    {
        private volatile long cpuTimeNanosWhenQueryStarted;
        private volatile long heapAllocatedBytesWhenQueryStarted;
        private volatile long waitingTimeNanos;
        private volatile long transactionThreadId;
        private volatile CursorContext cursorContext = CursorContext.NULL;
        private final KernelTransactionImplementation transaction;
        private final AtomicReference<CpuClock> cpuClockRef;
        private CpuClock cpuClock;
        private final HeapAllocation heapAllocation;

        public Statistics( KernelTransactionImplementation transaction, AtomicReference<CpuClock> cpuClockRef, boolean heapAllocationTracking )
        {
            this.transaction = transaction;
            this.cpuClockRef = cpuClockRef;
            this.heapAllocation = heapAllocationTracking ? HeapAllocation.HEAP_ALLOCATION : HeapAllocation.NOT_AVAILABLE;
        }

        protected void init( long threadId, CursorContext cursorContext )
        {
            this.cpuClock = cpuClockRef.get();
            this.transactionThreadId = threadId;
            this.cursorContext = cursorContext;
            this.cpuTimeNanosWhenQueryStarted = cpuClock.cpuTimeNanos( transactionThreadId );
            this.heapAllocatedBytesWhenQueryStarted = heapAllocation.allocatedBytes( transactionThreadId );
        }

        /**
         * Returns number of allocated bytes by current transaction.
         * @return number of allocated bytes by the thread.
         */
        long heapAllocatedBytes()
        {
            return heapAllocation.allocatedBytes( transactionThreadId ) - heapAllocatedBytesWhenQueryStarted;
        }

        /**
         * @return estimated amount of used heap memory
         */
        long estimatedHeapMemory()
        {
            return transaction.memoryTracker().estimatedHeapMemory();
        }

        /**
         * @return amount of native memory
         */
        long usedNativeMemory()
        {
            return transaction.memoryTracker().usedNativeMemory();
        }

        /**
         * Return CPU time used by current transaction in milliseconds
         * @return the current CPU time used by the transaction, in milliseconds.
         */
        public long cpuTimeMillis()
        {
            long cpuTimeNanos = cpuClock.cpuTimeNanos( transactionThreadId ) - cpuTimeNanosWhenQueryStarted;
            return NANOSECONDS.toMillis( cpuTimeNanos );
        }

        /**
         * Return total number of page cache hits that current transaction performed
         * @return total page cache hits
         */
        long totalTransactionPageCacheHits()
        {
            return cursorContext.getCursorTracer().hits();
        }

        /**
         * Return total number of page cache faults that current transaction performed
         * @return total page cache faults
         */
        long totalTransactionPageCacheFaults()
        {
            return cursorContext.getCursorTracer().faults();
        }

        /**
         * Report how long any particular query was waiting during it's execution
         * @param waitTimeNanos query waiting time in nanoseconds
         */
        @SuppressWarnings( "NonAtomicOperationOnVolatileField" )
        void addWaitingTime( long waitTimeNanos )
        {
            waitingTimeNanos += waitTimeNanos;
        }

        /**
         * Accumulated transaction waiting time that includes waiting time of all already executed queries
         * plus waiting time of currently executed query.
         * @return accumulated transaction waiting time
         * @param nowNanos current moment in nanoseconds
         */
        long getWaitingTimeNanos( long nowNanos )
        {
            Optional<ExecutingQuery> query = transaction.executingQuery();
            long waitingTime = waitingTimeNanos;
            if ( query.isPresent() )
            {
                long latestQueryWaitingNanos = query.get().totalWaitingTimeNanos( nowNanos );
                waitingTime = waitingTime + latestQueryWaitingNanos;
            }
            return waitingTime;
        }

        void reset()
        {
            cursorContext = CursorContext.NULL;
            cpuTimeNanosWhenQueryStarted = 0;
            heapAllocatedBytesWhenQueryStarted = 0;
            waitingTimeNanos = 0;
            transactionThreadId = -1;
        }
    }

    @Override
    public ClockContext clocks()
    {
        return clocks;
    }

    @Override
    public NodeCursor ambientNodeCursor()
    {
        return operations.nodeCursor();
    }

    @Override
    public RelationshipScanCursor ambientRelationshipCursor()
    {
        return operations.relationshipCursor();
    }

    @Override
    public PropertyCursor ambientPropertyCursor()
    {
        return operations.propertyCursor();
    }

    private void registerConfigChangeListeners( Config config )
    {
        config.addListener( transaction_tracing_level, ( before, after ) -> traceProvider = getTraceProvider( config ) );
        config.addListener( transaction_sampling_percentage, ( before, after ) -> traceProvider = getTraceProvider( config ) );
        config.addListener( memory_transaction_max_size, ( before, after ) -> transactionHeapBytesLimit = after );
    }

    /**
     * It is not allowed for the same transaction to perform database writes as well as schema writes.
     * This enum tracks the current write transactionStatus of the transaction, allowing it to transition from
     * no writes (NONE) to data writes (DATA) or schema writes (SCHEMA), but it cannot transition between
     * DATA and SCHEMA without throwing an InvalidTransactionTypeKernelException. Note that this behavior
     * is orthogonal to the SecurityContext which manages what the transaction or statement is allowed to do
     * based on authorization.
     */
    private enum TransactionWriteState
    {
        NONE,
        DATA
                {
                    @Override
                    TransactionWriteState upgradeToSchemaWrites() throws InvalidTransactionTypeKernelException
                    {
                        throw new InvalidTransactionTypeKernelException(
                                "Cannot perform schema updates in a transaction that has performed data updates." );
                    }
                },
        SCHEMA
                {
                    @Override
                    TransactionWriteState upgradeToDataWrites() throws InvalidTransactionTypeKernelException
                    {
                        throw new InvalidTransactionTypeKernelException(
                                "Cannot perform data updates in a transaction that has performed schema updates." );
                    }
                };

        TransactionWriteState upgradeToDataWrites() throws InvalidTransactionTypeKernelException
        {
            return DATA;
        }

        TransactionWriteState upgradeToSchemaWrites() throws InvalidTransactionTypeKernelException
        {
            return SCHEMA;
        }
    }

    private class ThreadExecutionContext implements ExecutionContext, AutoCloseable
    {
        private final CursorContext context;
        private final AccessMode accessMode;
        private final ExecutionContextCursorTracer cursorTracer;

        ThreadExecutionContext()
        {
            this.cursorTracer = new ExecutionContextCursorTracer( PageCacheTracer.NULL, TRANSACTION_TAG );
            this.context = new CursorContext( cursorTracer, cursorContext.getVersionContext() );
            this.accessMode = securityContext.mode();
        }

        @Override
        public CursorContext cursorContext()
        {
            return context;
        }

        @Override
        public AccessMode accessMode()
        {
            return accessMode;
        }

        @Override
        public void complete()
        {
            context.getCursorTracer().reportEvents();
        }

        @Override
        public void close()
        {
            while ( !cursorTracer.isCompleted() )
            {
                Thread.onSpinWait();
            }
            mergeExecutionContext( this );
        }

    }

    private static class ExecutionContextCursorTracer extends DefaultPageCursorTracer
    {
        private long pins;
        private long unpins;
        private long hits;
        private long faults;
        private long bytesRead;
        private long bytesWritten;
        private long evictions;
        private long evictionExceptions;
        private long flushes;
        private long merges;
        private volatile boolean completed;

        ExecutionContextCursorTracer( PageCacheTracer pageCacheTracer, String tag )
        {
            super( pageCacheTracer, tag );
        }

        // We override report events here since we want to capture all the events accumulated in the tracer and another thread and make
        // then available to consumer thread. That in ensued by waiting for completed flag by consumer thread.
        @Override
        public void reportEvents()
        {
            pins = super.pins();
            unpins = super.unpins();
            hits = super.hits();
            faults = super.faults();
            bytesRead = super.bytesRead();
            bytesWritten = super.bytesWritten();
            evictions = super.evictions();
            evictionExceptions = super.evictionExceptions();
            flushes = super.flushes();
            merges = super.merges();
            completed = true;
        }

        public boolean isCompleted()
        {
            return completed;
        }

        @Override
        public long faults()
        {
            return faults;
        }

        @Override
        public long pins()
        {
            return pins;
        }

        @Override
        public long unpins()
        {
            return unpins;
        }

        @Override
        public long hits()
        {
            return hits;
        }

        @Override
        public long bytesRead()
        {
            return bytesRead;
        }

        @Override
        public long evictions()
        {
            return evictions;
        }

        @Override
        public long evictionExceptions()
        {
            return evictionExceptions;
        }

        @Override
        public long bytesWritten()
        {
            return bytesWritten;
        }

        @Override
        public long flushes()
        {
            return flushes;
        }

        @Override
        public long merges()
        {
            return merges;
        }
    }
}
