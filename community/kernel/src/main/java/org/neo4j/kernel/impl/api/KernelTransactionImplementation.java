/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
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
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.FrozenStatementLocks;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors;
import org.neo4j.kernel.impl.newapi.IndexTxStateUpdater;
import org.neo4j.kernel.impl.newapi.KernelToken;
import org.neo4j.kernel.impl.newapi.Operations;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.TransactionListenersState;
import org.neo4j.lock.LockTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_sampling_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.kernel.impl.api.transaction.trace.TraceProviderFactory.getTraceProvider;
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

    private final CollectionsFactory collectionsFactory;

    // Logic
    private final DatabaseTransactionEventListeners eventListeners;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final StorageEngine storageEngine;
    private final TransactionTracer transactionTracer;
    private final Pool<KernelTransactionImplementation> pool;

    // For committing
    private final TransactionHeaderInformationFactory headerInformationFactory;
    private final TransactionCommitProcess commitProcess;
    private final TransactionMonitor transactionMonitor;
    private final PageCursorTracerSupplier cursorTracerSupplier;
    private final VersionContextSupplier versionContextSupplier;
    private final AvailabilityGuard databaseAvailabilityGuard;
    private final DatabaseId databaseId;
    private final StorageReader storageReader;
    private final CommandCreationContext commandCreationContext;
    private final ClockContext clocks;
    private final AccessCapability accessCapability;
    private final ConstraintSemantics constraintSemantics;

    // State that needs to be reset between uses. Most of these should be cleared or released in #release(),
    // whereas others, such as timestamp or txId when transaction starts, even locks, needs to be set in #initialize().
    private TxState txState;
    private volatile TransactionWriteState writeState;
    private final KernelStatement currentStatement;
    private final List<CloseListener> closeListeners = new ArrayList<>( 2 );
    private SecurityContext securityContext;
    private volatile StatementLocks statementLocks;
    private volatile long userTransactionId;
    private volatile boolean closing;
    private volatile boolean closed;
    private boolean failure;
    private boolean success;
    private volatile Status terminationReason;
    private long startTimeMillis;
    private long startTimeNanos;
    private long timeoutMillis;
    private long lastTransactionIdWhenStarted;
    private volatile long lastTransactionTimestampWhenStarted;
    private final Statistics statistics;
    private TransactionEvent transactionEvent;
    private Type type;
    private long transactionId;
    private long commitTime;
    private ClientConnectionInfo clientInfo;
    private volatile int reuseCount;
    private volatile Map<String,Object> userMetaData;
    private final AllStoreHolder allStoreHolder;
    private final Operations operations;
    private volatile TraceProvider traceProvider;
    private volatile TransactionInitializationTrace initializationTrace;

    /**
     * Lock prevents transaction {@link #markForTermination(Status)}  transaction termination} from interfering with
     * {@link #close() transaction commit} and specifically with {@link #release()}.
     * Termination can run concurrently with commit and we need to make sure that it terminates the right lock client
     * and the right transaction (with the right {@link #reuseCount}) because {@link KernelTransactionImplementation}
     * instances are pooled.
     */
    private final Lock terminationReleaseLock = new ReentrantLock();

    public KernelTransactionImplementation( Config config,
            DatabaseTransactionEventListeners eventListeners, ConstraintIndexCreator constraintIndexCreator, GlobalProcedures globalProcedures,
            TransactionHeaderInformationFactory headerInformationFactory, TransactionCommitProcess commitProcess, TransactionMonitor transactionMonitor,
            Pool<KernelTransactionImplementation> pool, SystemNanoClock clock,
            AtomicReference<CpuClock> cpuClockRef, AtomicReference<HeapAllocation> heapAllocationRef, TransactionTracer transactionTracer,
            LockTracer lockTracer, PageCursorTracerSupplier cursorTracerSupplier, StorageEngine storageEngine, AccessCapability accessCapability,
            VersionContextSupplier versionContextSupplier, CollectionsFactorySupplier collectionsFactorySupplier,
            ConstraintSemantics constraintSemantics, SchemaState schemaState, TokenHolders tokenHolders, IndexingService indexingService,
            LabelScanStore labelScanStore, IndexStatisticsStore indexStatisticsStore, Dependencies dependencies,
            AvailabilityGuard databaseAvailabilityGuard, DatabaseId databaseId )
    {
        this.eventListeners = eventListeners;
        this.constraintIndexCreator = constraintIndexCreator;
        this.headerInformationFactory = headerInformationFactory;
        this.commitProcess = commitProcess;
        this.transactionMonitor = transactionMonitor;
        this.storageReader = storageEngine.newReader();
        this.commandCreationContext = storageEngine.newCommandCreationContext();
        this.storageEngine = storageEngine;
        this.pool = pool;
        this.clocks = new ClockContext( clock );
        this.transactionTracer = transactionTracer;
        this.cursorTracerSupplier = cursorTracerSupplier;
        this.versionContextSupplier = versionContextSupplier;
        this.databaseAvailabilityGuard = databaseAvailabilityGuard;
        this.databaseId = databaseId;
        this.currentStatement = new KernelStatement( this, lockTracer, this.clocks, versionContextSupplier, cpuClockRef, databaseId );
        this.accessCapability = accessCapability;
        this.statistics = new Statistics( this, cpuClockRef, heapAllocationRef );
        this.userMetaData = emptyMap();
        this.constraintSemantics = constraintSemantics;
        DefaultPooledCursors cursors = new DefaultPooledCursors( storageReader );
        this.allStoreHolder =
                new AllStoreHolder( storageReader, this, cursors, globalProcedures, schemaState, indexingService, labelScanStore, indexStatisticsStore,
                        dependencies );
        this.operations =
                new Operations(
                        allStoreHolder,
                        storageReader,
                        new IndexTxStateUpdater( storageReader, allStoreHolder, indexingService ),
                        commandCreationContext,
                        this,
                        new KernelToken( storageReader, commandCreationContext, this, tokenHolders ),
                        cursors,
                        constraintIndexCreator,
                        constraintSemantics,
                        indexingService,
                        config );
        traceProvider = getTraceProvider( config );
        registerConfigChangeListeners( config );
        this.collectionsFactory = collectionsFactorySupplier.create();
    }

    /**
     * Reset this transaction to a vanilla state, turning it into a logically new transaction.
     */
    public KernelTransactionImplementation initialize( long lastCommittedTx, long lastTimeStamp, StatementLocks statementLocks, Type type,
            SecurityContext frozenSecurityContext, long transactionTimeout, long userTransactionId, ClientConnectionInfo clientInfo )
    {
        this.type = type;
        this.statementLocks = statementLocks;
        this.userTransactionId = userTransactionId;
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
        this.transactionEvent = transactionTracer.beginTransaction();
        assert transactionEvent != null : "transactionEvent was null!";
        this.securityContext = frozenSecurityContext;
        this.transactionId = NOT_COMMITTED_TRANSACTION_ID;
        this.commitTime = NOT_COMMITTED_TRANSACTION_COMMIT_TIME;
        this.clientInfo = clientInfo;
        PageCursorTracer pageCursorTracer = cursorTracerSupplier.get();
        this.statistics.init( currentThread().getId(), pageCursorTracer );
        this.currentStatement.initialize( statementLocks, pageCursorTracer );
        this.operations.initialize();
        this.initializationTrace = traceProvider.getTraceInfo();
        return this;
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
     * {@link #close()} and {@link #release()} calls.
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
    public DatabaseId getDatabaseId()
    {
        return databaseId;
    }

    private boolean markForTerminationIfPossible( Status reason )
    {
        if ( canBeTerminated() )
        {
            failure = true;
            terminationReason = reason;
            if ( statementLocks != null )
            {
                statementLocks.stop();
            }
            transactionMonitor.transactionTerminated( hasTxStateWithChanges() );
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
    public AvailabilityGuard getAvailabilityGuard()
    {
        return databaseAvailabilityGuard;
    }

    @Override
    public Map<String, Object> getMetaData()
    {
        return userMetaData;
    }

    @Override
    public KernelStatement acquireStatement()
    {
        assertTransactionOpen();
        currentStatement.acquire();
        return currentStatement;
    }

    @Override
    public IndexDescriptor indexUniqueCreate( IndexBackedConstraintDescriptor constraint, String provider )
    {
        return operations.indexUniqueCreate( constraint, provider );
    }

    @Override
    public long pageHits()
    {
        return cursorTracerSupplier.get().hits();
    }

    @Override
    public long pageFaults()
    {
        return cursorTracerSupplier.get().faults();
    }

    Optional<ExecutingQuery> executingQuery()
    {
        return currentStatement.executingQuery();
    }

    void upgradeToDataWrites() throws InvalidTransactionTypeKernelException
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
            transactionMonitor.upgradeToWriteTransaction();
            txState = new TxState( collectionsFactory );
        }
        return txState;
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return txState != null && txState.hasChanges();
    }

    private void markAsClosed( long txId )
    {
        assertTransactionOpen();
        closed = true;
        notifyListeners( txId );
        closeCurrentStatementIfAny();
    }

    private void notifyListeners( long txId )
    {
        for ( CloseListener closeListener : closeListeners )
        {
            closeListener.notify( txId );
        }
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
            throw new IllegalStateException( "This transaction has already been completed." );
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
        if ( closed )
        {
            throw new NotInTransactionException( "The transaction has been closed." );
        }
    }

    private boolean hasChanges()
    {
        return hasTxStateWithChanges();
    }

    @Override
    public long commit() throws TransactionFailureException
    {
        success();
        return closeTransaction();
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        failure();
        closeTransaction();
    }

    @Override
    public long closeTransaction() throws TransactionFailureException
    {
        assertTransactionOpen();
        assertTransactionNotClosing();
        closing = true;
        try
        {
            if ( failure || !success || isTerminated() )
            {
                rollback( null );
                failOnNonExplicitRollbackIfNeeded();
                return ROLLBACK;
            }
            else
            {
                return commitTransaction();
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
                release();
            }
        }
    }

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
        long txId = READ_ONLY;
        TransactionListenersState listenersState = null;
        try ( CommitEvent commitEvent = transactionEvent.beginCommitEvent() )
        {
            listenersState = eventListeners.beforeCommit( txState, this, storageReader );
            if ( listenersState != null && listenersState.isFailed() )
            {
                Throwable cause = listenersState.failure();
                throw new TransactionFailureException( Status.Transaction.TransactionHookFailed, cause, "" );
            }

            // Convert changes into commands and commit
            if ( hasChanges() )
            {
                forceThawLocks();

                // grab all optimistic locks now, locks can't be deferred any further
                statementLocks.prepareForCommit( currentStatement.lockTracer() );
                // use pessimistic locks for the rest of the commit process, locks can't be deferred any further
                Locks.Client commitLocks = statementLocks.pessimistic();

                // Gather up commands from the various sources
                Collection<StorageCommand> extractedCommands = new ArrayList<>();
                storageEngine.createCommands(
                        extractedCommands,
                        txState,
                        storageReader,
                        commandCreationContext,
                        commitLocks,
                        lastTransactionIdWhenStarted,
                        this::enforceConstraints );

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
                    TransactionHeaderInformation headerInformation = headerInformationFactory.create();
                    long timeCommitted = clocks.systemClock().millis();
                    transactionRepresentation.setHeader( headerInformation.getAdditionalHeader(),
                            headerInformation.getMasterId(),
                            headerInformation.getAuthorId(),
                            startTimeMillis, lastTransactionIdWhenStarted, timeCommitted,
                            commitLocks.getLockSessionId() );

                    // Commit the transaction
                    success = true;
                    TransactionToApply batch = new TransactionToApply( transactionRepresentation,
                            versionContextSupplier.getVersionContext() );
                    txId = transactionId = commitProcess.commit( batch, commitEvent, INTERNAL );
                    commitTime = timeCommitted;
                }
            }
            success = true;
            return txId;
        }
        catch ( ConstraintValidationException | CreateConstraintFailureException e )
        {
            throw new ConstraintViolationTransactionFailureException(
                    e.getUserMessage( new SilentTokenNameLookup( tokenRead() ) ), e );
        }
        finally
        {
            if ( !success )
            {
                rollback( listenersState );
            }
            else
            {
                afterCommit( txId, listenersState );
            }
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

            // Free any acquired id's
            if ( txState != null )
            {
                try
                {
                    // When rolling back we may have allocated some ids already, ids which are exposed to user e.g. node/relationship ids.
                    // Rollback may also occur after a failure in the middle of creating commands (for example on deleting a node
                    // that still has relationships or something like that), where ids of other more internal types have been allocated.
                    // However these other allocations are harder to track since we don't have the commands so the command extractor
                    // would have to keep track of allocated ids and have them accessible here just for the event of failure creating all commands.
                    // Would that added overhead be worth it? What happens if we don't release these ids here in rollback is that
                    // those ids won't be marked as reusable, which is bad for this session. However a restart will see those ids as
                    // reusable again so they aren't entirely lost, just for this session.
                    // So what we currently do is to release the readily available lists of created nodes/relationships.

                    txState.accept( new TxStateVisitor.Adapter()
                    {
                        @Override
                        public void visitCreatedNode( long id )
                        {
                            commandCreationContext.releaseNode( id );
                        }

                        @Override
                        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
                        {
                            commandCreationContext.releaseRelationship( id );
                        }

                        @Override
                        public void visitAddedIndex( IndexDescriptor index )
                        {
                            commandCreationContext.releaseSchema( index.getId() );
                        }
                    } );
                }
                catch ( ConstraintValidationException | CreateConstraintFailureException e )
                {
                    throw new IllegalStateException(
                            "Releasing locks during rollback should perform no constraints checking.", e );
                }
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
        assertAllowsReads();
        return operations.dataRead();
    }

    @Override
    public Write dataWrite() throws InvalidTransactionTypeKernelException
    {
        accessCapability.assertCanWrite();
        assertAllowsWrites();
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
        assertAllowsTokenReads();
        return operations.token();
    }

    @Override
    public SchemaRead schemaRead()
    {
        assertAllowsReads();
        return operations.schemaRead();
    }

    @Override
    public SchemaWrite schemaWrite() throws InvalidTransactionTypeKernelException
    {
        accessCapability.assertCanWrite();
        assertAllowsSchemaWrites();

        upgradeToSchemaWrites();
        return operations;
    }

    @Override
    public org.neo4j.internal.kernel.api.Locks locks()
    {
       return operations.locks();
    }

    @Override
    public void freezeLocks()
    {
        StatementLocks locks = statementLocks;
        if ( !(locks instanceof FrozenStatementLocks) )
        {
            this.statementLocks = new FrozenStatementLocks( locks );
        }
        else
        {
            ((FrozenStatementLocks)locks).freeze();
        }
    }

    @Override
    public void thawLocks()
    {
        StatementLocks locks = statementLocks;
        if ( locks instanceof FrozenStatementLocks )
        {
            FrozenStatementLocks frozenLocks = (FrozenStatementLocks) locks;
            if ( frozenLocks.thaw() )
            {
                statementLocks = frozenLocks.getRealStatementLocks();
            }
        }
        else
        {
            throw new IllegalStateException( "Attempted to thaw Transaction locks that were not frozen." );
        }
    }

    private void forceThawLocks()
    {
        StatementLocks locks = statementLocks;
        if ( locks instanceof FrozenStatementLocks )
        {
            statementLocks = ((FrozenStatementLocks) locks).getRealStatementLocks();
        }
    }

    public StatementLocks statementLocks()
    {
        assertOpen();
        return statementLocks;
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

    private void assertAllowsTokenReads()
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsTokenReads() )
        {
            throw accessMode.onViolation( format( "Read operations are not allowed for %s.", securityContext().description() ) );
        }
    }

    private void assertAllowsReads()
    {
        AccessMode accessMode = securityContext().mode();
        if ( !accessMode.allowsReads() )
        {
            throw accessMode.onViolation( format( "Read operations are not allowed for %s.", securityContext().description() ) );
        }
    }

    private void assertAllowsWrites()
    {
        AccessMode accessMode = securityContext().mode();
        if ( !accessMode.allowsWrites() )
        {
            throw accessMode.onViolation( format( "Write operations are not allowed for %s.", securityContext().description() ) );
        }
    }

    private void assertAllowsSchemaWrites()
    {
        AccessMode accessMode = securityContext().mode();
        if ( !accessMode.allowsSchemaWrites() )
        {
            throw accessMode.onViolation( format( "Schema operations are not allowed for %s.", securityContext().description() ) );
        }
    }

    public final void assertAllowsTokenCreates()
    {
        AccessMode accessMode = securityContext().mode();
        if ( !accessMode.allowsTokenCreates() )
        {
            throw accessMode.onViolation( format( "Token create operations are not allowed for %s.", securityContext().description() ) );
        }
    }

    private void afterCommit( long txId, TransactionListenersState listenersState )
    {
        try
        {
            markAsClosed( txId );
            eventListeners.afterCommit( listenersState );
        }
        finally
        {
            transactionMonitor.transactionFinished( true, hasTxStateWithChanges() );
        }
    }

    private void afterRollback( TransactionListenersState listenersState )
    {
        try
        {
            markAsClosed( ROLLBACK );
            eventListeners.afterRollback( listenersState );
        }
        finally
        {
            transactionMonitor.transactionFinished( false, hasTxStateWithChanges() );
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
     * Release resources held up by this transaction & return it to the transaction pool.
     * This method is guarded by {@link #terminationReleaseLock} to coordinate concurrent
     * {@link #markForTermination(Status)} calls.
     */
    private void release()
    {
        terminationReleaseLock.lock();
        try
        {
            forceThawLocks();
            statementLocks.close();
            statementLocks = null;
            terminationReason = null;
            type = null;
            securityContext = null;
            transactionEvent = null;
            txState = null;
            collectionsFactory.release();
            closeListeners.clear();
            reuseCount++;
            userMetaData = emptyMap();
            clientInfo = null;
            userTransactionId = 0;
            statistics.reset();
            releaseStatementResources();
            operations.release();
            initializationTrace = null;
            pool.release( this );
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
    public void registerCloseListener( CloseListener listener )
    {
        assert listener != null;
        closeListeners.add( listener );
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
        String lockSessionId;
        StatementLocks locks = statementLocks;
        if ( locks == null )
        {
            lockSessionId = "statementLocks == null";
        }
        else
        {
            if ( locks instanceof FrozenStatementLocks )
            {
                locks = ((FrozenStatementLocks)locks).getRealStatementLocks();
            }
            lockSessionId = String.valueOf( locks.pessimistic().getLockSessionId() );
        }

        return "KernelTransaction[" + lockSessionId + "]";
    }

    public void dispose()
    {
        storageReader.close();
        commandCreationContext.close();
    }

    /**
     * This method will be invoked by concurrent threads for inspecting the locks held by this transaction.
     * <p>
     * The fact that {@link #statementLocks} is a volatile fields, grants us enough of a read barrier to get a good
     * enough snapshot of the lock state (as long as the underlying methods give us such guarantees).
     *
     * @return the locks held by this transaction.
     */
    public Stream<? extends ActiveLock> activeLocks()
    {
        StatementLocks locks = this.statementLocks;
        return locks == null ? Stream.empty() : locks.activeLocks();
    }

    long userTransactionId()
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
        return constraintSemantics.decorateTxStateVisitor( storageReader, operations.dataRead(), operations.cursors(), txState, txStateVisitor );
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

    public static class Statistics
    {
        private volatile long cpuTimeNanosWhenQueryStarted;
        private volatile long heapAllocatedBytesWhenQueryStarted;
        private volatile long waitingTimeNanos;
        private volatile long transactionThreadId;
        private volatile PageCursorTracer pageCursorTracer = PageCursorTracer.NULL;
        private final KernelTransactionImplementation transaction;
        private final AtomicReference<CpuClock> cpuClockRef;
        private final AtomicReference<HeapAllocation> heapAllocationRef;
        private CpuClock cpuClock;
        private HeapAllocation heapAllocation;

        public Statistics( KernelTransactionImplementation transaction, AtomicReference<CpuClock> cpuClockRef,
                AtomicReference<HeapAllocation> heapAllocationRef )
        {
            this.transaction = transaction;
            this.cpuClockRef = cpuClockRef;
            this.heapAllocationRef = heapAllocationRef;
        }

        protected void init( long threadId, PageCursorTracer pageCursorTracer )
        {
            this.cpuClock = cpuClockRef.get();
            this.heapAllocation = heapAllocationRef.get();
            this.transactionThreadId = threadId;
            this.pageCursorTracer = pageCursorTracer;
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
         * Returns amount of direct memory allocated by current transaction.
         *
         * @return amount of direct memory allocated by the thread in bytes.
         */
        long directAllocatedBytes()
        {
            return transaction.collectionsFactory.getMemoryTracker().usedDirectMemory();
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
            return pageCursorTracer.accumulatedHits();
        }

        /**
         * Return total number of page cache faults that current transaction performed
         * @return total page cache faults
         */
        long totalTransactionPageCacheFaults()
        {
            return pageCursorTracer.accumulatedFaults();
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
            pageCursorTracer = PageCursorTracer.NULL;
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
}
