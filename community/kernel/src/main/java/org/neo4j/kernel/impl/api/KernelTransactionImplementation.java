/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.collection.pool.Pool;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KeyReadTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.cursor.DegreeItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.procedures.ProcedureDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.util.collection.ArrayCollection;

import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.INTERNAL;

/**
 * This class should replace the {@link org.neo4j.kernel.api.KernelTransaction} interface, and take its name, as soon
 * as
 * {@code TransitionalTxManagementKernelTransaction} is gone from {@code server}.
 */
public class KernelTransactionImplementation implements KernelTransaction, TxStateHolder
{
    /*
     * IMPORTANT:
     * This class is pooled and re-used. If you add *any* state to it, you *must* make sure that the #initialize()
     * method resets that state for re-use.
     */

    private enum TransactionType
    {
        ANY,
        DATA
                {
                    @Override
                    TransactionType upgradeToSchemaTransaction() throws InvalidTransactionTypeKernelException
                    {
                        throw new InvalidTransactionTypeKernelException(
                                "Cannot perform schema updates in a transaction that has performed data updates." );
                    }
                },
        SCHEMA
                {
                    @Override
                    TransactionType upgradeToDataTransaction() throws InvalidTransactionTypeKernelException
                    {
                        throw new InvalidTransactionTypeKernelException(
                                "Cannot perform data updates in a transaction that has performed schema updates." );
                    }
                };

        TransactionType upgradeToDataTransaction() throws InvalidTransactionTypeKernelException
        {
            return DATA;
        }

        TransactionType upgradeToSchemaTransaction() throws InvalidTransactionTypeKernelException
        {
            return SCHEMA;
        }
    }

    // Logic
    private final SchemaWriteGuard schemaWriteGuard;
    private final IndexingService indexService;
    private final TransactionHooks hooks;
    private final LabelScanStore labelScanStore;
    private final SchemaStorage schemaStorage;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final SchemaIndexProviderMap providerMap;
    private final UpdateableSchemaState schemaState;
    private final StatementOperationParts operations;
    private final Pool<KernelTransactionImplementation> pool;
    private final ConstraintSemantics constraintSemantics;
    // State
    private final TransactionRecordState recordState;
    private final CountsRecordState counts = new CountsRecordState();
    // For committing
    private final TransactionHeaderInformationFactory headerInformationFactory;
    private final TransactionCommitProcess commitProcess;
    private final TransactionMonitor transactionMonitor;
    private final StoreReadLayer storeLayer;
    private final ProcedureCache procedureCache;
    private final StatementLocksFactory statementLocksFactory;
    private final Clock clock;
    private final TransactionToRecordStateVisitor txStateToRecordStateVisitor = new TransactionToRecordStateVisitor();
    private final Collection<Command> extractedCommands = new ArrayCollection<>( 32 );
    private final boolean txTerminationAwareLocks;
    private TransactionState txState;
    private LegacyIndexTransactionState legacyIndexTransactionState;
    private TransactionType transactionType = TransactionType.ANY;
    private TransactionHooks.TransactionHooksState hooksState;
    private boolean beforeHookInvoked;
    private StatementLocks statementLocks;
    private StoreStatement storeStatement;
    private boolean closing, closed;
    private boolean failure, success;
    private volatile Status terminationReason;
    // Some header information
    private long startTimeMillis;
    private long lastTransactionIdWhenStarted;
    private long lastTransactionTimestampWhenStarted;

    /**
     * Implements reusing the same underlying {@link KernelStatement} for overlapping statements.
     */
    private KernelStatement currentStatement;
    // Event tracing
    private final TransactionTracer tracer;
    private TransactionEvent transactionEvent;
    private CloseListener closeListener;
    private final NeoStoreTransactionContext context;
    private volatile int reuseCount;

    /**
     * Lock prevents transaction {@link #markForTermination(Status)}  transction termination} from interfering with {@link
     * #close() transaction commit} and specifically with {@link #release()}.
     * Termination can run concurrently with commit and we need to make sure that it terminates the right lock client
     * and the right transaction (with the right {@link #reuseCount}) because {@link KernelTransactionImplementation}
     * instances are pooled.
     */
    private final Lock terminationReleaseLock = new ReentrantLock();

    public KernelTransactionImplementation( StatementOperationParts operations,
                                            SchemaWriteGuard schemaWriteGuard,
                                            LabelScanStore labelScanStore,
                                            IndexingService indexService,
                                            UpdateableSchemaState schemaState,
                                            TransactionRecordState recordState,
                                            SchemaIndexProviderMap providerMap,
                                            NeoStores neoStores,
                                            TransactionHooks hooks,
                                            ConstraintIndexCreator constraintIndexCreator,
                                            TransactionHeaderInformationFactory headerInformationFactory,
                                            TransactionCommitProcess commitProcess,
                                            TransactionMonitor transactionMonitor,
                                            StoreReadLayer storeLayer,
                                            LegacyIndexTransactionState legacyIndexTransactionState,
                                            Pool<KernelTransactionImplementation> pool,
                                            ConstraintSemantics constraintSemantics,
                                            Clock clock,
                                            TransactionTracer tracer,
                                            ProcedureCache procedureCache,
                                            StatementLocksFactory statementLocksFactory,
                                            NeoStoreTransactionContext context,
                                            boolean txTerminationAwareLocks )
    {
        this.operations = operations;
        this.schemaWriteGuard = schemaWriteGuard;
        this.labelScanStore = labelScanStore;
        this.indexService = indexService;
        this.recordState = recordState;
        this.providerMap = providerMap;
        this.schemaState = schemaState;
        this.txTerminationAwareLocks = txTerminationAwareLocks;
        this.hooks = hooks;
        this.constraintIndexCreator = constraintIndexCreator;
        this.headerInformationFactory = headerInformationFactory;
        this.commitProcess = commitProcess;
        this.transactionMonitor = transactionMonitor;
        this.storeLayer = storeLayer;
        this.procedureCache = procedureCache;
        this.statementLocksFactory = statementLocksFactory;
        this.context = context;
        this.legacyIndexTransactionState = new CachingLegacyIndexTransactionState( legacyIndexTransactionState );
        this.pool = pool;
        this.constraintSemantics = constraintSemantics;
        this.clock = clock;
        this.schemaStorage = new SchemaStorage( neoStores.getSchemaStore() );
        this.tracer = tracer;
    }

    /**
     * Reset this transaction to a vanilla state, turning it into a logically new transaction.
     */
    public KernelTransactionImplementation initialize( long lastCommittedTx, long lastTimeStamp )
    {
        this.statementLocks = statementLocksFactory.newInstance();
        this.terminationReason = null;
        this.closing = closed = failure = success = false;
        this.transactionType = TransactionType.ANY;
        this.beforeHookInvoked = false;
        this.recordState.initialize( lastCommittedTx );
        this.startTimeMillis = clock.currentTimeMillis();
        this.lastTransactionIdWhenStarted = lastCommittedTx;
        this.lastTransactionTimestampWhenStarted = lastTimeStamp;
        this.transactionEvent = tracer.beginTransaction();
        assert transactionEvent != null : "transactionEvent was null!";
        this.storeStatement = storeLayer.acquireStatement();
        this.closeListener = null;
        return this;
    }

    int getReuseCount()
    {
        return reuseCount;
    }

    @Override
    public long localStartTime()
    {
        return startTimeMillis;
    }

    @Override
    public long lastTransactionIdWhenStarted()
    {
        return lastTransactionIdWhenStarted;
    }

    @Override
    public void success()
    {
        this.success = true;
    }

    @Override
    public void failure()
    {
        failure = true;
    }

    @Override
    public Status getReasonIfTerminated()
    {
        return terminationReason;
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
        if ( !canBeTerminated() )
        {
            return;
        }

        int initialReuseCount = reuseCount;
        terminationReleaseLock.lock();
        try
        {
            // this instance could have been reused, make sure we are trying to terminate the right transaction
            // without this check there exists a possibility to terminate lock client that has just been returned to
            // the pool or a transaction that was reused and represents a completely different logical transaction
            boolean stillSameTransaction = initialReuseCount == reuseCount;
            if ( stillSameTransaction && canBeTerminated() )
            {
                failure = true;
                terminationReason = reason;
                if ( txTerminationAwareLocks && statementLocks != null )
                {
                    statementLocks.stop();
                }
                transactionMonitor.transactionTerminated();
            }
        }
        finally
        {
            terminationReleaseLock.unlock();
        }
    }

    @Override
    public boolean isOpen()
    {
        return !closed && !closing;
    }

    @Override
    public KernelStatement acquireStatement()
    {
        assertTransactionOpen();
        if ( currentStatement == null )
        {
            currentStatement = new KernelStatement( this, new IndexReaderFactory.Caching( indexService ),
                    labelScanStore, this, statementLocks, operations, storeStatement );
        }
        currentStatement.acquire();
        return currentStatement;
    }

    public void releaseStatement( Statement statement )
    {
        assert currentStatement == statement;
        currentStatement = null;
    }

    public void upgradeToDataTransaction() throws InvalidTransactionTypeKernelException
    {
        transactionType = transactionType.upgradeToDataTransaction();
    }

    public void upgradeToSchemaTransaction() throws InvalidTransactionTypeKernelException
    {
        doUpgradeToSchemaTransaction();
        transactionType = transactionType.upgradeToSchemaTransaction();
    }

    public void doUpgradeToSchemaTransaction() throws InvalidTransactionTypeKernelException
    {
        schemaWriteGuard.assertSchemaWritesAllowed();
    }

    private void dropCreatedConstraintIndexes() throws TransactionFailureException
    {
        if ( hasTxStateWithChanges() )
        {
            for ( IndexDescriptor createdConstraintIndex : txState().constraintIndexesCreatedInTx() )
            {
                try
                {
                    // TODO logically, which statement should this operation be performed on?
                    constraintIndexCreator.dropUniquenessConstraintIndex( createdConstraintIndex );
                }
                catch ( DropIndexFailureException e )
                {
                    throw new IllegalStateException( "Constraint index that was created in a transaction should be " +
                            "possible to drop during rollback of that transaction.", e );
                }
            }
        }
    }

    @Override
    public TransactionState txState()
    {
        if ( txState == null )
        {
            txState = new TxState();
        }
        return txState;
    }

    @Override
    public LegacyIndexTransactionState legacyIndexTxState()
    {
        return legacyIndexTransactionState;
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return txState != null && txState.hasChanges();
    }

    private void closeTransaction()
    {
        assertTransactionOpen();
        closed = true;
        closeCurrentStatementIfAny();
        if ( closeListener != null )
        {
            closeListener.notify( success );
        }
    }

    private void closeCurrentStatementIfAny()
    {
        if ( currentStatement != null )
        {
            currentStatement.forceClose();
            currentStatement = null;
        }
    }

    private void assertTransactionNotClosing()
    {
        if ( closing )
        {
            throw new IllegalStateException( "This transaction is already being closed." );
        }
    }

    private void prepareRecordChangesFromTransactionState()
            throws ConstraintValidationKernelException, CreateConstraintFailureException
    {
        if ( hasTxStateWithChanges() )
        {
            txState().accept( txStateVisitor() );
            txStateToRecordStateVisitor.done();
        }
    }

    private TxStateVisitor txStateVisitor()
    {
        return constraintSemantics
                .decorateTxStateVisitor( operations, storeStatement, storeLayer, this, txStateToRecordStateVisitor );
    }

    private void assertTransactionOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "This transaction has already been completed." );
        }
    }

    private boolean hasChanges()
    {
        return hasTxStateWithChanges() ||
                recordState.hasChanges() ||
                legacyIndexTransactionState.hasChanges() ||
                counts.hasChanges();
    }

    // Only for test-access
    public TransactionRecordState getTransactionRecordState()
    {
        return recordState;
    }

    @Override
    public void close() throws TransactionFailureException
    {
        assertTransactionOpen();
        assertTransactionNotClosing();
        closeCurrentStatementIfAny();
        closing = true;
        try
        {
            if ( failure || !success || isTerminated() )
            {
                rollback();
                failOnNonExplicitRollbackIfNeeded();
            }
            else
            {
                commit();
            }
        }
        finally
        {
            try
            {
                closed = true;
                closing = false;
                transactionEvent.setSuccess( success );
                transactionEvent.setFailure( failure );
                transactionEvent.setTransactionType( transactionType.name() );
                transactionEvent.setReadOnly( txState == null || !txState.hasChanges() );
                transactionEvent.close();
                transactionEvent = null;
                legacyIndexTransactionState.clear();
                recordState.clear();
                counts.clear();
                txState = null;
                hooksState = null;
                closeListener = null;
            }
            finally
            {
                release();
            }
        }
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
            throw new TransactionFailureException( Status.Transaction.MarkedAsFailed,
                    "Transaction rolled back even if marked as successful" );
        }
    }

    protected void dispose()
    {
        if ( statementLocks != null )
        {
            statementLocks.close();
        }

        this.statementLocks = null;
        this.transactionType = null;
        this.hooksState = null;
        this.txState = null;
        this.legacyIndexTransactionState = null;

        if ( storeStatement != null )
        {
            this.storeStatement.close();
            this.storeStatement = null;
        }
    }

    private void commit() throws TransactionFailureException
    {
        boolean success = false;

        try ( CommitEvent commitEvent = transactionEvent.beginCommitEvent() )
        {
            // Trigger transaction "before" hooks.
            if ( hasTxStateWithChanges() )
            {
                if ( txState.hasDataChanges() )
                {
                    try
                    {
                        if ( (hooksState = hooks.beforeCommit( txState, this, storeLayer )) != null && hooksState.failed() )
                        {
                            throw new TransactionFailureException( Status.Transaction.HookFailed, hooksState.failure(),
                                    "" );
                        }
                    }
                    finally
                    {
                        beforeHookInvoked = true;
                    }
                }

                prepareStateForCommit();
            }

            // Convert changes into commands and commit
            if ( hasChanges() )
            {
                try ( LockGroup lockGroup = new LockGroup() )
                {
                    // Gather up commands from the various sources
                    extractedCommands.clear();
                    recordState.extractCommands( extractedCommands );
                    legacyIndexTransactionState.extractCommands( extractedCommands );
                    counts.extractCommands( extractedCommands );

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
                        transactionRepresentation.setHeader( headerInformation.getAdditionalHeader(),
                                headerInformation.getMasterId(),
                                headerInformation.getAuthorId(),
                                startTimeMillis, lastTransactionIdWhenStarted, clock.currentTimeMillis(),
                                statementLocks.pessimistic().getLockSessionId() );

                        // Commit the transaction
                        commitProcess.commit( transactionRepresentation, lockGroup, commitEvent, INTERNAL );
                    }
                }
            }
            success = true;
        }
        catch ( ConstraintValidationKernelException | CreateConstraintFailureException e )
        {
            throw new ConstraintViolationTransactionFailureException(
                    e.getUserMessage( new KeyReadTokenNameLookup( operations.keyReadOperations() ) ), e );
        }
        finally
        {
            if ( !success )
            {
                rollback();
            }
            else
            {
                afterCommit();
            }
        }
    }

    private void prepareStateForCommit() throws ConstraintValidationKernelException, CreateConstraintFailureException
    {
        // grab all optimistic locks now, locks can't be deferred any further
        statementLocks.prepareForCommit();

        // use pessimistic locks for the rest of the commit process, locks can't be deferred any further
        context.init( statementLocks.pessimistic() );

        prepareRecordChangesFromTransactionState();
    }

    private void rollback() throws TransactionFailureException
    {
        try
        {
            try
            {
                dropCreatedConstraintIndexes();
            }
            catch ( IllegalStateException | SecurityException e )
            {
                throw new TransactionFailureException( Status.Transaction.CouldNotRollback, e,
                        "Could not drop created constraint indexes" );
            }

            // Free any acquired id's
            if ( txState != null )
            {
                try
                {
                    txState.accept( new TxStateVisitor.Adapter()
                    {
                        @Override
                        public void visitCreatedNode( long id )
                        {
                            storeLayer.releaseNode( id );
                        }

                        @Override
                        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
                        {
                            storeLayer.releaseRelationship( id );
                        }
                    } );
                }
                catch ( ConstraintValidationKernelException | CreateConstraintFailureException e )
                {
                    throw new IllegalStateException(
                            "Releasing locks during rollback should perform no constraints checking.", e );
                }
            }
        }
        finally
        {
            afterRollback();
        }
    }

    private void afterCommit()
    {
        try
        {
            closeTransaction();
            if ( beforeHookInvoked )
            {
                hooks.afterCommit( txState, this, hooksState );
            }
        }
        finally
        {
            transactionMonitor.transactionFinished( true );
        }
    }

    private void afterRollback()
    {
        try
        {
            closeTransaction();
            if ( beforeHookInvoked )
            {
                hooks.afterRollback( txState, this, hooksState );
            }
        }
        finally
        {
            transactionMonitor.transactionFinished( false );
        }
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
            statementLocks.close();
            statementLocks = null;
            terminationReason = null;
            pool.release( this );
            if ( storeStatement != null )
            {
                storeStatement.close();
                storeStatement = null;
            }
        }
        finally
        {
            reuseCount++;
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

    private boolean isTerminated()
    {
        return terminationReason != null;
    }

    @Override
    public long lastTransactionTimestampWhenStarted()
    {
        return lastTransactionTimestampWhenStarted;
    }

    private class TransactionToRecordStateVisitor extends TxStateVisitor.Adapter
    {
        private final RelationshipDataExtractor edge = new RelationshipDataExtractor();
        private boolean clearState;

        void done()
        {
            try
            {
                if ( clearState )
                {
                    schemaState.clear();
                }
            }
            finally
            {
                clearState = false;
            }
        }

        @Override
        public void visitCreatedNode( long id )
        {
            recordState.nodeCreate( id );
            counts.incrementNodeCount( ANY_LABEL, 1 );
        }

        @Override
        public void visitDeletedNode( long id )
        {
            try ( StoreStatement statement = storeLayer.acquireStatement() )
            {
                counts.incrementNodeCount( ANY_LABEL, -1 );
                try ( Cursor<NodeItem> node = statement.acquireSingleNodeCursor( id ) )
                {
                    if ( node.next() )
                    {
                        // TODO Rewrite this to use cursors directly instead of iterator
                        PrimitiveIntIterator labels = node.get().getLabels();
                        if ( labels.hasNext() )
                        {
                            final int[] removed = PrimitiveIntCollections.asArray( labels );
                            for ( int label : removed )
                            {
                                counts.incrementNodeCount( label, -1 );
                            }

                            try ( Cursor<DegreeItem> degrees = node.get().degrees() )
                            {
                                while ( degrees.next() )
                                {
                                    DegreeItem degree = degrees.get();
                                    for ( int label : removed )
                                    {
                                        updateRelationshipsCountsFromDegrees( degree.type(), label, -degree.outgoing(),
                                                -degree.incoming() );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            recordState.nodeDelete( id );
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
        {
            try
            {
                updateRelationshipCount( startNode, type, endNode, 1 );
            }
            catch ( EntityNotFoundException e )
            {
                throw new IllegalStateException( "Nodes with added relationships should exist.", e );
            }

            // record the state changes to be made to the store
            recordState.relCreate( id, type, startNode, endNode );
        }

        @Override
        public void visitDeletedRelationship( long id )
        {
            try
            {
                storeLayer.relationshipVisit( id, edge );
                updateRelationshipCount( edge.startNode(), edge.type(), edge.endNode(), -1 );
            }
            catch ( EntityNotFoundException e )
            {
                throw new IllegalStateException(
                        "Relationship being deleted should exist along with its nodes.", e );
            }

            // record the state changes to be made to the store
            recordState.relDelete( id );
        }

        @Override
        public void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added,
                Iterator<DefinedProperty> changed, Iterator<Integer> removed )
        {
            while ( removed.hasNext() )
            {
                recordState.nodeRemoveProperty( id, removed.next() );
            }
            while ( changed.hasNext() )
            {
                DefinedProperty prop = changed.next();
                recordState.nodeChangeProperty( id, prop.propertyKeyId(), prop.value() );
            }
            while ( added.hasNext() )
            {
                DefinedProperty prop = added.next();
                recordState.nodeAddProperty( id, prop.propertyKeyId(), prop.value() );
            }
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added,
                Iterator<DefinedProperty> changed, Iterator<Integer> removed )
        {
            while ( removed.hasNext() )
            {
                recordState.relRemoveProperty( id, removed.next() );
            }
            while ( changed.hasNext() )
            {
                DefinedProperty prop = changed.next();
                recordState.relChangeProperty( id, prop.propertyKeyId(), prop.value() );
            }
            while ( added.hasNext() )
            {
                DefinedProperty prop = added.next();
                recordState.relAddProperty( id, prop.propertyKeyId(), prop.value() );
            }
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                Iterator<Integer> removed )
        {
            while ( removed.hasNext() )
            {
                recordState.graphRemoveProperty( removed.next() );
            }
            while ( changed.hasNext() )
            {
                DefinedProperty prop = changed.next();
                recordState.graphChangeProperty( prop.propertyKeyId(), prop.value() );
            }
            while ( added.hasNext() )
            {
                DefinedProperty prop = added.next();
                recordState.graphAddProperty( prop.propertyKeyId(), prop.value() );
            }
        }

        @Override
        public void visitNodeLabelChanges( long id, final Set<Integer> added, final Set<Integer> removed )
        {
            try ( StoreStatement statement = storeLayer.acquireStatement() )
            {
                // update counts
                if ( !(added.isEmpty() && removed.isEmpty()) )
                {
                    for ( Integer label : added )
                    {
                        counts.incrementNodeCount( label, 1 );
                    }
                    for ( Integer label : removed )
                    {
                        counts.incrementNodeCount( label, -1 );
                    }
                    // get the relationship counts from *before* this transaction,
                    // the relationship changes will compensate for what happens during the transaction
                    try ( Cursor<NodeItem> node = statement.acquireSingleNodeCursor( id ) )
                    {
                        if ( node.next() )
                        {
                            try ( Cursor<DegreeItem> degrees = node.get().degrees() )
                            {
                                while ( degrees.next() )
                                {
                                    DegreeItem degree = degrees.get();

                                    for ( Integer label : added )
                                    {
                                        updateRelationshipsCountsFromDegrees( degree.type(), label, degree.outgoing(),
                                                degree.incoming() );
                                    }
                                    for ( Integer label : removed )
                                    {
                                        updateRelationshipsCountsFromDegrees( degree.type(), label, -degree.outgoing(),
                                                -degree.incoming() );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // record the state changes to be made to the store
            for ( Integer label : removed )
            {
                recordState.removeLabelFromNode( label, id );
            }
            for ( Integer label : added )
            {
                recordState.addLabelToNode( label, id );
            }
        }

        @Override
        public void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {
            SchemaIndexProvider.Descriptor providerDescriptor = providerMap.getDefaultProvider()
                    .getProviderDescriptor();
            IndexRule rule;
            if ( isConstraintIndex )
            {
                rule = IndexRule.constraintIndexRule( schemaStorage.newRuleId(), element.getLabelId(),
                        element.getPropertyKeyId(), providerDescriptor,
                        null );
            }
            else
            {
                rule = IndexRule.indexRule( schemaStorage.newRuleId(), element.getLabelId(),
                        element.getPropertyKeyId(), providerDescriptor );
            }
            recordState.createSchemaRule( rule );
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {
            SchemaStorage.IndexRuleKind kind = isConstraintIndex ?
                    SchemaStorage.IndexRuleKind.CONSTRAINT
                    : SchemaStorage.IndexRuleKind.INDEX;
            IndexRule rule = schemaStorage.indexRule( element.getLabelId(), element.getPropertyKeyId(), kind );
            recordState.dropSchemaRule( rule );
        }

        @Override
        public void visitAddedUniquePropertyConstraint( UniquenessConstraint element )
        {
            clearState = true;
            long constraintId = schemaStorage.newRuleId();
            IndexRule indexRule = schemaStorage.indexRule(
                    element.label(),
                    element.propertyKey(),
                    SchemaStorage.IndexRuleKind.CONSTRAINT );
            recordState.createSchemaRule( constraintSemantics
                    .writeUniquePropertyConstraint( constraintId, element.label(), element.propertyKey(),
                            indexRule.getId() ) );
            recordState.setConstraintIndexOwner( indexRule, constraintId );
        }

        @Override
        public void visitRemovedUniquePropertyConstraint( UniquenessConstraint element )
        {
            try
            {
                clearState = true;
                UniquePropertyConstraintRule rule = schemaStorage
                        .uniquenessConstraint( element.label(), element.propertyKey() );
                recordState.dropSchemaRule( rule );
            }
            catch ( SchemaRuleNotFoundException e )
            {
                throw new ThisShouldNotHappenError(
                        "Tobias Lindaaker",
                        "Constraint to be removed should exist, since its existence should " +
                                "have been validated earlier and the schema should have been locked." );
            }
            catch ( DuplicateSchemaRuleException de )
            {
                throw new IllegalStateException( "Multiple constraints found for specified label and property." );
            }
            // Remove the index for the constraint as well
            visitRemovedIndex( new IndexDescriptor( element.label(), element.propertyKey() ), true );
        }

        @Override
        public void visitAddedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
                throws CreateConstraintFailureException
        {
            clearState = true;
            recordState.createSchemaRule( constraintSemantics.writeNodePropertyExistenceConstraint(
                    schemaStorage.newRuleId(), element.label(), element.propertyKey() ) );
        }

        @Override
        public void visitRemovedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
        {
            try
            {
                clearState = true;
                recordState.dropSchemaRule(
                        schemaStorage.nodePropertyExistenceConstraint( element.label(), element.propertyKey() ) );
            }
            catch ( SchemaRuleNotFoundException e )
            {
                throw new IllegalStateException(
                        "Node property existence constraint to be removed should exist, since its existence should " +
                        "have been validated earlier and the schema should have been locked." );
            }
            catch ( DuplicateSchemaRuleException de )
            {
                throw new IllegalStateException( "Multiple node property constraints found for specified label and " +
                                                 "property." );
            }
        }

        @Override
        public void visitAddedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
                throws CreateConstraintFailureException
        {
            clearState = true;
            recordState.createSchemaRule( constraintSemantics.writeRelationshipPropertyExistenceConstraint(
                    schemaStorage.newRuleId(), element.relationshipType(), element.propertyKey() ) );
        }

        @Override
        public void visitRemovedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
        {
            try
            {
                clearState = true;
                SchemaRule rule = schemaStorage.relationshipPropertyExistenceConstraint( element.relationshipType(),
                        element.propertyKey() );
                recordState.dropSchemaRule( rule );
            }
            catch ( SchemaRuleNotFoundException e )
            {
                throw new IllegalStateException(
                        "Relationship property existence constraint to be removed should exist, since its existence " +
                        "should have been validated earlier and the schema should have been locked." );
            }
            catch ( DuplicateSchemaRuleException re )
            {
                throw new IllegalStateException( "Multiple relationship property constraints found for specified " +
                                                 "property and relationship type." );
            }
        }

        @Override
        public void visitCreatedLabelToken( String name, int id )
        {
            recordState.createLabelToken( name, id );
        }

        @Override
        public void visitCreatedPropertyKeyToken( String name, int id )
        {
            recordState.createPropertyKeyToken( name, id );
        }

        @Override
        public void visitCreatedRelationshipTypeToken( String name, int id )
        {
            recordState.createRelationshipTypeToken( name, id );
        }

        @Override
        public void visitCreatedNodeLegacyIndex( String name, Map<String, String> config )
        {
            legacyIndexTransactionState.createIndex( IndexEntityType.Node, name, config );
        }

        @Override
        public void visitCreatedRelationshipLegacyIndex( String name, Map<String, String> config )
        {
            legacyIndexTransactionState.createIndex( IndexEntityType.Relationship, name, config );
        }

        @Override
        public void visitCreatedProcedure( ProcedureDescriptor procedureDescriptor )
        {
            // TODO: This is a temporary measure to allow trialing procedures without changing the store format. Clearly, this is not safe or useful for
            // production. This will need to be changed before we release a useful 3.x series release.
            procedureCache.createProcedure( procedureDescriptor );
        }

        @Override
        public void visitDroppedProcedure( ProcedureDescriptor procedureDescriptor )
        {
            procedureCache.dropProcedure( procedureDescriptor );
        }
    }

    private void updateRelationshipsCountsFromDegrees( int type, int label, long outgoing, long incoming )
    {
        // untyped
        counts.incrementRelationshipCount( label, ANY_RELATIONSHIP_TYPE, ANY_LABEL, outgoing );
        counts.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, label, incoming );
        // typed
        counts.incrementRelationshipCount( label, type, ANY_LABEL, outgoing );
        counts.incrementRelationshipCount( ANY_LABEL, type, label, incoming );
    }

    private void updateRelationshipCount( long startNode, int type, long endNode, int delta )
            throws EntityNotFoundException
    {
        updateRelationshipsCountsFromDegrees( type, ANY_LABEL, delta, 0 );
        for ( PrimitiveIntIterator startLabels = labelsOf( startNode ); startLabels.hasNext(); )
        {
            updateRelationshipsCountsFromDegrees( type, startLabels.next(), delta, 0 );
        }
        for ( PrimitiveIntIterator endLabels = labelsOf( endNode ); endLabels.hasNext(); )
        {
            updateRelationshipsCountsFromDegrees( type, endLabels.next(), 0, delta );
        }
    }

    private PrimitiveIntIterator labelsOf( long nodeId )
    {
        try ( StoreStatement statement = storeLayer.acquireStatement() )
        {
            try ( Cursor<NodeItem> node = operations.entityReadOperations().nodeCursor( this, statement, nodeId ) )
            {
                if ( node.next() )
                {
                    return node.get().getLabels();
                }
                else
                {
                    return PrimitiveIntCollections.emptyIterator();

                }
            }
        }
    }

    @Override
    public void registerCloseListener( CloseListener listener )
    {
        assert closeListener == null;
        closeListener = listener;
    }

    @Override
    public String toString()
    {
        return "KernelTransaction[" + this.statementLocks.pessimistic().getLockSessionId() + "]";
    }
}
