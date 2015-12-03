/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.collection.pool.Pool;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KeyReadTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
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
import org.neo4j.kernel.api.procedures.ProcedureDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.util.collection.ArrayCollection;

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
    private final TransactionHooks hooks;
    private final SchemaStorage schemaStorage;
    private final ConstraintIndexCreator constraintIndexCreator;
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
    private final StorageEngine storageEngine;
    private final Clock clock;
    private final TransactionToRecordStateVisitor txStateToRecordStateVisitor = new TransactionToRecordStateVisitor();
    private final Collection<Command> extractedCommands = new ArrayCollection<>( 32 );
    private TransactionState txState;
    private LegacyIndexTransactionState legacyIndexTransactionState;
    private TransactionType transactionType = TransactionType.ANY;
    private TransactionHooks.TransactionHooksState hooksState;
    private boolean beforeHookInvoked;
    private Locks.Client locks;
    private StoreStatement storeStatement;
    private boolean closing, closed;
    private boolean failure, success;
    private volatile boolean terminated;
    // Some header information
    private long startTimeMillis;
    private long lastTransactionIdWhenStarted;
    /**
     * Implements reusing the same underlying {@link KernelStatement} for overlapping statements.
     */
    private KernelStatement currentStatement;
    // Event tracing
    private final TransactionTracer tracer;
    private TransactionEvent transactionEvent;
    private CloseListener closeListener;

    public KernelTransactionImplementation( StatementOperationParts operations,
            SchemaWriteGuard schemaWriteGuard,
            UpdateableSchemaState schemaState,
            TransactionRecordState recordState,
            Locks.Client locks,
            TransactionHooks hooks,
            ConstraintIndexCreator constraintIndexCreator,
            TransactionHeaderInformationFactory headerInformationFactory,
            TransactionCommitProcess commitProcess,
            TransactionMonitor transactionMonitor,
            LegacyIndexTransactionState legacyIndexTransactionState,
            Pool<KernelTransactionImplementation> pool,
            ConstraintSemantics constraintSemantics,
            Clock clock,
            TransactionTracer tracer,
            StorageEngine storageEngine )
    {
        this.operations = operations;
        this.schemaWriteGuard = schemaWriteGuard;
        this.recordState = recordState;
        this.schemaState = schemaState;
        this.hooks = hooks;
        this.locks = locks;
        this.constraintIndexCreator = constraintIndexCreator;
        this.headerInformationFactory = headerInformationFactory;
        this.commitProcess = commitProcess;
        this.transactionMonitor = transactionMonitor;
        this.storeLayer = storageEngine.storeReadLayer();
        this.storageEngine = storageEngine;
        this.legacyIndexTransactionState = new CachingLegacyIndexTransactionState( legacyIndexTransactionState );
        this.pool = pool;
        this.constraintSemantics = constraintSemantics;
        this.clock = clock;
        this.schemaStorage = new SchemaStorage( storageEngine.neoStores().getSchemaStore() );
        this.tracer = tracer;
    }

    /**
     * Reset this transaction to a vanilla state, turning it into a logically new transaction.
     */
    public KernelTransactionImplementation initialize( long lastCommittedTx )
    {
        assert locks != null : "This transaction has been disposed off, it should not be used.";
        this.closing = closed = failure = success = false;
        this.transactionType = TransactionType.ANY;
        this.beforeHookInvoked = false;
        this.recordState.initialize( lastCommittedTx );
        this.startTimeMillis = clock.currentTimeMillis();
        this.lastTransactionIdWhenStarted = lastCommittedTx;
        this.transactionEvent = tracer.beginTransaction();
        assert transactionEvent != null : "transactionEvent was null!";
        this.storeStatement = storeLayer.acquireStatement();
        this.closeListener = null;
        return this;
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
    public boolean shouldBeTerminated()
    {
        return terminated;
    }

    @Override
    public void markForTermination()
    {
        if ( !terminated )
        {
            failure = true;
            terminated = true;
            if ( !closed )
            {
                transactionMonitor.transactionTerminated( hasTxStateWithChanges() );
            }
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
            IndexReaderFactory.Caching caching = new IndexReaderFactory.Caching( storageEngine.indexingService() );
            currentStatement = new KernelStatement( this, caching, storageEngine.labelScanStore(), this, locks,
                    operations, storeStatement );
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
            transactionMonitor.upgradeToWriteTransaction();
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
        TxStateVisitor constraintVisitor = constraintSemantics.decorateTxStateVisitor(
                operations, storeStatement, storeLayer, this, txStateToRecordStateVisitor );
        return new TransactionCountingStateVisitor( constraintVisitor, storeLayer,
                operations.entityReadOperations(), this, counts );
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

    private boolean hasDataChanges()
    {
        return hasTxStateWithChanges() && txState.hasDataChanges();
    }

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
            if ( failure || !success )
            {
                rollback();
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

    protected void dispose()
    {
        if ( locks != null )
        {
            locks.close();
        }

        this.locks = null;
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
            if ( hasDataChanges() )
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

            prepareRecordChangesFromTransactionState();

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
                        transactionRepresentation
                                .setHeader( headerInformation.getAdditionalHeader(), headerInformation.getMasterId(),
                                        headerInformation.getAuthorId(), startTimeMillis, lastTransactionIdWhenStarted,
                                        clock.currentTimeMillis(), locks.getLockSessionId() );

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
            transactionMonitor.transactionFinished( true, hasTxStateWithChanges() );
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
            transactionMonitor.transactionFinished( false, hasTxStateWithChanges() );
        }
    }

    /**
     * Release resources held up by this transaction & return it to the transaction pool.
     */
    private void release()
    {
        locks.releaseAll();
        if ( terminated )
        {
            // This transaction has been externally marked for termination.
            // Just dispose of this transaction and don't return it to the pool.
            dispose();
        }
        else
        {
            // Return this instance to the pool so that another transaction may use it.
            pool.release( this );
            if ( storeStatement != null )
            {
                storeStatement.close();
                storeStatement = null;
            }
        }
    }

    private class TransactionToRecordStateVisitor extends TxStateVisitor.Adapter
    {
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
        }

        @Override
        public void visitDeletedNode( long id )
        {
            recordState.nodeDelete( id );
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
        {
            // record the state changes to be made to the store
            recordState.relCreate( id, type, startNode, endNode );
        }

        @Override
        public void visitDeletedRelationship( long id )
        {
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
            SchemaIndexProvider.Descriptor providerDescriptor =
                    storageEngine.schemaIndexProviderMap().getDefaultProvider().getProviderDescriptor();
            IndexRule rule;
            if ( isConstraintIndex )
            {
                rule = IndexRule.constraintIndexRule( schemaStorage.newRuleId(), element.getLabelId(),
                        element.getPropertyKeyId(), providerDescriptor, null );
            }
            else
            {
                rule = IndexRule.indexRule( schemaStorage.newRuleId(), element.getLabelId(), element.getPropertyKeyId(),
                        providerDescriptor );
            }
            recordState.createSchemaRule( rule );
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {
            SchemaStorage.IndexRuleKind kind =
                    isConstraintIndex ? SchemaStorage.IndexRuleKind.CONSTRAINT : SchemaStorage.IndexRuleKind.INDEX;
            IndexRule rule = schemaStorage.indexRule( element.getLabelId(), element.getPropertyKeyId(), kind );
            recordState.dropSchemaRule( rule );
        }

        @Override
        public void visitAddedUniquePropertyConstraint( UniquenessConstraint element )
        {
            clearState = true;
            long constraintId = schemaStorage.newRuleId();
            IndexRule indexRule = schemaStorage
                    .indexRule( element.label(), element.propertyKey(), SchemaStorage.IndexRuleKind.CONSTRAINT );
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
                UniquePropertyConstraintRule rule =
                        schemaStorage.uniquenessConstraint( element.label(), element.propertyKey() );
                recordState.dropSchemaRule( rule );
            }
            catch ( SchemaRuleNotFoundException e )
            {
                throw new ThisShouldNotHappenError( "Tobias Lindaaker",
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
            recordState.createSchemaRule( constraintSemantics
                    .writeNodePropertyExistenceConstraint( schemaStorage.newRuleId(), element.label(),
                            element.propertyKey() ) );
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
                throw new IllegalStateException(
                        "Multiple node property constraints found for specified label and " + "property." );
            }
        }

        @Override
        public void visitAddedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
                throws CreateConstraintFailureException
        {
            clearState = true;
            recordState.createSchemaRule( constraintSemantics
                    .writeRelationshipPropertyExistenceConstraint( schemaStorage.newRuleId(),
                            element.relationshipType(), element.propertyKey() ) );
        }

        @Override
        public void visitRemovedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint rpec )
        {
            try
            {
                clearState = true;
                SchemaRule rule = schemaStorage
                        .relationshipPropertyExistenceConstraint( rpec.relationshipType(), rpec.propertyKey() );
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
        public void visitCreatedNodeLegacyIndex( String name, Map<String,String> config )
        {
            legacyIndexTransactionState.createIndex( IndexEntityType.Node, name, config );
        }

        @Override
        public void visitCreatedRelationshipLegacyIndex( String name, Map<String,String> config )
        {
            legacyIndexTransactionState.createIndex( IndexEntityType.Relationship, name, config );
        }

        @Override
        public void visitCreatedProcedure( ProcedureDescriptor procedureDescriptor )
        {
            // TODO: This is a temporary measure to allow trialing procedures without changing the store format.
            // Clearly, this is not safe or useful for
            // production. This will need to be changed before we release a useful 3.x series release.
            storageEngine.procedureCache().createProcedure( procedureDescriptor );
        }

        @Override
        public void visitDroppedProcedure( ProcedureDescriptor procedureDescriptor )
        {
            storageEngine.procedureCache().dropProcedure( procedureDescriptor );
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
        return "KernelTransaction[" + this.locks.getLockSessionId() + "]";
    }
}
