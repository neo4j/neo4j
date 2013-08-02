/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.ConstraintCreationException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.TransactionalException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintCreationKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.operations.WritableStatementState;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.OldTxStateBridge;
import org.neo4j.kernel.impl.api.state.OldTxStateBridgeImpl;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.state.TxStateImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class StateHandlingKernelTransaction extends DelegatingKernelTransaction implements TxState.Holder
{
    private final SchemaIndexProviderMap providerMap;
    private final PersistenceCache persistenceCache;
    private final SchemaCache schemaCache;
    private final SchemaStorage schemaStorage;
    private final PersistenceManager persistenceManager;
    private final UpdateableSchemaState schemaState;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final NodeManager nodeManager;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;

    private final OldTxStateBridge legacyStateBridge;

    private TxState txState;

    public StateHandlingKernelTransaction( StoreKernelTransaction delegate,
                                           SchemaStorage schemaStorage,
                                           TransactionState legacyState,
                                           SchemaIndexProviderMap providerMap,
                                           PersistenceCache persistenceCache,
                                           SchemaCache schemaCache,
                                           PersistenceManager persistenceManager, UpdateableSchemaState schemaState,
                                           ConstraintIndexCreator constraintIndexCreator,
                                           PropertyKeyTokenHolder propertyKeyTokenHolder,
                                           NodeManager nodeManager )
    {
        super( delegate );
        this.schemaStorage = schemaStorage;
        this.providerMap = providerMap;
        this.persistenceCache = persistenceCache;
        this.schemaCache = schemaCache;
        this.persistenceManager = persistenceManager;
        this.schemaState = schemaState;
        this.constraintIndexCreator = constraintIndexCreator;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.nodeManager = nodeManager;
        this.legacyStateBridge = new OldTxStateBridgeImpl( nodeManager, legacyState );
    }

    @Override
    public StatementOperationParts newStatementOperations()
    {
        // Store stuff
        StatementOperationParts parts = delegate.newStatementOperations();

        // + Caching
        CachingStatementOperations cachingContext = new CachingStatementOperations(
                parts.entityReadOperations(),
                parts.schemaReadOperations(),
                persistenceCache, schemaCache );
        parts = parts.override( null, null, cachingContext, null, cachingContext, null, null );

        // + Transaction-local state awareness
        AuxiliaryStoreOperations auxStoreOperations = parts.resolve( AuxiliaryStoreOperations.class );
        auxStoreOperations = new LegacyAutoIndexAuxStoreOps( auxStoreOperations, propertyKeyTokenHolder,
                nodeManager.getNodePropertyTrackers(), nodeManager.getRelationshipPropertyTrackers(), nodeManager );
        
        StateHandlingStatementOperations stateHandlingContext = new StateHandlingStatementOperations(
                parts.entityReadOperations(),
                parts.schemaReadOperations(),
                auxStoreOperations,
                constraintIndexCreator );
        parts = parts.override(
                null, null, stateHandlingContext, stateHandlingContext, stateHandlingContext, stateHandlingContext,
                new SchemaStateConcern( schemaState ) );
                
        // done
        return parts;
    }
    
    @Override
    public StatementState newStatementState()
    {
        WritableStatementState statement = (WritableStatementState) super.newStatementState();
        statement.provide( this );
        return statement;
    }

    @Override
    public void commit() throws TransactionFailureException
    {
        boolean success = false;
        try
        {
            createTransactionCommands();
            // - Ensure transaction is committed to disk at this point
            super.commit();
            success = true;
        }
        finally
        {
            if ( !success )
            {
                dropCreatedConstraintIndexes();
            }
        }

        // - commit changes from tx state to the cache
        // TODO: This should be done by log application, not by this level of the stack.
        if ( hasTxStateWithChanges() )
        {
            persistenceCache.apply( this.txState() );
        }
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        try
        {
            dropCreatedConstraintIndexes();
        }
        finally
        {
            super.rollback();
        }
    }

    private void createTransactionCommands()
    {
        if ( hasTxStateWithChanges() )
        {
            final AtomicBoolean clearState = new AtomicBoolean( false );
            txState().accept( new TxState.Visitor()
            {
                @Override
                public void visitNodeLabelChanges( long id, Set<Long> added, Set<Long> removed )
                {
                    // TODO: move store level changes here.
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
                                element.getPropertyKeyId(), providerDescriptor, null );
                    }
                    else
                    {
                        rule = IndexRule.indexRule( schemaStorage.newRuleId(), element.getLabelId(),
                                element.getPropertyKeyId(), providerDescriptor );
                    }
                    persistenceManager.createSchemaRule( rule );
                }

                @Override
                public void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex )
                {
                    try
                    {
                        IndexRule rule = schemaStorage.indexRule( element.getLabelId(), element.getPropertyKeyId() );
                        persistenceManager.dropSchemaRule( rule.getId() );
                    }
                    catch ( SchemaRuleNotFoundException e )
                    {
                        throw new ThisShouldNotHappenError(
                                "Tobias Lindaaker", "Index to be removed should exist, since its existence should have " +
                                "been validated earlier and the schema should have been locked." );
                    }
                }

                @Override
                public void visitAddedConstraint( UniquenessConstraint element, long indexId )
                {
                    try
                    {
                        constraintIndexCreator.validateConstraintIndex( element, indexId );
                    }
                    catch ( ConstraintCreationKernelException e )
                    {
                        // TODO: Revisit decision to rethrow as RuntimeException.
                        throw new ConstraintCreationException( e );
                    }
                    clearState.set( true );
                    long constraintId = schemaStorage.newRuleId();
                    persistenceManager.createSchemaRule( UniquenessConstraintRule.uniquenessConstraintRule(
                            constraintId, element.label(), element.property(), indexId ) );
                    persistenceManager.setConstraintIndexOwner( indexId, constraintId );
                }

                @Override
                public void visitRemovedConstraint( UniquenessConstraint element )
                {
                    try
                    {
                        clearState.set( true );
                        UniquenessConstraintRule rule = schemaStorage
                                .uniquenessConstraint( element.label(), element.property() );
                        persistenceManager.dropSchemaRule( rule.getId() );
                    }
                    catch ( SchemaRuleNotFoundException e )
                    {
                        throw new ThisShouldNotHappenError(
                                "Tobias Lindaaker", "Constraint to be removed should exist, since its existence should " +
                                "have been validated earlier and the schema should have been locked." );
                    }
                    // Remove the index for the constraint as well
                    visitRemovedIndex( new IndexDescriptor( element.label(), element.property() ), true );
                }
            } );
            if ( clearState.get() )
            {
                schemaState.clear();
            }
        }
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
                catch ( SchemaKernelException e )
                {
                    throw new IllegalStateException( "Constraint index that was created in a transaction should be " +
                                                     "possible to drop during rollback of that transaction.", e );
                }
                catch ( TransactionFailureException e )
                {
                    throw e;
                }
                catch ( TransactionalException e )
                {
                    throw new IllegalStateException( "The transaction manager could not fulfill the transaction for " +
                                                     "dropping the constraint.", e );
                }
            }
        }
    }

    @Override
    public TxState txState()
    {
        if ( !hasTxState() )
        {
            txState = new TxStateImpl( legacyStateBridge, persistenceManager, null );
        }
        return txState;
    }

    @Override
    public boolean hasTxState()
    {
        return null != txState;
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return legacyStateBridge.hasChanges() || ( hasTxState() && txState.hasChanges() );
    }
}
