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
import org.neo4j.kernel.api.ConstraintCreationException;
import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.TransactionFailureException;
import org.neo4j.kernel.api.TransactionalException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class StateHandlingTransactionContext extends DelegatingTransactionContext
{
    private final SchemaIndexProviderMap providerMap;
    private final PersistenceCache persistenceCache;
    private final TxState state;
    private final SchemaCache schemaCache;
    private final PersistenceManager persistenceManager;
    private final SchemaStorage schemaStorage;

    private final UpdateableSchemaState schemaState;
    private final ConstraintIndexCreator constraintIndexCreator;

    public StateHandlingTransactionContext( TransactionContext actual,
                                            SchemaStorage schemaStorage,
                                            TxState state,
                                            SchemaIndexProviderMap providerMap,
                                            PersistenceCache persistenceCache,
                                            SchemaCache schemaCache,
                                            PersistenceManager persistenceManager, UpdateableSchemaState schemaState,
                                            ConstraintIndexCreator constraintIndexCreator )
    {
        super( actual );
        this.schemaStorage = schemaStorage;
        this.providerMap = providerMap;
        this.persistenceCache = persistenceCache;
        this.schemaCache = schemaCache;
        this.persistenceManager = persistenceManager;
        this.schemaState = schemaState;
        this.state = state;
        this.constraintIndexCreator = constraintIndexCreator;
    }

    @Override
    public StatementContext newStatementContext()
    {
        // Store stuff
        StatementContext result = super.newStatementContext();
        // + Caching
        result = new CachingStatementContext( result, persistenceCache, schemaCache );
        // + Transaction-local state awareness
        result = new StateHandlingStatementContext( result, new SchemaStateConcern( schemaState ), state,
                                                    constraintIndexCreator );
        // done
        return result;
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
        persistenceCache.apply( state );
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
        final AtomicBoolean clearState = new AtomicBoolean( false );
        state.accept( new TxState.Visitor()
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
                    throw new ConstraintCreationException(e);
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

    private void dropCreatedConstraintIndexes() throws TransactionFailureException
    {
        for ( IndexDescriptor createdConstraintIndex : state.createdConstraintIndexes() )
        {
            try
            {
                constraintIndexCreator.dropUniquenessConstraintIndex( createdConstraintIndex );
            }
            catch ( DataIntegrityKernelException e )
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
