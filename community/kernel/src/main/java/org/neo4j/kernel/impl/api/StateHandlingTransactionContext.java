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
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.TransactionFailureException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
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

    public StateHandlingTransactionContext( TransactionContext actual,
                                            SchemaStorage schemaStorage,
                                            TxState state,
                                            SchemaIndexProviderMap providerMap,
                                            PersistenceCache persistenceCache,
                                            SchemaCache schemaCache,
                                            PersistenceManager persistenceManager, UpdateableSchemaState schemaState )
    {
        super( actual );
        this.schemaStorage = schemaStorage;
        this.providerMap = providerMap;
        this.persistenceCache = persistenceCache;
        this.schemaCache = schemaCache;
        this.persistenceManager = persistenceManager;
        this.schemaState = schemaState;
        this.state = state;
    }

    @Override
    public StatementContext newStatementContext()
    {
        // Store stuff
        StatementContext result = super.newStatementContext();
        // + Caching
        result = new CachingStatementContext( result, persistenceCache, schemaCache );
        // + Transaction-local state awareness
        result = new StateHandlingStatementContext( result, new SchemaStateConcern( schemaState ), state );
        // done
        return result;
    }

    @Override
    public void commit() throws TransactionFailureException
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
            public void visitAddedIndex( IndexDescriptor element )
            {
                SchemaIndexProvider.Descriptor providerDescriptor = providerMap.getDefaultProvider().getProviderDescriptor();
                IndexRule rule = new IndexRule( schemaStorage.newRuleId(), element.getLabelId(), providerDescriptor,
                                                element.getPropertyKeyId() );
                persistenceManager.createSchemaRule( rule );
            }

            @Override
            public void visitRemovedIndex( IndexDescriptor element )
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
            public void visitAddedConstraint( UniquenessConstraint element )
            {
                clearState.set( true );
                persistenceManager.createSchemaRule( new UniquenessConstraintRule(
                        schemaStorage.newRuleId(), element.label(), element.property() ) );
            }

            @Override
            public void visitRemovedConstraint( UniquenessConstraint element )
            {
                try
                {
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
            }
        } );
        if ( clearState.get() )
        {
            schemaState.clear();
        }
        // - Ensure transaction is committed to disk at this point
        super.commit();

        // - commit changes from tx state to the cache
        // TODO: This should be done by log application, not by this level of the stack.
        persistenceCache.apply( state );
    }
}
