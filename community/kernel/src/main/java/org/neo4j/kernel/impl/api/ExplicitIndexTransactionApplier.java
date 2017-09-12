/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.graphdb.index.IndexManager.PROVIDER;

/**
 * This class caches the appliers for different {@link IndexCommand}s for performance reasons. These appliers are then
 * closed on the batch level in {@link ExplicitBatchIndexApplier#close()}, together with the last transaction in the
 * batch.
 */
public class ExplicitIndexTransactionApplier extends TransactionApplier.Adapter
{

    // We have these two maps here for "applier lookup" performance reasons. Every command that we apply we must
    // redirect to the correct applier, i.e. the _single_ applier for the provider managing the specific index.
    // Looking up provider for an index has a certain cost so those are cached in applierByIndex.
    private Map<String/*indexName*/,TransactionApplier> applierByNodeIndex = Collections.emptyMap();
    private Map<String/*indexName*/,TransactionApplier> applierByRelationshipIndex = Collections.emptyMap();
    Map<String/*providerName*/,TransactionApplier> applierByProvider = Collections.emptyMap();

    private final ExplicitIndexApplierLookup applierLookup;
    private final IndexConfigStore indexConfigStore;
    private final TransactionApplicationMode mode;
    private final IdOrderingQueue transactionOrdering;
    private IndexDefineCommand defineCommand;
    private long transactionId = -1;

    public ExplicitIndexTransactionApplier( ExplicitIndexApplierLookup applierLookup,
            IndexConfigStore indexConfigStore, TransactionApplicationMode mode, IdOrderingQueue transactionOrdering )
    {
        this.applierLookup = applierLookup;
        this.indexConfigStore = indexConfigStore;
        this.mode = mode;
        this.transactionOrdering = transactionOrdering;
    }

    /**
     * Ability to set transaction id allows the applier instance to be cached.
     * @param txId the currently active TransactionId
     */
    void setTransactionId( long txId )
    {
        this.transactionId = txId;
    }

    /**
     * Get an applier suitable for the specified IndexCommand.
     */
    private TransactionApplier applier( IndexCommand command ) throws IOException
    {
        // Have we got an applier for this index?
        String indexName = defineCommand.getIndexName( command.getIndexNameId() );
        Map<String,TransactionApplier> applierByIndex = applierByIndexMap( command );
        TransactionApplier applier = applierByIndex.get( indexName );
        if ( applier == null )
        {
            // We don't. Have we got an applier for the provider of this index?
            IndexEntityType entityType = IndexEntityType.byId( command.getEntityType() );
            Map<String,String> config = indexConfigStore.get( entityType.entityClass(), indexName );
            if ( config == null )
            {
                // This provider doesn't even exist, return an EMPTY handler, i.e. ignore these changes.
                // Could be that the index provider is temporarily unavailable?
                return TransactionApplier.EMPTY;
            }
            String providerName = config.get( PROVIDER );
            applier = applierByProvider.get( providerName );
            if ( applier == null )
            {
                // We don't, so create the applier
                applier = applierLookup.newApplier( providerName, mode.needsIdempotencyChecks() );
                applier.visitIndexDefineCommand( defineCommand );
                applierByProvider.put( providerName, applier );
            }

            // Also cache this applier for this index
            applierByIndex.put( indexName, applier );
        }
        return applier;
    }

    // Some lazy creation of Maps for holding appliers per provider and index
    private Map<String,TransactionApplier> applierByIndexMap( IndexCommand command )
    {
        if ( command.getEntityType() == IndexEntityType.Node.id() )
        {
            if ( applierByNodeIndex.isEmpty() )
            {
                applierByNodeIndex = new HashMap<>();
                lazyCreateApplierByprovider();
            }
            return applierByNodeIndex;
        }
        if ( command.getEntityType() == IndexEntityType.Relationship.id() )
        {
            if ( applierByRelationshipIndex.isEmpty() )
            {
                applierByRelationshipIndex = new HashMap<>();
                lazyCreateApplierByprovider();
            }
            return applierByRelationshipIndex;
        }
        throw new UnsupportedOperationException( "Unknown entity type " + command.getEntityType() );
    }

    private void lazyCreateApplierByprovider()
    {
        if ( applierByProvider.isEmpty() )
        {
            applierByProvider = new HashMap<>();
        }
    }

    @Override
    public void close()
    {
        // Let other transactions in same batch run
        // Last transaction notifies on the batch level, to let appliers close before-hand.
        // Internal appliers are closed on the batch level (ExplicitIndexBatchApplier)
        notifyExplicitIndexOperationQueue();

    }

    private void notifyExplicitIndexOperationQueue()
    {
        if ( transactionId != -1 )
        {
            transactionOrdering.removeChecked( transactionId );
            transactionId = -1;
        }
    }

    @Override
    public boolean visitIndexAddNodeCommand( IndexCommand.AddNodeCommand command ) throws IOException
    {
        return applier( command ).visitIndexAddNodeCommand( command );
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( IndexCommand.AddRelationshipCommand command ) throws IOException
    {
        return applier( command ).visitIndexAddRelationshipCommand( command );
    }

    @Override
    public boolean visitIndexRemoveCommand( IndexCommand.RemoveCommand command ) throws IOException
    {
        return applier( command ).visitIndexRemoveCommand( command );
    }

    @Override
    public boolean visitIndexDeleteCommand( IndexCommand.DeleteCommand command ) throws IOException
    {
        return applier( command ).visitIndexDeleteCommand( command );
    }

    @Override
    public boolean visitIndexCreateCommand( IndexCommand.CreateCommand command ) throws IOException
    {
        indexConfigStore.setIfNecessary( IndexEntityType.byId( command.getEntityType() ).entityClass(),
                defineCommand.getIndexName( command.getIndexNameId() ), command.getConfig() );
        return applier( command ).visitIndexCreateCommand( command );
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        this.defineCommand = command;
        forward( command, applierByNodeIndex );
        forward( command, applierByRelationshipIndex );
        forward( command, applierByProvider );
        return false;
    }

    private void forward( IndexDefineCommand definitions, Map<String,TransactionApplier> appliers ) throws IOException
    {
        for ( CommandVisitor applier : appliers.values() )
        {
            applier.visitIndexDefineCommand( definitions );
        }
    }
}
