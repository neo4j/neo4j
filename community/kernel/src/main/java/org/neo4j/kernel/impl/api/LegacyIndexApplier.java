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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

import static org.neo4j.graphdb.index.IndexManager.PROVIDER;

public class LegacyIndexApplier extends CommandHandler.Adapter
{
    private final LegacyIndexApplierLookup applierLookup;

    // We have these two maps here for "applier lookup" performance reasons. Every command that we apply we must
    // redirect to the correct applier, i.e. the _single_ applier for the provider managing the specific index.
    // Looking up provider for an index has a certain cost so those are cached in applierByIndex.
    private Map<String/*indexName*/,CommandHandler> applierByNodeIndex = Collections.emptyMap();
    private Map<String/*indexName*/,CommandHandler> applierByRelationshipIndex = Collections.emptyMap();
    private Map<String/*providerName*/,CommandHandler> applierByProvider = Collections.emptyMap();

    private final IndexConfigStore indexConfigStore;
    private final IdOrderingQueue transactionOrdering;
    private final TransactionApplicationMode mode;
    private IndexDefineCommand defineCommand;
    private long activeTransactionId = -1;
    private boolean isLastTransactionInBatch = false;

    public LegacyIndexApplier( IndexConfigStore indexConfigStore, LegacyIndexApplierLookup applierLookup,
            IdOrderingQueue transactionOrdering, TransactionApplicationMode mode )
    {
        this.indexConfigStore = indexConfigStore;
        this.applierLookup = applierLookup;
        this.transactionOrdering = transactionOrdering;
        this.mode = mode;
    }

    private CommandHandler applier( IndexCommand command ) throws IOException
    {
        // Have we got an applier for this index?
        String indexName = defineCommand.getIndexName( command.getIndexNameId() );
        Map<String,CommandHandler> applierByIndex = applierByIndexMap( command );
        CommandHandler applier = applierByIndex.get( indexName );
        if ( applier == null )
        {
            // We don't. Have we got an applier for the provider of this index?
            IndexEntityType entityType = IndexEntityType.byId( command.getEntityType() );
            Map<String,String> config = indexConfigStore.get( entityType.entityClass(), indexName );
            if ( config == null )
            {
                // This provider doesn't even exist, return an EMPTY handler, i.e. ignore these changes.
                // Could be that the index provider is temporarily unavailable?
                return CommandHandler.EMPTY;
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
    private Map<String,CommandHandler> applierByIndexMap( IndexCommand command )
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
    public void begin( TransactionToApply transaction, LockGroup locks ) throws IOException
    {
        if ( transaction.commitment().hasLegacyIndexChanges() )
        {
            activeTransactionId = transaction.transactionId();
            try
            {
                transactionOrdering.waitFor( activeTransactionId );
                // Need to know if this is the last transaction in this batch of legacy index changes in order to
                // run apply before other batches are allowed to run, in order to preserve ordering.
                if ( transaction.next() == null )
                {
                    isLastTransactionInBatch = true;
                }
            }
            catch ( InterruptedException e )
            {
                throw new IOException( "Interrupted while waiting for applying tx:" + activeTransactionId +
                        " legacy index updates", e );
            }
        }
    }

    @Override
    public void end()
    {
        if ( !isLastTransactionInBatch )
        {
            // Let other transactions in same batch run
            notifyLegacyIndexOperationQueue();
        }
    }

    @Override
    public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
    {
        return applier( command ).visitIndexAddNodeCommand( command );
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
    {
        return applier( command ).visitIndexAddRelationshipCommand( command );
    }

    @Override
    public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
    {
        return applier( command ).visitIndexRemoveCommand( command );
    }

    @Override
    public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
    {
        return applier( command ).visitIndexDeleteCommand( command );
    }

    @Override
    public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
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

    private void forward( IndexDefineCommand definitions, Map<String,CommandHandler> appliers ) throws IOException
    {
        for ( CommandHandler applier : appliers.values() )
        {
            applier.visitIndexDefineCommand( definitions );
        }
    }

    @Override
    public void apply()
    {
        for ( CommandHandler applier : applierByProvider.values() )
        {
            applier.apply();
        }
    }

    @Override
    public void close()
    {
        for ( CommandHandler applier : applierByProvider.values() )
        {
            applier.close();
        }
        if ( isLastTransactionInBatch )
        {
            // Let other batches run
            notifyLegacyIndexOperationQueue();
        }
    }

    private void notifyLegacyIndexOperationQueue()
    {
        if ( activeTransactionId != -1 )
        {
            transactionOrdering.removeChecked( activeTransactionId );
            activeTransactionId = -1;
        }
    }
}
