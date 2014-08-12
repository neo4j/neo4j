/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier.HighIdTrackerFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.Mode;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;

public class NeoTransactionStoreApplier extends NeoCommandHandler.Adapter
{
    private final NeoStore neoStore;
    private final IndexingService indexes;
    private final boolean recovery;
    private final CacheAccessBackDoor cacheAccess;
    private final LockService lockService;
    private final LockGroup lockGroup;
    private final long transactionId;
    private final HighIdTracker highIdTracker;
    
    public NeoTransactionStoreApplier( NeoStore store, IndexingService indexes, CacheAccessBackDoor cacheAccess,
            LockService lockService, long transactionId, HighIdTrackerFactory highIdTrackerFactory, boolean recovery )
    {
        this.cacheAccess = cacheAccess;
        this.lockService = lockService;
        this.transactionId = transactionId;
        this.highIdTracker = highIdTrackerFactory.create( recovery );
        this.recovery = recovery;
        this.neoStore = store;
        this.indexes = indexes;
        this.lockGroup = new LockGroup();
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        // acquire lock
        lockGroup.add( lockService.acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK ) );

        // update store
        NodeStore nodeStore = neoStore.getNodeStore();
        highIdTracker.track( nodeStore, command.getAfter().getId() );
        nodeStore.updateRecord( command.getAfter() );

        // Dynamic Label Records
        Collection<DynamicRecord> toUpdate = new ArrayList<>( command.getAfter().getDynamicLabelRecords() );
        // the dynamic label records that exist in before, but not in after should be deleted.
        Set<Long> idsToRemove = new HashSet<>();
        for ( DynamicRecord record : command.getBefore().getDynamicLabelRecords() )
        {
            idsToRemove.add( record.getId() );
        }
        for ( DynamicRecord record : command.getAfter().getDynamicLabelRecords() )
        {
            idsToRemove.remove( record.getId() );
        }
        for ( long id : idsToRemove )
        {
            toUpdate.add( new DynamicRecord( id ) );
        }

        trackHighIds( nodeStore.getDynamicLabelStore(), toUpdate );
        nodeStore.updateDynamicLabelRecords( toUpdate );

        // Additional cache invalidation check for nodes that have just been upgraded to dense
        invalidateCache( command, nodeHasBeenUpgradedToDense( command ) );
        return true;
    }

    private boolean nodeHasBeenUpgradedToDense( NodeCommand command )
    {
        final NodeRecord before = command.getBefore();
        final NodeRecord after = command.getAfter();

        return before.inUse() && !before.isDense() &&
                after.inUse() && after.isDense();
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
    {
        RelationshipRecord record = command.getRecord();

        RelationshipStore relationshipStore = neoStore.getRelationshipStore();
        if ( recovery && !record.inUse() )
        {
            /*
             * If read from a log (either on recovery or HA) then all the fields but for the Id are -1. If the
             * record is deleted, then we'll need to invalidate the cache and patch the node's relationship chains.
             * Therefore, we need to read the record from the store. This is not too expensive, since the window
             * will be either in memory or will soon be anyway and we are just saving the write the trouble.
             */
             command.setBefore( relationshipStore.forceGetRaw( record.getId() ) );
        }
        highIdTracker.track( relationshipStore, record.getLongId() );
        relationshipStore.updateRecord( record );
        invalidateCache( command );
        return true;
    }

    @Override
    public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
    {
        // acquire lock
        long nodeId = command.getNodeId();
        if ( nodeId != -1 )
        {
            lockGroup.add( lockService.acquireNodeLock( nodeId, LockService.LockType.WRITE_LOCK ) );
        }

        // track the dynamic value record high ids
        PropertyStore propertyStore = neoStore.getPropertyStore();
        highIdTracker.track( propertyStore, command.getAfter().getId() );
        if ( recovery )
        {
            for ( PropertyBlock block : command.getAfter().getPropertyBlocks() )
            {
                switch ( block.getType() )
                {
                case STRING:
                    trackHighIds( propertyStore.getStringStore(), block.getValueRecords() );
                    break;
                case ARRAY:
                    trackHighIds( propertyStore.getArrayStore(), block.getValueRecords() );
                    break;
                default:
                    // Not needed, no dynamic records then
                    break;
                }
            }
        }

        // update store
        propertyStore.updateRecord( command.getAfter() );
        invalidateCache( command );
        return true;
    }

    private void trackHighIds( CommonAbstractStore store, Collection<? extends Abstract64BitRecord> records )
    {
        // A slight optimization where we know that we only need to track things if we're doing recovery
        if ( recovery )
        {
            for ( Abstract64BitRecord record : records )
            {
                highIdTracker.track( store, record.getId() );
            }
        }
    }

    @Override
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
    {
    	RelationshipGroupStore relationshipGroupStore = neoStore.getRelationshipGroupStore();
    	highIdTracker.track( relationshipGroupStore, command.getRecord().getId() );
        relationshipGroupStore.updateRecord( command.getRecord() );
        invalidateCache( command );
        return true;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException
    {
    	RelationshipTypeTokenStore relationshipTypeTokenStore = neoStore.getRelationshipTypeTokenStore();
    	highIdTracker.track( relationshipTypeTokenStore, command.getRecord().getId() );
    	trackHighIds( relationshipTypeTokenStore.getNameStore(), command.getRecord().getNameRecords() );
        relationshipTypeTokenStore.updateRecord( command.getRecord() );
        if ( recovery )
        {
            Token type = relationshipTypeTokenStore.getToken( (int) command.getKey() );
            cacheAccess.addRelationshipTypeToken( type );
        }
        invalidateCache( command );
        return true;
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
    {
    	LabelTokenStore labelTokenStore = neoStore.getLabelTokenStore();
    	highIdTracker.track( labelTokenStore, command.getRecord().getId() );
    	trackHighIds( labelTokenStore.getNameStore(), command.getRecord().getNameRecords() );
        labelTokenStore.updateRecord( command.getRecord() );
        if ( recovery )
        {
            Token labelId = labelTokenStore.getToken( (int) command.getKey() );
            cacheAccess.addLabelToken( labelId );
        }
        invalidateCache( command );
        return true;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
    {
    	PropertyKeyTokenStore propertyKeyTokenStore = neoStore.getPropertyKeyTokenStore();
    	highIdTracker.track( propertyKeyTokenStore, command.getRecord().getId() );
    	trackHighIds( propertyKeyTokenStore.getNameStore(), command.getRecord().getNameRecords() );
        propertyKeyTokenStore.updateRecord( command.getRecord() );
        if ( recovery )
        {
            Token index = propertyKeyTokenStore.getToken( (int) command.getKey() );
            cacheAccess.addPropertyKeyToken( index );
        }
        invalidateCache( command );
        return true;
    }

    @Override
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
    {
        // schema rules. Execute these after generating the property updates so. If executed
        // before and we've got a transaction that sets properties/labels as well as creating an index
        // we might end up with this corner-case:
        // 1) index rule created and index population job started
        // 2) index population job processes some nodes, but doesn't complete
        // 3) we gather up property updates and send those to the indexes. The newly created population
        //    job might get those as updates
        // 4) the population job will apply those updates as added properties, and might end up with duplicate
        //    entries for the same property

        SchemaStore schemaStore = neoStore.getSchemaStore();
        for ( DynamicRecord record : command.getRecordsAfter() )
        {
            highIdTracker.track( schemaStore, record.getId() );
        	schemaStore.updateRecord( record );
        }

        if ( command.getSchemaRule() instanceof IndexRule )
        {
            switch ( command.getMode() )
            {
                case UPDATE:
                    // Shouldn't we be more clear about that we are waiting for an index to come online here?
                    // right now we just assume that an update to index records means wait for it to be online.
                    if ( ((IndexRule) command.getSchemaRule()).isConstraintIndex() )
                    {
                        try
                        {
                            indexes.activateIndex( command.getSchemaRule().getId() );
                        }
                        catch ( IndexNotFoundKernelException | IndexActivationFailedKernelException |
                                IndexPopulationFailedKernelException e )
                        {
                            throw new IllegalStateException( "Unable to enable constraint, backing index is not online.", e );
                        }
                    }
                    break;
                case CREATE:
                    indexes.createIndex( (IndexRule) command.getSchemaRule() );
                    break;
                case DELETE:
                    indexes.dropIndex( (IndexRule) command.getSchemaRule() );
                    break;
                default:
                    throw new IllegalStateException( command.getMode().name() );
            }
        }

        if ( command.getSchemaRule() instanceof UniquenessConstraintRule )
        {
            switch ( command.getMode() )
            {
                case UPDATE:
                case CREATE:
                    neoStore.setLatestConstraintIntroducingTx( transactionId );
                    break;
                case DELETE:
                    break;
                default:
                    throw new IllegalStateException( command.getMode().name() );
            }
        }

        switch ( command.getMode() )
        {
            case DELETE:
            	invalidateCache( command );
                break;
            default:
                cacheAccess.addSchemaRule( command.getSchemaRule() );
        }
        return true;
    }

    @Override
    public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
    {
        neoStore.setGraphNextProp( command.getRecord().getNextProp() );
        if ( recovery )
        {
        	cacheAccess.removeGraphPropertiesFromCache();
        }
        invalidateCache( command );
        return true;
    }

    private void invalidateCache( Command command ) {
        invalidateCache( command, false );
    }

    private void invalidateCache( Command command, boolean force )
    {
        if ( force || recovery || command.getMode() == Mode.DELETE )
        {
            command.invalidateCache( cacheAccess );
        }
    }
    
    @Override
    public void apply()
    {
        highIdTracker.apply();
    }

    @Override
    public void close()
    {
        lockGroup.close();
    }
}
