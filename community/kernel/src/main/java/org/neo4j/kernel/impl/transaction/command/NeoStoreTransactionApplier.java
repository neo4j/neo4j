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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;

import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;

/**
 * Visits commands targeted towards the {@link NeoStore} and update corresponding stores.
 * What happens in here is what will happen in a "internal" transaction, i.e. a transaction that has been
 * forged in this database, with transaction state, a KernelTransaction and all that and is now committing.
 * <p>
 * For other modes of application, like recovery or external there are other, added functionality, decorated
 * outside this applier.
 */
public class NeoStoreTransactionApplier extends NeoCommandHandler.Adapter
{
    private final NeoStore neoStore;
    // Ideally we don't want any cache access in here, but it is how it is. At least we try to minimize use of it
    private final CacheAccessBackDoor cacheAccess;
    private final LockService lockService;
    private final LockGroup lockGroup;
    private final long transactionId;
    private RelationshipHoles relationshipHoles;

    public NeoStoreTransactionApplier( NeoStore store, CacheAccessBackDoor cacheAccess,
                                       LockService lockService, LockGroup lockGroup, long transactionId )
    {
        this.neoStore = store;
        this.cacheAccess = cacheAccess;
        this.lockService = lockService;
        this.transactionId = transactionId;
        this.lockGroup = lockGroup;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        // acquire lock
        lockGroup.add( lockService.acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK ) );

        // update store
        NodeStore nodeStore = neoStore.getNodeStore();
        nodeStore.updateRecord( command.getAfter() );
        // getDynamicLabelRecords will contain even deleted records
        nodeStore.updateDynamicLabelRecords( command.getAfter().getDynamicLabelRecords() );

        // Ideally we don't any cache invalidation in this "normal" transaction case, but upgraded dense
        // nodes must me evicted here as well.
        if ( nodeHasBeenUpgradedToDense( command ) )
        {
            cacheAccess.removeNodeFromCache( command.getKey() );
        }
        return false;
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
    {
        RelationshipRecord record = command.getRecord();
        neoStore.getRelationshipStore().updateRecord( record );

        /*
         * The pointers in record are like they were before this command was executed UNLESS this is the second
         * time this command is executed (think recovery), where it might have been actually written out to disk
         * so the fields are already -1. So we still need to check.
         * We don't check for !inUse() though because that is implicit in the call of this method.
         * The above is a hand waiving proof that the conditions that lead to the patchDeletedRelationshipNodes()
         * in the if below are the same as in RelationshipCommand.execute() so it should be safe.
         */
        boolean relationshipHasBeenDeletedButNotPreviouslyAppliedToStore =
                !record.inUse() && (record.getFirstNode() != -1 || record.getSecondNode() != -1);
        if ( relationshipHasBeenDeletedButNotPreviouslyAppliedToStore )
        {   // ... then we can use the fields in that record to patch the cache
            if ( relationshipHoles == null )
            {
                relationshipHoles = new RelationshipHoles();
            }
            relationshipHoles.deleted( record );
        }
        return false;
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
        // update store
        neoStore.getPropertyStore().updateRecord( command.getAfter() );
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
    {
        neoStore.getRelationshipGroupStore().updateRecord( command.getRecord() );
        return false;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException
    {
        neoStore.getRelationshipTypeTokenStore().updateRecord( command.getRecord() );
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
    {
        neoStore.getLabelTokenStore().updateRecord( command.getRecord() );
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
    {
        neoStore.getPropertyKeyTokenStore().updateRecord( command.getRecord() );
        return false;
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
            schemaStore.updateRecord( record );
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
            cacheAccess.removeSchemaRuleFromCache( command.getKey() );
            break;
        default:
            cacheAccess.addSchemaRule( command.getSchemaRule() );
        }
        return false;
    }

    @Override
    public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
    {
        neoStore.setGraphNextProp( command.getRecord().getNextProp() );
        return false;
    }

    @Override
    public void apply()
    {
        if ( relationshipHoles != null )
        {
            relationshipHoles.apply( cacheAccess );
        }
    }

    private boolean nodeHasBeenUpgradedToDense( NodeCommand command )
    {
        final NodeRecord before = command.getBefore();
        final NodeRecord after = command.getAfter();

        return before.inUse() && !before.isDense() &&
               after.inUse() && after.isDense();
    }
}
