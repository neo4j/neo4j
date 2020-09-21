/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import java.util.function.LongFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.recordstorage.RecordAccess.LoadMonitor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;
import static org.neo4j.lock.ResourceTypes.NODE;
import static org.neo4j.lock.ResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP_GROUP;
import static org.neo4j.util.Preconditions.checkState;

public class LockVerificationMonitor implements LoadMonitor
{
    private final ResourceLocker locks;
    private final ReadableTransactionState txState;
    private final StoreLoader loader;

    LockVerificationMonitor( ResourceLocker locks, ReadableTransactionState txState, StoreLoader loader )
    {
        this.locks = locks;
        this.txState = txState;
        this.loader = loader;
    }

    @Override
    public void markedAsChanged( AbstractBaseRecord before )
    {
        // This is assuming that all before records coming here are inUse, they should really always be when getting a call to this method
        if ( !before.inUse() )
        {
            return; //we can not do anything useful with unused before records
        }

        if ( before instanceof NodeRecord )
        {
            verifyNodeSufficientlyLocked( (NodeRecord) before );
        }
        else if ( before instanceof RelationshipRecord )
        {
            verifyRelationshipSufficientlyLocked( (RelationshipRecord) before );
        }
        else if ( before instanceof RelationshipGroupRecord )
        {
            verifyRelationshipGroupSufficientlyLocked( (RelationshipGroupRecord) before );
        }
    }

    private void verifyNodeSufficientlyLocked( NodeRecord before )
    {
        assertRecordsEquals( before, loader::loadNode );
        long id = before.getId();
        if ( !txState.nodeIsAddedInThisTx( id ) )
        {
            assertLocked( id, NODE, before );
        }
        if ( txState.nodeIsDeletedInThisTx( id ) )
        {
            assertLocked( id, NODE_RELATIONSHIP_GROUP_DELETE, before );
        }
    }

    private void verifyRelationshipSufficientlyLocked( RelationshipRecord before )
    {
        assertRecordsEquals( before, loader::loadRelationship );
        long id = before.getId();
        boolean addedInThisTx = txState.relationshipIsAddedInThisTx( id );
        checkState( before.inUse() == !addedInThisTx, "Relationship[%d] inUse:%b, but txState.relationshipIsAddedInThisTx:%b", id, before.inUse(),
                addedInThisTx );
        checkRelationship( txState, locks, loader, before );
    }

    private void verifyRelationshipGroupSufficientlyLocked( RelationshipGroupRecord before )
    {
        assertRecordsEquals( before, loader::loadRelationshipGroup );

        long node = before.getOwningNode();
        if ( !txState.nodeIsAddedInThisTx( node ) )
        {
            assertLocked( node, RELATIONSHIP_GROUP, before );
        }
    }

    private void assertLocked( long id, ResourceType resource, AbstractBaseRecord record )
    {
        assertLocked( locks, id, resource, EXCLUSIVE, record );
    }

    static void checkRelationship( ReadableTransactionState txState, ResourceLocker locks, StoreLoader loader, RelationshipRecord record )
    {
        long id = record.getId();
        if ( !txState.relationshipIsAddedInThisTx( id ) && !txState.relationshipIsDeletedInThisTx( id ) )
        {
            //relationship only modified
            assertLocked( locks, id, RELATIONSHIP, EXCLUSIVE, record );
        }
        else
        {
            if ( txState.relationshipIsDeletedInThisTx( id ) )
            {
                assertLocked( locks, id, RELATIONSHIP, EXCLUSIVE, record );
            }
            else
            {
                checkRelationshipNode( txState, locks, loader, record.getFirstNode() );
                checkRelationshipNode( txState, locks, loader, record.getSecondNode() );
            }
        }
    }

    private static void checkRelationshipNode( ReadableTransactionState txState, ResourceLocker locks, StoreLoader loader, long nodeId )
    {
        if ( !txState.nodeIsAddedInThisTx( nodeId ) )
        {
            NodeRecord node = loader.loadNode( nodeId );
            if ( node.inUse() && node.isDense() )
            {
                assertLocked( locks, nodeId, NODE_RELATIONSHIP_GROUP_DELETE, SHARED, node );
                checkState( hasLock( locks, nodeId, NODE, EXCLUSIVE ) ||
                        hasLock( locks, nodeId, NODE_RELATIONSHIP_GROUP_DELETE, SHARED ),
                        "%s modified w/ neither [%s,%s] nor [%s,%s]", locks, NODE, EXCLUSIVE, NODE_RELATIONSHIP_GROUP_DELETE, SHARED );
            }
        }
    }

    static void assertLocked( ResourceLocker locks, long id, ResourceType resource, LockType type, AbstractBaseRecord record )
    {
        checkState( hasLock( locks, id, resource, type ), "%s [%s,%s] modified without %s lock, record:%s.", locks, resource, id, type, record );
    }

    private static boolean hasLock( ResourceLocker locks, long id, ResourceType resource, LockType type )
    {
        return locks.activeLocks().anyMatch( lock -> lock.resourceId() == id && lock.resourceType() == resource && lock.lockType() == type );
    }

    static <RECORD extends AbstractBaseRecord> void assertRecordsEquals( RECORD before, LongFunction<RECORD> loader )
    {
        RECORD stored = loader.apply( before.getId() );
        if ( before.inUse() || stored.inUse() )
        {
            checkState( stored.equals( before ),
                    "Record which got marked as changed is not what the store has, i.e. it was read before lock was acquired%nbefore:%s%nstore:%s",
                    before, stored );
        }
    }

    public interface Factory
    {
        RecordAccess.LoadMonitor create( ResourceLocker locks, ReadableTransactionState txState, NeoStores neoStores );

        static Factory defaultFactory( Config config )
        {
            boolean enabled = config.get( GraphDatabaseInternalSettings.additional_lock_verification );
            return enabled ? ( locks, txState, neoStores ) -> new LockVerificationMonitor( locks, txState, new NeoStoresLoader( neoStores ) ) : IGNORE;
        };

        Factory IGNORE = ( locks, txState, neoStores ) -> LoadMonitor.NULL_MONITOR;
    }

    public interface StoreLoader
    {
        NodeRecord loadNode( long id );

        RelationshipRecord loadRelationship( long id );

        RelationshipGroupRecord loadRelationshipGroup( long id );
    }

    public static class NeoStoresLoader implements StoreLoader
    {
        private final NeoStores neoStores;

        public NeoStoresLoader( NeoStores neoStores )
        {
            this.neoStores = neoStores;
        }

        @Override
        public NodeRecord loadNode( long id )
        {
            return readRecord( id, neoStores.getNodeStore() );
        }

        @Override
        public RelationshipRecord loadRelationship( long id )
        {
            return readRecord( id, neoStores.getRelationshipStore() );
        }

        @Override
        public RelationshipGroupRecord loadRelationshipGroup( long id )
        {
            return readRecord( id, neoStores.getRelationshipGroupStore() );
        }

        private <RECORD extends AbstractBaseRecord> RECORD readRecord( long id, RecordStore<RECORD> store )
        {
            return store.getRecord( id, store.newRecord(), RecordLoad.ALWAYS, PageCursorTracer.NULL );
        }
    }
}
