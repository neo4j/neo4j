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

import static org.neo4j.internal.recordstorage.RecordStorageCommandCreationContext.buildLogicalRelationshipGroupResourceId;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;
import static org.neo4j.lock.ResourceTypes.NODE;
import static org.neo4j.lock.ResourceTypes.NODE_DELETE;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.util.Preconditions.checkState;

public class LockVerificationMonitor implements LoadMonitor
{
    private final ResourceLocker locks;
    private final ReadableTransactionState txState;
    private final NeoStores neoStores;

    LockVerificationMonitor( ResourceLocker locks, ReadableTransactionState txState, NeoStores neoStores )
    {
        this.locks = locks;
        this.txState = txState;
        this.neoStores = neoStores;
    }

    @Override
    public void markedAsChanged( Object before )
    {
        // This is assuming that all before records coming here are inUse, they should really always be when getting a call to this method
        assert ((AbstractBaseRecord) before).inUse();

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
        assertRecordsEquals( before, neoStores.getNodeStore() );
        long id = before.getId();
        if ( !txState.nodeIsAddedInThisTx( id ) )
        {
            assertLocked( id, NODE, EXCLUSIVE );
        }
        if ( txState.nodeIsDeletedInThisTx( id ) )
        {
            assertLocked( id, NODE_DELETE, EXCLUSIVE );
        }
    }

    private void verifyRelationshipSufficientlyLocked( RelationshipRecord before )
    {
        assertRecordsEquals( before, neoStores.getRelationshipStore() );
        long id = before.getId();
        boolean addedInThisTx = txState.relationshipIsAddedInThisTx( id );
        checkState( before.inUse() == !addedInThisTx, "Relationship[%d] inUse:%b, but txState.relationshipIsAddedInThisTx:%b", id, before.inUse(),
                addedInThisTx );
        if ( !addedInThisTx )
        {
            assertLocked( id, RELATIONSHIP, EXCLUSIVE );
        }

        long firstNode = before.getFirstNode();
        long secondNode = before.getSecondNode();
        int type = before.getType();
        if ( !txState.nodeIsAddedInThisTx( firstNode ) )
        {
            NodeRecord first = readRecord( firstNode, neoStores.getNodeStore() );
            if ( first.inUse() && first.isDense() )
            {
                assertLocked( buildLogicalRelationshipGroupResourceId( firstNode, type ), RELATIONSHIP_GROUP_DELETE, SHARED );
                assertLocked( firstNode, NODE_DELETE, SHARED );
            }
        }

        if ( !txState.nodeIsAddedInThisTx( secondNode ) )
        {
            NodeRecord second = readRecord( secondNode, neoStores.getNodeStore() );
            if ( second.inUse() && second.isDense() )
            {
                assertLocked( buildLogicalRelationshipGroupResourceId( secondNode, type ), RELATIONSHIP_GROUP_DELETE, SHARED );
                assertLocked( secondNode, NODE_DELETE, SHARED );
            }
        }
    }

    private void verifyRelationshipGroupSufficientlyLocked( RelationshipGroupRecord before )
    {
        assertRecordsEquals( before, neoStores.getRelationshipGroupStore() );

        long node = before.getOwningNode();
        if ( !txState.nodeIsAddedInThisTx( node ) )
        {
            assertLocked( node, NODE, EXCLUSIVE );
        }
    }

    private void assertLocked( long id, ResourceType resource, LockType type )
    {
        assertLocked( locks, id, resource, type );
    }

    static void assertLocked( ResourceLocker locks, long id, ResourceType resource, LockType type )
    {
        if ( locks.activeLocks().noneMatch(
                lock -> lock.resourceId() == id && lock.resourceType() == resource && lock.lockType() == type ) )
        {
            throw new IllegalStateException( String.format( "[%s,%s] modified without %s lock.", resource, id, type ) );
        }
    }

    static <RECORD extends AbstractBaseRecord> void assertRecordsEquals( RECORD before, RecordStore<RECORD> store )
    {
        RECORD readRecord = readRecord( before.getId(), store );
        checkState( readRecord.equals( before ),
                "Record which got marked as changed is not what the store has, i.e. it was read before lock was acquired%before:  %s%nstore: %s", before,
                readRecord );
    }

    static <RECORD extends AbstractBaseRecord> RECORD readRecord( long id, RecordStore<RECORD> store )
    {
        return store.getRecord( id, store.newRecord(), RecordLoad.ALWAYS, PageCursorTracer.NULL );
    }

    public interface Factory
    {
        RecordAccess.LoadMonitor create( ResourceLocker locks, ReadableTransactionState txState, NeoStores neoStores );

        static Factory defaultFactory()
        {
            boolean test = false;
            assert test = true;
            return test ? ( locks, txState, neoStores ) -> new LockVerificationMonitor( locks, txState, neoStores ) : IGNORE;
        };

        Factory IGNORE = ( locks, txState, neoStores ) -> LoadMonitor.NULL_MONITOR;
    }
}
