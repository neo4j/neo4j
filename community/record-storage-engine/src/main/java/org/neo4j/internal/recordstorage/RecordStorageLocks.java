/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

class RecordStorageLocks implements StorageLocks
{
    private final ResourceLocker locker;

    RecordStorageLocks( ResourceLocker locker )
    {
        this.locker = locker;
    }

    @Override
    public void acquireExclusiveNodeLock( LockTracer lockTracer, long... ids )
    {
        locker.acquireExclusive( lockTracer, RecordResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, ids );
        locker.acquireExclusive( lockTracer, ResourceTypes.NODE, ids );
    }

    @Override
    public void releaseExclusiveNodeLock( long... ids )
    {
        locker.releaseExclusive( ResourceTypes.NODE, ids );
        locker.releaseExclusive( RecordResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, ids );
    }

    @Override
    public void acquireSharedNodeLock( LockTracer lockTracer, long... ids )
    {
        locker.acquireShared( lockTracer, RecordResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, ids );
        locker.acquireShared( lockTracer, ResourceTypes.NODE, ids );
    }

    @Override
    public void releaseSharedNodeLock( long... ids )
    {
        locker.releaseShared( ResourceTypes.NODE, ids );
        locker.releaseShared( RecordResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, ids );
    }

    @Override
    public void acquireExclusiveRelationshipLock( LockTracer lockTracer, long... ids )
    {
        locker.acquireExclusive( lockTracer, RecordResourceTypes.RELATIONSHIP_DELETE, ids );
        locker.acquireExclusive( lockTracer, ResourceTypes.RELATIONSHIP, ids );
    }

    @Override
    public void releaseExclusiveRelationshipLock( long... ids )
    {
        locker.releaseExclusive( ResourceTypes.RELATIONSHIP, ids );
        locker.releaseExclusive( RecordResourceTypes.RELATIONSHIP_DELETE, ids );
    }

    @Override
    public void acquireSharedRelationshipLock( LockTracer lockTracer, long... ids )
    {
        locker.acquireShared( lockTracer, RecordResourceTypes.RELATIONSHIP_DELETE, ids );
        locker.acquireShared( lockTracer, ResourceTypes.RELATIONSHIP, ids );
    }

    @Override
    public void releaseSharedRelationshipLock( long... ids )
    {
        locker.releaseShared( ResourceTypes.RELATIONSHIP, ids );
        locker.releaseShared( RecordResourceTypes.RELATIONSHIP_DELETE, ids );
    }

    @Override
    public void acquireRelationshipCreationLock( ReadableTransactionState txState, LockTracer lockTracer, long sourceNode, long targetNode )
    {
        lockGroupAndDegrees( txState, locker, lockTracer, sourceNode, targetNode );
    }

    @Override
    public void acquireRelationshipDeletionLock( ReadableTransactionState txState, LockTracer lockTracer, long sourceNode, long targetNode, long relationship )
    {
        lockGroupAndDegrees( txState, locker, lockTracer, sourceNode, targetNode );
        locker.acquireExclusive( lockTracer, RecordResourceTypes.RELATIONSHIP_DELETE, relationship );
    }

    @Override
    public void acquireNodeDeletionLock( ReadableTransactionState txState, LockTracer lockTracer, long node )
    {
        if ( !txState.nodeIsAddedInThisTx( node ) )
        {
            locker.acquireExclusive( lockTracer, RecordResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, node );
            locker.acquireExclusive( lockTracer, ResourceTypes.NODE, node );
            locker.acquireExclusive( lockTracer, RecordResourceTypes.DEGREES, node );
            //Note. We also take RELATIONSHIP_GROUP lock in TransactionRecordState.nodeDelete if we need to clean up any left over empty groups.
        }
    }

    @Override
    public void acquireNodeLabelChangeLock( LockTracer lockTracer, long node, int labelId )
    {
        // This is done for e.g. NODE ADD/REMOVE label where we need a stable degree to tell the counts store. Acquiring this lock
        // will prevent any RELATIONSHIP CREATE/DELETE to happen on this node until this transaction is committed.
        // What would happen w/o this lock (simplest scenario):
        // T1: Start TX, add label L on node N, which currently has got 5 relationships
        // T2: Start TX, create another relationship on N
        // T1: Go into commit where commands are created and for the added label the relationship degrees are read and placed in a counts command
        // T2: Go into commit and fully complete commit, i.e. before T1. N now has 6 relationships
        // T1: Complete the commit
        // --> N has 6 relationships, but counts store says that it has 5
        locker.acquireExclusive( lockTracer, RecordResourceTypes.DEGREES, node );
    }

    private static void lockGroupAndDegrees( ReadableTransactionState txState, ResourceLocker locker, LockTracer lockTracer, long sourceNode, long targetNode )
    {
        lockGroupAndDegrees( txState, locker, lockTracer, Math.min( sourceNode, targetNode ) );
        if ( sourceNode != targetNode )
        {
            lockGroupAndDegrees( txState, locker, lockTracer, Math.max( sourceNode, targetNode ) );
        }
    }

    private static void lockGroupAndDegrees( ReadableTransactionState txState, ResourceLocker locker, LockTracer lockTracer, long node )
    {
        if ( !txState.nodeIsAddedInThisTx( node ) )
        {
            locker.acquireShared( lockTracer, RecordResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, node );
            locker.acquireShared( lockTracer, RecordResourceTypes.DEGREES, node );
        }
    }
}
