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

import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

class CommandCreationLocking
{
    public void acquireRelationshipCreationLock( ReadableTransactionState txState, ResourceLocker locker, LockTracer lockTracer,
            long sourceNode, long targetNode )
    {
        lockGroupAndDegrees( txState, locker, lockTracer, sourceNode, targetNode );
    }

    public void acquireRelationshipDeletionLock( ReadableTransactionState txState, ResourceLocker locker, LockTracer lockTracer,
            long sourceNode, long targetNode, long relationship )
    {
        lockGroupAndDegrees( txState, locker, lockTracer, sourceNode, targetNode );
        locker.acquireExclusive( lockTracer, ResourceTypes.RELATIONSHIP_DELETE, relationship );
    }

    public void acquireNodeDeletionLock( ReadableTransactionState txState, ResourceLocker locker, LockTracer lockTracer, long node )
    {
        if ( !txState.nodeIsAddedInThisTx( node ) )
        {
            locker.acquireExclusive( lockTracer, ResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, node );
            locker.acquireExclusive( lockTracer, ResourceTypes.NODE, node );
            locker.acquireExclusive( lockTracer, ResourceTypes.DEGREES, node );
        }
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
            locker.acquireShared( lockTracer, ResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE, node );
            locker.acquireShared( lockTracer, ResourceTypes.DEGREES, node );
        }
    }
}
