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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

class TwoPhaseNodeForRelationshipLocking
{
    private final PrimitiveLongSet nodeIds = Primitive.longSet();
    private final EntityReadOperations entityReadOperations;
    private final ThrowingConsumer<Long,KernelException> relIdAction;

    private long firstRelId;

    TwoPhaseNodeForRelationshipLocking( EntityReadOperations entityReadOperations,
            ThrowingConsumer<Long,KernelException> relIdAction )
    {
        this.entityReadOperations = entityReadOperations;
        this.relIdAction = relIdAction;
    }

    void lockAllNodesAndConsumeRelationships( long nodeId, final KernelStatement state ) throws KernelException
    {
        boolean retry;
        do
        {
            nodeIds.add( nodeId );
            retry = false;
            firstRelId = -1;

            // lock all the nodes involved by following the node id ordering
            try ( Cursor<NodeItem> node = entityReadOperations.nodeCursorById( state, nodeId ) )
            {
                entityReadOperations.nodeGetRelationships( state, node.get(), Direction.BOTH )
                        .forAll( this::collectNodeId );
            }

            lockAllNodes( state );

            // perform the action on each relationship, we will retry if the the relationship iterator contains new relationships
            try ( Cursor<NodeItem> node = entityReadOperations.nodeCursorById( state, nodeId ) )
            {
                try ( Cursor<RelationshipItem> relationships = entityReadOperations
                        .nodeGetRelationships( state, node.get(), Direction.BOTH ) )
                {
                    boolean first = true;
                    while ( relationships.next() && !retry )
                    {
                        retry = performAction( state, relationships.get(), first );
                        first = false;
                    }
                }
            }
        }
        while ( retry );
    }

    private void lockAllNodes( KernelStatement state )
    {
        PrimitiveLongIterator nodeIdIterator = nodeIds.iterator();
        while ( nodeIdIterator.hasNext() )
        {
            state.locks().optimistic()
                    .acquireExclusive( state.lockTracer(), ResourceTypes.NODE, nodeIdIterator.next() );
        }
    }

    private void unlockAllNodes( KernelStatement state )
    {
        PrimitiveLongIterator iterator = nodeIds.iterator();
        while ( iterator.hasNext() )
        {
            state.locks().optimistic().releaseExclusive( ResourceTypes.NODE, iterator.next() );
        }
        nodeIds.clear();
    }

    private boolean performAction( KernelStatement state, RelationshipItem rel, boolean first ) throws KernelException
    {
        if ( first )
        {
            if ( rel.id() != firstRelId )
            {
                // if the first relationship is not the same someone added some new rels, so we need to
                // lock them all again
                unlockAllNodes( state );
                return true;
            }
        }

        relIdAction.accept( rel.id() );
        return false;
    }

    private void collectNodeId( RelationshipItem rel )
    {
        if ( firstRelId == -1 )
        {
            firstRelId = rel.id();
        }

        nodeIds.add( rel.startNode() );
        nodeIds.add( rel.endNode() );
    }
}
