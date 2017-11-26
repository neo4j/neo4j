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

import java.util.Arrays;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveArrays;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;

class TwoPhaseNodeForRelationshipLocking
{
    private final EntityReadOperations ops;
    private final ThrowingConsumer<Long,KernelException> relIdAction;

    private long firstRelId;
    private long[] sortedNodeIds;

    TwoPhaseNodeForRelationshipLocking( EntityReadOperations entityReadOperations,
            ThrowingConsumer<Long,KernelException> relIdAction )
    {
        this.ops = entityReadOperations;
        this.relIdAction = relIdAction;
    }

    void lockAllNodesAndConsumeRelationships( long nodeId, final KernelStatement state ) throws KernelException
    {
        boolean retry;
        do
        {
            retry = false;
            firstRelId = NO_SUCH_RELATIONSHIP;

            // lock all the nodes involved by following the node id ordering
            collectAndSortNodeIds( nodeId, state );

            lockAllNodes( state, sortedNodeIds );

            // perform the action on each relationship, we will retry if the the relationship iterator contains new relationships
            try ( Cursor<NodeItem> node = ops.nodeCursorById( state, nodeId ) )
            {
                try ( Cursor<RelationshipItem> relationships = ops.nodeGetRelationships( state, node.get(), Direction.BOTH ) )
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

    private void collectAndSortNodeIds( long nodeId, KernelStatement state ) throws EntityNotFoundException
    {
        PrimitiveLongSet nodeIdSet = Primitive.longSet();
        nodeIdSet.add( nodeId );

        try ( Cursor<NodeItem> node = ops.nodeCursorById( state, nodeId ) )
        {
            try ( Cursor<RelationshipItem> rels = ops.nodeGetRelationships( state, node.get(), Direction.BOTH ) )
            {
                while ( rels.next() )
                {
                    RelationshipItem rel = rels.get();
                    if ( firstRelId == NO_SUCH_RELATIONSHIP )
                    {
                        firstRelId = rel.id();
                    }

                    nodeIdSet.add( rel.startNode() );
                    nodeIdSet.add( rel.endNode() );
                }
            }
        }

        long[] nodeIds = PrimitiveArrays.of( nodeIdSet );
        Arrays.sort( nodeIds );
        this.sortedNodeIds = nodeIds;
    }

    private void lockAllNodes( KernelStatement state, long[] nodeIds )
    {
        state.locks().optimistic().acquireExclusive( state.lockTracer(), ResourceTypes.NODE, nodeIds );
    }

    private void unlockAllNodes( KernelStatement state, long[] nodeIds  )
    {
        for ( long nodeId : nodeIds )
        {
            state.locks().optimistic().releaseExclusive( ResourceTypes.NODE, nodeId );
        }
    }

    private boolean performAction( KernelStatement state, RelationshipItem rel, boolean first ) throws KernelException
    {
        if ( first )
        {
            if ( rel.id() != firstRelId )
            {
                // if the first relationship is not the same someone added some new rels, so we need to
                // lock them all again
                unlockAllNodes( state, sortedNodeIds );
                sortedNodeIds = null;
                return true;
            }
        }

        relIdAction.accept( rel.id() );
        return false;
    }
}
