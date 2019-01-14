/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveArrays;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;

class TwoPhaseNodeForRelationshipLocking
{
    private final ThrowingConsumer<Long,KernelException> relIdAction;

    private long firstRelId;
    private long[] sortedNodeIds;
    private static final long[] EMPTY = new long[0];
    private final Locks.Client locks;
    private final LockTracer lockTracer;

    TwoPhaseNodeForRelationshipLocking(
            ThrowingConsumer<Long,KernelException> relIdAction, Locks.Client locks,
            LockTracer lockTracer )
    {
        this.relIdAction = relIdAction;
        this.locks = locks;
        this.lockTracer = lockTracer;
    }

    void lockAllNodesAndConsumeRelationships( long nodeId, final Transaction transaction, NodeCursor nodes ) throws KernelException
    {
        boolean retry;
        do
        {
            retry = false;
            firstRelId = NO_SUCH_RELATIONSHIP;

            // lock all the nodes involved by following the node id ordering
            collectAndSortNodeIds( nodeId, transaction, nodes );
            lockAllNodes( sortedNodeIds );

            // perform the action on each relationship, we will retry if the the relationship iterator contains
            // new relationships
            org.neo4j.internal.kernel.api.Read read = transaction.dataRead();
            read.singleNode( nodeId, nodes );
            //if the node is not there, someone else probably deleted it, just ignore
            if ( nodes.next() )
            {
                RelationshipSelectionCursor rels =
                        RelationshipSelections.allCursor( transaction.cursors(), nodes, null );
                boolean first = true;
                while ( rels.next() && !retry )
                {
                    retry = performAction( rels.relationshipReference(), first );
                    first = false;
                }
            }
        }
        while ( retry );
    }

    private void collectAndSortNodeIds( long nodeId, Transaction transaction, NodeCursor nodes )
    {
        PrimitiveLongSet nodeIdSet = Primitive.longSet();
        nodeIdSet.add( nodeId );

        org.neo4j.internal.kernel.api.Read read = transaction.dataRead();
        read.singleNode( nodeId, nodes );
        if ( !nodes.next() )
        {
            this.sortedNodeIds = EMPTY;
            return;
        }
        RelationshipSelectionCursor rels =
                RelationshipSelections.allCursor( transaction.cursors(), nodes, null );
        while ( rels.next() )
        {
            if ( firstRelId == NO_SUCH_RELATIONSHIP )
            {
                firstRelId = rels.relationshipReference();
            }

            nodeIdSet.add( rels.sourceNodeReference() );
            nodeIdSet.add( rels.targetNodeReference() );
        }

        long[] nodeIds = PrimitiveArrays.of( nodeIdSet );
        Arrays.sort( nodeIds );
        this.sortedNodeIds = nodeIds;
    }

    private void lockAllNodes( long[] nodeIds )
    {
        locks.acquireExclusive( lockTracer, ResourceTypes.NODE, nodeIds );
    }

    private void unlockAllNodes( long[] nodeIds )
    {
        locks.releaseExclusive( ResourceTypes.NODE, nodeIds );
    }

    private boolean performAction( long rel, boolean first )
            throws KernelException
    {
        if ( first )
        {
            if ( rel != firstRelId )
            {
                // if the first relationship is not the same someone added some new rels, so we need to
                // lock them all again
                unlockAllNodes( sortedNodeIds );
                sortedNodeIds = null;
                return true;
            }
        }

        relIdAction.accept( rel );
        return false;
    }
}
