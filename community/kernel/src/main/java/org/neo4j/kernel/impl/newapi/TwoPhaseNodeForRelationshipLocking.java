/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveArrays;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;

class TwoPhaseNodeForRelationshipLocking
{
    private final ThrowingConsumer<Long,KernelException> relIdAction;

    private long firstRelId;
    private long[] sortedNodeIds;

    TwoPhaseNodeForRelationshipLocking(
            ThrowingConsumer<Long,KernelException> relIdAction )
    {
        this.relIdAction = relIdAction;
    }

    void lockAllNodesAndConsumeRelationships( long nodeId, final KernelTransactionImplementation transaction )
            throws KernelException
    {
        boolean retry;
        do
        {
            retry = false;
            firstRelId = NO_SUCH_RELATIONSHIP;

            // lock all the nodes involved by following the node id ordering
            collectAndSortNodeIds( nodeId, transaction );

            lockAllNodes( transaction, sortedNodeIds );

            // perform the action on each relationship, we will retry if the the relationship iterator contains
            // new relationships
            NodeCursor nodes = transaction.nodeCursor();
            org.neo4j.internal.kernel.api.Read read = transaction.dataRead();
            read.singleNode( nodeId, nodes );
            RelationshipSelectionCursor rels =
                    RelationshipSelections.allCursor( transaction.cursors(), nodes, null );
            boolean first = true;
            while ( rels.next() && !retry )
            {
                retry = performAction( transaction, rels.relationshipReference(), first );
                first = false;
            }
        }
        while ( retry );
    }

    private void collectAndSortNodeIds( long nodeId, KernelTransaction transaction ) throws EntityNotFoundException
    {
        PrimitiveLongSet nodeIdSet = Primitive.longSet();
        nodeIdSet.add( nodeId );

        NodeCursor nodes = transaction.nodeCursor();
        org.neo4j.internal.kernel.api.Read read = transaction.dataRead();
        read.singleNode( nodeId, nodes );
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

    private void lockAllNodes( KernelTransactionImplementation transaction, long[] nodeIds )
    {
        transaction.statementLocks().optimistic()
                .acquireExclusive( transaction.lockTracer(), ResourceTypes.NODE, nodeIds );
    }

    private void unlockAllNodes( KernelTransactionImplementation transaction, long[] nodeIds )
    {
        for ( long nodeId : nodeIds )
        {
            transaction.statementLocks().optimistic().releaseExclusive( ResourceTypes.NODE, nodeId );
        }
    }

    private boolean performAction( KernelTransactionImplementation transaction, long rel, boolean first )
            throws KernelException
    {
        if ( first )
        {
            if ( rel != firstRelId )
            {
                // if the first relationship is not the same someone added some new rels, so we need to
                // lock them all again
                unlockAllNodes( transaction, sortedNodeIds );
                sortedNodeIds = null;
                return true;
            }
        }

        relIdAction.accept( rel );
        return false;
    }
}
