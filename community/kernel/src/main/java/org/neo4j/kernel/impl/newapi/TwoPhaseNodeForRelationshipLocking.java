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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.LockTracer;

class TwoPhaseNodeForRelationshipLocking
{
    private final ThrowingConsumer<Long,KernelException> relIdAction;

    private long[] sortedNodeIds;
    private MutableLongSet relIds;
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
            relIds = new LongHashSet();

            collectAndSortNodeIds( nodeId, transaction, nodes );
            // lock all the nodes involved by following the node id ordering
            lockAllNodes( sortedNodeIds );

            // perform the action on each relationship, we will retry if the the relationship iterator contains
            // new relationships
            transaction.dataRead().singleNode( nodeId, nodes );
            //if the node is not there, someone else probably deleted it, just ignore
            if ( nodes.next() )
            {
                try ( RelationshipSelectionCursor rels =
                              RelationshipSelections.allCursor( transaction.cursors(), nodes, null ) )
                {
                    while ( rels.next() && !retry )
                    {
                        if ( !relIds.contains( rels.relationshipReference() ) )
                        {
                            retry = true;
                            unlockAllNodes( sortedNodeIds );
                            sortedNodeIds = null;
                        }
                    }
                }
            }
        }
        while ( retry );
        long[] sortedRelIds = relIds.toSortedArray();
        if ( sortedRelIds.length > 0 )
        {
            locks.acquireExclusive( lockTracer, ResourceTypes.RELATIONSHIP, sortedRelIds );
            for ( long relId : sortedRelIds )
            {
                relIdAction.accept( relId );
            }
        }
    }

    private void collectAndSortNodeIds( long nodeId, Transaction transaction, NodeCursor nodes )
    {
        final MutableLongSet nodeIdSet = new LongHashSet();
        nodeIdSet.add( nodeId );

        org.neo4j.internal.kernel.api.Read read = transaction.dataRead();
        read.singleNode( nodeId, nodes );
        if ( !nodes.next() )
        {
            this.sortedNodeIds = EMPTY;
            return;
        }
        try ( RelationshipSelectionCursor rels =
                      RelationshipSelections.allCursor( transaction.cursors(), nodes, null ) )
        {
            while ( rels.next() )
            {
                relIds.add( rels.relationshipReference() );
                nodeIdSet.add( rels.sourceNodeReference() );
                nodeIdSet.add( rels.targetNodeReference() );
            }
        }

        this.sortedNodeIds = nodeIdSet.toSortedArray();
    }

    private void lockAllNodes( long[] nodeIds )
    {
        locks.acquireExclusive( lockTracer, ResourceTypes.NODE, nodeIds );
    }

    private void unlockAllNodes( long[] nodeIds )
    {
        locks.releaseExclusive( ResourceTypes.NODE, nodeIds );
    }
}
