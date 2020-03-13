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

import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;

class TwoPhaseNodeForRelationshipLocking
{
    private final ThrowingConsumer<Long,KernelException> relIdAction;
    private final Locks.Client locks;
    private final LockTracer lockTracer;
    private final PageCursorTracer cursorTracer;

    TwoPhaseNodeForRelationshipLocking( ThrowingConsumer<Long,KernelException> relIdAction, Locks.Client locks, LockTracer lockTracer,
            PageCursorTracer cursorTracer )
    {
        this.relIdAction = relIdAction;
        this.locks = locks;
        this.lockTracer = lockTracer;
        this.cursorTracer = cursorTracer;
    }

    void lockAllNodesAndConsumeRelationships( long nodeId, final KernelTransaction transaction, NodeCursor nodes ) throws KernelException
    {
        // Read-lock the node, and acquire a consistent view of its relationships.
        locks.acquireShared( lockTracer, ResourceTypes.NODE, nodeId );
        long[] sortedNodeIds = collectAndSortNodeIds( nodeId, transaction, nodes );

        // Lock all the nodes involved by following the node id ordering
        lockAllNodes( sortedNodeIds );

        // Perform the action on each relationship.
        transaction.dataRead().singleNode( nodeId, nodes );

        // If the node is not there, someone else probably deleted it, just ignore.
        if ( nodes.next() )
        {
            try ( RelationshipSelectionCursor rels = RelationshipSelections.allCursor( transaction.cursors(), nodes, null, cursorTracer ) )
            {
                while ( rels.next() )
                {
                    relIdAction.accept( rels.relationshipReference() );
                }
            }
        }
    }

    private long[] collectAndSortNodeIds( long nodeId, KernelTransaction transaction, NodeCursor nodes )
    {
        MutableLongSet nodeIdSet = new LongHashSet();
        nodeIdSet.add( nodeId );

        transaction.dataRead().singleNode( nodeId, nodes );
        if ( nodes.next() )
        {
            try ( RelationshipSelectionCursor rels = RelationshipSelections.allCursor( transaction.cursors(), nodes, null, cursorTracer ) )
            {
                while ( rels.next() )
                {
                    nodeIdSet.add( rels.sourceNodeReference() );
                    nodeIdSet.add( rels.targetNodeReference() );
                }
            }
        }

        return nodeIdSet.toSortedArray();
    }

    private void lockAllNodes( long[] nodeIds )
    {
        locks.acquireExclusive( lockTracer, ResourceTypes.NODE, nodeIds );
    }
}
