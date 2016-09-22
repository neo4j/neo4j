/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;

class TwoPhaseNodeForRelationshipLocking
{
    private final PrimitiveLongSet nodeIds = Primitive.longSet();
    private final EntityReadOperations entityReadOperations;
    private final ThrowingConsumer<Long,KernelException> relIdAction;

    private boolean retry = true;
    private long firstRelId;
    private boolean first;

    private final RelationshipVisitor<RuntimeException> collectNodeIdVisitor =
            (relId, type, startNode, endNode) -> {
                if ( firstRelId == -1 )
                {
                    firstRelId = relId;
                }
                nodeIds.add( startNode );
                nodeIds.add( endNode );
            };

    private final RelationshipVisitor<KernelException> relationshipConsumingVisitor =
            new RelationshipVisitor<KernelException>()
            {
                @Override
                public void visit( long relId, int type, long startNode, long endNode ) throws KernelException
                {
                    if ( first )
                    {
                        first = false;
                        if ( relId != firstRelId )
                        {
                            // if the first relationship is not the same someone added some new rels, so we need to
                            // lock them all again
                            retry = true;
                            return;
                        }
                    }

                    relIdAction.accept( relId );
                }
            };

    TwoPhaseNodeForRelationshipLocking( EntityReadOperations entityReadOperations, ThrowingConsumer<Long,KernelException> relIdAction )
    {
        this.entityReadOperations = entityReadOperations;
        this.relIdAction = relIdAction;
    }

    void lockAllNodesAndConsumeRelationships( long nodeId, final KernelStatement state ) throws KernelException
    {
        nodeIds.add( nodeId );
        while ( retry )
        {
            retry = false;
            first = true;
            firstRelId = -1;

            // lock all the nodes involved by following the node id ordering
            try ( Cursor<NodeItem> cursor = entityReadOperations.nodeCursorById( state, nodeId ) )
            {
                RelationshipIterator relationships = cursor.get().getRelationships( Direction.BOTH );
                while ( relationships.hasNext() )
                {
                    entityReadOperations.relationshipVisit( state, relationships.next(), collectNodeIdVisitor );
                }
            }

            PrimitiveLongIterator nodeIdIterator = nodeIds.iterator();
            while ( nodeIdIterator.hasNext() )
            {
                state.locks().optimistic().acquireExclusive( ResourceTypes.NODE, nodeIdIterator.next() );
            }

            // perform the action on each relationship, we will retry if the the relationship iterator contains new relationships
            try ( Cursor<NodeItem> cursor = entityReadOperations.nodeCursorById( state, nodeId ) )
            {
                RelationshipIterator relationships = cursor.get().getRelationships( Direction.BOTH );
                while ( relationships.hasNext() )
                {
                    entityReadOperations.relationshipVisit( state, relationships.next(), relationshipConsumingVisitor );
                    if ( retry )
                    {
                        PrimitiveLongIterator iterator = nodeIds.iterator();
                        while ( iterator.hasNext() )
                        {
                            state.locks().optimistic().releaseExclusive( ResourceTypes.NODE, iterator.next() );
                        }
                        nodeIds.clear();
                        break;
                    }
                }
            }
        }
    }
}
