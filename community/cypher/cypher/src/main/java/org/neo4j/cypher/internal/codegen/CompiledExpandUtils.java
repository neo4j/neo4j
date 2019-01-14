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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;

import static org.neo4j.internal.kernel.api.helpers.Nodes.countAll;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countIncoming;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoing;

@SuppressWarnings( "unused" )
public abstract class CompiledExpandUtils
{
    private static final int NOT_DENSE_DEGREE = -1;

    public static RelationshipSelectionCursor connectingRelationships( Read read, CursorFactory cursors,
            NodeCursor nodeCursor,
            long fromNode, Direction direction, long toNode )
    {
        //Check from
        int fromDegree = nodeGetDegreeIfDense( read, fromNode, nodeCursor, cursors, direction );
        if ( fromDegree == 0 )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
        boolean fromNodeIsDense = fromDegree != NOT_DENSE_DEGREE;

        //Check to
        read.singleNode( toNode, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
        boolean toNodeIsDense = nodeCursor.isDense();

        //Both are dense, start with the one with the lesser degree
        if ( fromNodeIsDense && toNodeIsDense )
        {
            //Note that we have already position the cursor at toNode
            int toDegree = nodeGetDegree( nodeCursor, cursors, direction );
            long startNode;
            long endNode;
            Direction relDirection;
            if ( fromDegree < toDegree )
            {
                startNode = fromNode;
                endNode = toNode;
                relDirection = direction;
            }
            else
            {
                startNode = toNode;
                endNode = fromNode;
                relDirection = direction.reverse();
            }

            return connectingRelationshipsIterator( CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, startNode, relDirection ), endNode );
        }
        else if ( fromNodeIsDense )
        {
            return connectingRelationshipsIterator( CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, toNode, direction.reverse() ), fromNode );
        }
        else
        {   //either only toNode is dense or none of them, just go with what we got
            return connectingRelationshipsIterator( CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, fromNode, direction ), toNode );
        }
    }

    public static RelationshipSelectionCursor connectingRelationships( Read read, CursorFactory cursors,
            NodeCursor nodeCursor, long fromNode, Direction direction, long toNode, int[] relTypes )
    {
        //Check from
        int fromDegree = calculateTotalDegreeIfDense( read, fromNode, nodeCursor, direction, relTypes, cursors );
        if ( fromDegree == 0 )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
        boolean fromNodeIsDense = fromDegree != NOT_DENSE_DEGREE;

        //Check to
        read.singleNode( toNode, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
        boolean toNodeIsDense = nodeCursor.isDense();

        //Both are dense, start with the one with the lesser degree
        if ( fromNodeIsDense && toNodeIsDense )
        {
            //Note that we have already position the cursor at toNode
            int toDegree = calculateTotalDegree( nodeCursor, direction, relTypes, cursors );
            long startNode;
            long endNode;
            Direction relDirection;
            if ( fromDegree < toDegree )
            {
                startNode = fromNode;
                endNode = toNode;
                relDirection = direction;
            }
            else
            {
                startNode = toNode;
                endNode = fromNode;
                relDirection = direction.reverse();
            }

            return connectingRelationshipsIterator( CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, startNode, relDirection, relTypes ), endNode );
        }
        else if ( fromNodeIsDense )
        {
            return connectingRelationshipsIterator( CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, toNode, direction.reverse(), relTypes ), fromNode );
        }
        else
        {   //either only toNode is dense or none of them, just go with what we got
            return connectingRelationshipsIterator( CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, fromNode, direction, relTypes ), toNode );
        }
    }

    static int nodeGetDegreeIfDense( Read read, long node, NodeCursor nodeCursor, CursorFactory cursors,
            Direction direction )
    {
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return 0;
        }
        if ( !nodeCursor.isDense() )
        {
            return NOT_DENSE_DEGREE;
        }

        return nodeGetDegree( nodeCursor, cursors, direction );
    }

    private static int nodeGetDegree( NodeCursor nodeCursor, CursorFactory cursors,
            Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING:
            return countOutgoing( nodeCursor, cursors );
        case INCOMING:
            return countIncoming( nodeCursor, cursors );
        case BOTH:
            return countAll( nodeCursor, cursors );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    static int nodeGetDegreeIfDense( Read read, long node, NodeCursor nodeCursor, CursorFactory cursors,
            Direction direction, int type )
    {
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return 0;
        }
        if ( !nodeCursor.isDense() )
        {
            return NOT_DENSE_DEGREE;
        }

        return nodeGetDegree( nodeCursor, cursors, direction, type );
    }

    private static int nodeGetDegree( NodeCursor nodeCursor, CursorFactory cursors,
            Direction direction, int type )
    {
        switch ( direction )
        {
        case OUTGOING:
            return countOutgoing( nodeCursor, cursors, type );
        case INCOMING:
            return countIncoming( nodeCursor, cursors, type );
        case BOTH:
            return countAll( nodeCursor, cursors, type );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    private static int calculateTotalDegreeIfDense( Read read, long node, NodeCursor nodeCursor, Direction direction,
            int[] relTypes, CursorFactory cursors )
    {
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return 0;
        }
        if ( !nodeCursor.isDense() )
        {
            return NOT_DENSE_DEGREE;
        }
        return calculateTotalDegree( nodeCursor, direction, relTypes, cursors );
    }

    private static int calculateTotalDegree( NodeCursor nodeCursor, Direction direction, int[] relTypes,
            CursorFactory cursors )
    {
        int degree = 0;
        for ( int relType : relTypes )
        {
            degree += nodeGetDegree( nodeCursor, cursors, direction, relType );
        }

        return degree;
    }

    private static RelationshipSelectionCursor connectingRelationshipsIterator(
            final RelationshipSelectionCursor allRelationships, final long toNode )
    {
        return new RelationshipSelectionCursor()
        {
            @Override
            public void close()
            {
                allRelationships.close();
            }

            @Override
            public long relationshipReference()
            {
                return allRelationships.relationshipReference();
            }

            @Override
            public int type()
            {
                return allRelationships.type();
            }

            @Override
            public long otherNodeReference()
            {
                return allRelationships.otherNodeReference();
            }

            @Override
            public long sourceNodeReference()
            {
                return allRelationships.sourceNodeReference();
            }

            @Override
            public long targetNodeReference()
            {
                return allRelationships.targetNodeReference();
            }

            @Override
            public long propertiesReference()
            {
                return allRelationships.propertiesReference();
            }

            @Override
            public boolean next()
            {
                while ( allRelationships.next() )
                {
                    if ( allRelationships.otherNodeReference() == toNode )
                    {
                        return true;
                    }
                }

                return false;
            }
        };
    }
}
