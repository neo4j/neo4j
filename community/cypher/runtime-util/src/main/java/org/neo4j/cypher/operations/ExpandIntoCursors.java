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
package org.neo4j.cypher.operations;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CloseListener;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;

import static org.neo4j.internal.kernel.api.helpers.Nodes.countAllDense;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countIncomingDense;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoingDense;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allDenseCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allSparseCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingDenseCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingSparseCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingDenseCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingSparseCursor;

/**
 * Tools for creating a specialized cursor for handling Expand(Into)
 */
@SuppressWarnings( "unused" )
public abstract class ExpandIntoCursors
{
    private static final int NOT_DENSE_DEGREE = -1;

    /**
     * Creates a cursor for all connecting relationships given a start- and an endnode.
     * @param read Used for accessing store
     * @param nodeCursor Node cursor used in traversal
     * @param groupCursor Group cursor used in traversal
     * @param traversalCursor Traversal cursor used in traversal
     * @param fromNode The start node
     * @param direction The direction relative <code>fromNode</code>
     * @param toNode The end node
     * @param types The types of the interconnecting relationships or <code>null</code> if all types.
     * @return The interconnecting relationships in the given direction with any of the given types.
     */
    public static RelationshipSelectionCursor connectingRelationships(
            Read read,
            NodeCursor nodeCursor,
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor traversalCursor,
            long fromNode,
            Direction direction,
            long toNode,
            int[] types )
    {
        //Check from
        int fromDegree = calculateTotalDegreeIfDense( read, fromNode, nodeCursor, direction, types, groupCursor );
        if ( fromDegree == 0 )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
        boolean fromNodeIsDense = fromDegree != NOT_DENSE_DEGREE;

        //Check to
        if ( !singleNode( read, nodeCursor, toNode ) )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
        boolean toNodeIsDense = nodeCursor.isDense();

        //Both are dense, start with the one with the lesser degree
        if ( fromNodeIsDense && toNodeIsDense )
        {
            //Note that we have already position the cursor at toNode
            int toDegree = nodeGetDegreeDense( nodeCursor, groupCursor, direction );
            long startNode;
            long endNode;
            Direction relDirection;
            if ( fromDegree < toDegree )
            {
                // here we must reposition cursor to fromNode
                singleNode( read, nodeCursor, fromNode );
                endNode = toNode;
                relDirection = direction;
            }
            else
            {
                //cursor is already pointing at toNode
                endNode = fromNode;
                relDirection = direction.reverse();
            }

            return denseConnectingRelationshipsCursor( relDirection, groupCursor, traversalCursor, nodeCursor, types, endNode );
        }
        else if ( toNodeIsDense )
        {
            //we need to point the cursor to fromNode again
            singleNode( read, nodeCursor, fromNode );

            //TODO this closing is for compiled runtime and can be removed with the compiled runtime
            groupCursor.close();
            return sparseConnectingRelationshipsCursor( direction, traversalCursor, nodeCursor, types, toNode );
        }
        else
        {
            //Either the from node is dense or both are sparse, either way since the node cursor is currently pointing at
            //the toNode lets start from that one.

            //TODO this closing is for compiled runtime and can be removed with the compiled runtime
            groupCursor.close();
            return sparseConnectingRelationshipsCursor( direction.reverse(), traversalCursor, nodeCursor, types, fromNode );
        }
    }

    public static RelationshipSelectionCursor connectingRelationships(
            Read read,
            CursorFactory cursors,
            NodeCursor nodeCursor,
            long fromNode,
            Direction direction,
            long toNode )
    {
        return connectingRelationships( read, nodeCursor, cursors.allocateRelationshipGroupCursor(),
                cursors.allocateRelationshipTraversalCursor(), fromNode, direction, toNode, null );
    }

    public static RelationshipSelectionCursor connectingRelationships(
            Read read,
            CursorFactory cursors,
            NodeCursor nodeCursor,
            long fromNode,
            Direction direction,
            long toNode,
            int[] types )
    {
        return connectingRelationships( read, nodeCursor, cursors.allocateRelationshipGroupCursor(),
                cursors.allocateRelationshipTraversalCursor(), fromNode, direction, toNode, types );
    }

    public static int nodeGetDegreeDense( NodeCursor nodeCursor, RelationshipGroupCursor group, Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING:
            return countOutgoingDense( nodeCursor, group );
        case INCOMING:
            return countIncomingDense( nodeCursor, group );
        case BOTH:
            return countAllDense( nodeCursor, group );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    public static int nodeGetDegreeDense( NodeCursor nodeCursor, RelationshipGroupCursor groupCursor, Direction direction, int type )
    {
        switch ( direction )
        {
        case OUTGOING:
            return countOutgoingDense( nodeCursor, groupCursor, type );
        case INCOMING:
            return countIncomingDense( nodeCursor, groupCursor, type );
        case BOTH:
            return countAllDense( nodeCursor, groupCursor, type );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    private static int calculateTotalDegreeIfDense( Read read, long node, NodeCursor nodeCursor, Direction direction,
            int[] relTypes, RelationshipGroupCursor groupCursor )
    {
        if ( !singleNode( read, nodeCursor, node ) )
        {
            return 0;
        }
        if ( !nodeCursor.isDense() )
        {
            return NOT_DENSE_DEGREE;
        }
        return calculateTotalDegreeDense( nodeCursor, direction, relTypes, groupCursor );
    }

    private static int calculateTotalDegreeDense( NodeCursor nodeCursor, Direction direction, int[] relTypes,
            RelationshipGroupCursor groupCursor )
    {
        if ( relTypes == null )
        {
            return nodeGetDegreeDense( nodeCursor, groupCursor, direction );
        }
        int degree = 0;
        for ( int relType : relTypes )
        {
            degree += nodeGetDegreeDense( nodeCursor, groupCursor, direction, relType );
        }

        return degree;
    }

    private static RelationshipSelectionCursor denseConnectingRelationshipsCursor(
            Direction relDirection,
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor traversalCursor,
            NodeCursor nodeCursor,
            int[] types,
            long endNode )
    {
        switch ( relDirection )
        {
        case OUTGOING:
            return connectingRelationshipsCursor( outgoingDenseCursor( groupCursor, traversalCursor, nodeCursor, types ), endNode );
        case INCOMING:
            return connectingRelationshipsCursor( incomingDenseCursor( groupCursor, traversalCursor, nodeCursor, types ), endNode );
        case BOTH:
            return connectingRelationshipsCursor( allDenseCursor( groupCursor, traversalCursor, nodeCursor, types ), endNode );
        default:
            throw new IllegalStateException( "there is no such direction" );
        }
    }

    private static RelationshipSelectionCursor sparseConnectingRelationshipsCursor(
            Direction relDirection,
            RelationshipTraversalCursor traversalCursor,
            NodeCursor nodeCursor,
            int[] types,
            long endNode )
    {
        switch ( relDirection )
        {
        case OUTGOING:
            return connectingRelationshipsCursor( outgoingSparseCursor( traversalCursor, nodeCursor, types ), endNode );
        case INCOMING:
            return connectingRelationshipsCursor( incomingSparseCursor( traversalCursor, nodeCursor, types ), endNode );
        case BOTH:
            return connectingRelationshipsCursor( allSparseCursor( traversalCursor, nodeCursor, types ), endNode );
        default:
            throw new IllegalStateException( "there is no such direction" );
        }
    }

    private static boolean singleNode( Read read, NodeCursor nodeCursor, long node )
    {
        read.singleNode( node, nodeCursor );
        return nodeCursor.next();
    }

    private static RelationshipSelectionCursor connectingRelationshipsCursor(
            final RelationshipSelectionCursor allRelationships, final long toNode )
    {
        return new RelationshipSelectionCursor()
        {
            @Override
            public void close()
            {
                closeInternal();
                CloseListener closeListener = allRelationships.getCloseListener();
                if ( closeListener != null )
                {
                    closeListener.onClosed( this );
                }
            }

            @Override
            public void closeInternal()
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

            @Override
            public void setTracer( KernelReadTracer tracer )
            {
                allRelationships.setTracer( tracer );
            }

            @Override
            public boolean isClosed()
            {
                return allRelationships.isClosed();
            }

            @Override
            public void setCloseListener( CloseListener closeListener )
            {
                allRelationships.setCloseListener( closeListener );
            }

            @Override
            public CloseListener getCloseListener()
            {
                return allRelationships.getCloseListener();
            }

            @Override
            public void setToken( int token )
            {
                allRelationships.setToken( token );
            }

            @Override
            public int getToken()
            {
                return allRelationships.getToken();
            }

            @Override
            public void properties( PropertyCursor cursor )
            {
                allRelationships.properties( cursor );
            }
        };
    }
}
