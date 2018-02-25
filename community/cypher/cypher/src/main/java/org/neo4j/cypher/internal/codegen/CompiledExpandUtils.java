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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.kernel.api.ReadOperations;

public abstract class CompiledExpandUtils
{
    public static RelationshipSelectionCursor connectingRelationships(  ReadOperations readOperations,
            Read read, CursorFactory cursors, NodeCursor nodeCursor,
            long fromNode, Direction direction, long toNode )
    {
        try
        {
            int fromDegree = readOperations.nodeGetDegree( fromNode, direction );
            if ( fromDegree == 0 )
            {
                return RelationshipSelectionCursor.EMPTY;
            }

            int toDegree = readOperations.nodeGetDegree( toNode, direction.reverse() );
            if ( toDegree == 0 )
            {
                return RelationshipSelectionCursor.EMPTY;
            }

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

            RelationshipSelectionCursor selectionCursor = CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, startNode, relDirection );

            return connectingRelationshipsIterator( selectionCursor, endNode );
        }
        catch ( EntityNotFoundException ignore )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
    }

    public static RelationshipSelectionCursor connectingRelationships( ReadOperations readOperations, Read read, CursorFactory cursors, NodeCursor nodeCursor,
            long fromNode, Direction direction, long toNode, int[] relTypes )
    {
        try
        {
            int fromDegree = calculateTotalDegree( readOperations, fromNode, direction, relTypes );
            if ( fromDegree == 0 )
            {
                return RelationshipSelectionCursor.EMPTY;
            }

            int toDegree = calculateTotalDegree( readOperations, toNode, direction.reverse(), relTypes );
            if ( toDegree == 0 )
            {
                return RelationshipSelectionCursor.EMPTY;
            }

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

            RelationshipSelectionCursor selectionCursor = CompiledCursorUtils
                    .nodeGetRelationships( read, cursors, nodeCursor, startNode, relDirection, relTypes );

            return connectingRelationshipsIterator( selectionCursor, endNode );
        }
        catch ( EntityNotFoundException ignore )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
    }

    private static int calculateTotalDegree( ReadOperations readOperations, long fromNode, Direction direction,
            int[] relTypes ) throws EntityNotFoundException
    {
        int degree = 0;
        for ( int relType : relTypes )
        {
            degree += readOperations.nodeGetDegree( fromNode, direction, relType );
        }

        return degree;
    }

    private static RelationshipSelectionCursor connectingRelationshipsIterator( final RelationshipSelectionCursor allRelationships, final long toNode )
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
