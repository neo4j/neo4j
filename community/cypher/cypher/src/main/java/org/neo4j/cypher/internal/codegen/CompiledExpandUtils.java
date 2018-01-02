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
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

//TODO this must live outside 2.3
public abstract class CompiledExpandUtils
{
    public static RelationshipIterator connectingRelationships( ReadOperations readOperations,
            long fromNode, Direction direction, long toNode ) throws EntityNotFoundException
    {
        int fromDegree = readOperations.nodeGetDegree( fromNode, direction );
        if ( fromDegree == 0 )
        {
            return RelationshipIterator.EMPTY;
        }

        int toDegree = readOperations.nodeGetDegree( toNode, direction.reverse() );
        if ( toDegree == 0 )
        {
            return RelationshipIterator.EMPTY;
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

        RelationshipIterator allRelationships =  readOperations.nodeGetRelationships( startNode, relDirection );

        return connectingRelationshipsIterator( allRelationships, startNode, endNode );
    }


    public static RelationshipIterator connectingRelationships( ReadOperations readOperations,
            long fromNode, Direction direction, long toNode, int... relTypes ) throws EntityNotFoundException
    {
        int fromDegree = calculateTotalDegree( readOperations, fromNode, direction, relTypes);
        if ( fromDegree == 0 )
        {
            return RelationshipIterator.EMPTY;
        }

        int toDegree = calculateTotalDegree( readOperations, toNode, direction.reverse(), relTypes );
        if ( toDegree == 0 )
        {
            return RelationshipIterator.EMPTY;
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

        RelationshipIterator allRelationships = readOperations.nodeGetRelationships( startNode, relDirection, relTypes );

        return connectingRelationshipsIterator( allRelationships, startNode, endNode );
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

    private static RelationshipIterator connectingRelationshipsIterator( final RelationshipIterator allRelationships,
            final long fromNode, final long toNode )
    {
        return new RelationshipIterator.BaseIterator()
        {
            private final RelationshipDataExtractor extractor = new RelationshipDataExtractor();

            @Override
            public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                    RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
            {
                visitor.visit( extractor.relationship(), extractor.type(), extractor.startNode(), extractor.endNode() );
                return false;
            }

            @Override
            protected boolean fetchNext()
            {
                while ( allRelationships.hasNext() )
                {
                    allRelationships.relationshipVisit( allRelationships.next(), extractor );
                    if ( extractor.otherNode( fromNode ) == toNode )
                    {
                        next = extractor.relationship();
                        return true;
                    }
                }

                return false;
            }
        };
    }

}
