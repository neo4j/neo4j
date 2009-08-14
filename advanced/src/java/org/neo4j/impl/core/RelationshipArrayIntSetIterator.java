/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.util.ArrayIntSet;

class RelationshipArrayIntSetIterator implements Iterable<Relationship>,
    Iterator<Relationship>
{
    private Logger log = Logger
        .getLogger( RelationshipArrayIntSetIterator.class.getName() );

    private Iterator<Integer> relIds;
    private Node fromNode;
    private Direction direction = null;
    private Relationship nextElement = null;
    private final NodeManager nodeManager;

    RelationshipArrayIntSetIterator( ArrayIntSet relIds, Node fromNode,
        Direction direction, NodeManager nodeManager )
    {
        this.relIds = relIds.iterator();
        this.fromNode = fromNode;
        this.direction = direction;
        this.nodeManager = nodeManager;
    }

    RelationshipArrayIntSetIterator( Iterator<Integer> relIds, Node fromNode,
        Direction direction, NodeManager nodeManager )
    {
        this.relIds = relIds;
        this.fromNode = fromNode;
        this.direction = direction;
        this.nodeManager = nodeManager;
    }

    public Iterator<Relationship> iterator()
    {
        return new RelationshipArrayIntSetIterator( relIds, fromNode,
            direction, nodeManager );
    }

    public boolean hasNext()
    {
        if ( nextElement != null )
        {
            return true;
        }
        do
        {
            // if ( nextPosition < relIds.length )
            if ( relIds.hasNext() )
            {
                int nextId = relIds.next();
                try
                {
                    Relationship possibleElement = nodeManager
                        .getRelationshipById( nextId );
                    if ( direction == Direction.INCOMING
                        && possibleElement.getEndNode().equals( fromNode ) )
                    {
                        nextElement = possibleElement;
                    }
                    else if ( direction == Direction.OUTGOING
                        && possibleElement.getStartNode().equals( fromNode ) )
                    {
                        nextElement = possibleElement;
                    }
                    else if ( direction == Direction.BOTH )
                    {
                        nextElement = possibleElement;
                    }
                }
                catch ( Throwable t )
                {
                    log.log( Level.FINE,
                        "Unable to get relationship " + nextId, t );
                }
            }
        }
        while ( nextElement == null && relIds.hasNext() );
        return nextElement != null;
    }

    public Relationship next()
    {
        hasNext();
        if ( nextElement != null )
        {
            Relationship elementToReturn = nextElement;
            nextElement = null;
            return elementToReturn;
        }
        throw new NoSuchElementException();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
