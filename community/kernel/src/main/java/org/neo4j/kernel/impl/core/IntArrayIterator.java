/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.RelIdArray;

class IntArrayIterator implements Iterable<Relationship>,
    Iterator<Relationship>
{
    private Iterator<RelTypeElementIterator> typeIterator;
    private RelTypeElementIterator currentTypeIterator = null;
    private final NodeImpl fromNode;
    private final Direction direction;
    private Relationship nextElement = null;
    private final NodeManager nodeManager;
    private final RelationshipType types[];

    private final List<RelTypeElementIterator> rels;

    IntArrayIterator( List<RelTypeElementIterator> rels, NodeImpl fromNode,
        Direction direction, NodeManager nodeManager, RelationshipType[] types )
    {
        this.rels = rels;
        this.typeIterator = rels.iterator();
        if ( typeIterator.hasNext() )
        {
            currentTypeIterator = typeIterator.next();
        }
        else
        {
            currentTypeIterator = RelTypeElementIterator.EMPTY;
        }
        this.fromNode = fromNode;
        this.direction = direction;
        this.nodeManager = nodeManager;
        this.types = types;
    }

    public Iterator<Relationship> iterator()
    {
        return this;
    }

    public boolean hasNext()
    {
        if ( nextElement != null )
        {
            return true;
        }
        do
        {
            if ( currentTypeIterator.hasNext( nodeManager ) )
            {
                long nextId = currentTypeIterator.next( nodeManager );
                try
                {
                    if ( direction == Direction.BOTH )
                    {
                        nextElement = new RelationshipProxy( nextId, nodeManager );
                        return true;
                    }
                    RelationshipImpl possibleElement = nodeManager.getRelForProxy( nextId );
                    if ( direction == Direction.INCOMING 
                         && possibleElement.getEndNodeId() == fromNode.id )
                    {
                        nextElement = new RelationshipProxy( nextId, nodeManager );
                        return true;
                    }
                    else if ( direction == Direction.OUTGOING
                              && possibleElement.getStartNodeId() == fromNode.id )
                    {
                        nextElement = new RelationshipProxy( nextId, nodeManager );
                        return true;
                    }
                    // No match
                }
                catch ( NotFoundException e )
                { // ok deleted 
                }
            }
            
            while ( !currentTypeIterator.hasNext( nodeManager ) )
            {
                if ( typeIterator.hasNext() )
                {
                    currentTypeIterator = typeIterator.next();
                }
                else if ( fromNode.getMoreRelationships( nodeManager ) )
                {
                    Map<String, RelTypeElementIterator> newRels = new HashMap<String, RelTypeElementIterator>();
                    for ( RelTypeElementIterator itr : rels )
                    {
                        RelTypeElementIterator newItr = itr;
                        if ( itr.isSrcEmpty() )
                        {
                            RelIdArray newSrc = fromNode.getRelationshipIds( itr.getType() );
                            if ( newSrc != null )
                            {
                                newItr = itr.setSrc( newSrc );
                            }
                        }
                        newRels.put( newItr.getType(), newItr );
                    }
                    if ( types.length == 0 )
                    {
                        for ( Map.Entry<String, RelIdArray> entry : fromNode.getRelationshipIds().entrySet() )
                        {
                            String type = entry.getKey();
                            RelTypeElementIterator itr = newRels.get( type );
                            if ( itr == null || itr.isSrcEmpty() )
                            {
                                itr = itr == null ? new FastRelTypeElement( type, fromNode, entry.getValue() ) :
                                        itr.setSrc( entry.getValue() );
                                newRels.put( type, itr );
                            }
                        }
                    }
                    
                    rels.clear();
                    rels.addAll( newRels.values() );
                    
                    typeIterator = rels.iterator();
                    currentTypeIterator = typeIterator.hasNext() ? typeIterator.next() : RelTypeElementIterator.EMPTY;
                }
                else
                {
                    break;
                }
            }
         } while ( currentTypeIterator.hasNext( nodeManager ) );
        // no next element found
        return false;
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
