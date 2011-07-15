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

import static org.neo4j.kernel.impl.core.RelTypeElementIterator.EMPTY;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

class IntArrayIterator implements Iterable<Relationship>,
    Iterator<Relationship>
{
    private Iterator<RelTypeElementIterator> typeIterator;
    private RelTypeElementIterator currentTypeIterator = null;
    private final NodeImpl fromNode;
    private final DirectionWrapper direction;
    private Relationship nextElement = null;
    private final NodeManager nodeManager;
    private final RelationshipType types[];

    private final List<RelTypeElementIterator> rels;
    
    // This is just for optimization
    private boolean isFullyLoaded;

    IntArrayIterator( List<RelTypeElementIterator> rels, NodeImpl fromNode,
        DirectionWrapper direction, NodeManager nodeManager, RelationshipType[] types,
        boolean isFullyLoaded )
    {
        this.rels = rels;
        this.isFullyLoaded = isFullyLoaded;
        this.typeIterator = rels.iterator();
        this.currentTypeIterator = typeIterator.hasNext() ? typeIterator.next() : EMPTY;
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
                    nextElement = new RelationshipProxy( nextId, nodeManager );
                    return true;
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
                else if ( fromNode.getMoreRelationships( nodeManager ) ||
                        // This is here to guard for that someone else might have loaded
                        // stuff in this relationship chain (and exhausted it) while I
                        // iterated over my batch of relationships. It will only happen
                        // for nodes which have more than <grab size> relationships and
                        // isn't fully loaded when starting iterating.
                        !isFullyLoaded )
                {
                    Map<String, RelTypeElementIterator> newRels = new HashMap<String, RelTypeElementIterator>();
                    for ( RelTypeElementIterator itr : rels )
                    {
                        RelTypeElementIterator newItr = itr;
                        RelIdArray newSrc = fromNode.getRelationshipIds( itr.getType() );
                        if ( newSrc != null )
                        {
                            if ( itr.isSrcEmpty() )
                            {
                                newItr = itr.setSrc( newSrc );
                            }
                            else if ( newSrc.couldBeNeedingUpdate() )
                            {
                                itr.updateSrc( newSrc );
                            }
                            newItr.notifyAboutMoreRelationships();
                        }
                        newRels.put( newItr.getType(), newItr );
                    }
                    
                    // If we wanted relationships of any type check if there are
                    // any new relationship types loaded for this node and if so
                    // initiate iterators for them
                    if ( types.length == 0 )
                    {
                        for ( RelIdArray ids : fromNode.getRelationshipIds() )
                        {
                            String type = ids.getType();
                            RelTypeElementIterator itr = newRels.get( type );
                            if ( itr == null || itr.isSrcEmpty() )
                            {
                                if ( itr == null )
                                {
                                    RelIdArray remove = nodeManager.getCowRelationshipRemoveMap( fromNode, type );
                                    itr = RelTypeElement.create( type, fromNode, ids, null, remove, direction );
                                }
                                else
                                {
                                    itr = itr.setSrc( ids );
                                }
                                newRels.put( type, itr );
                            }
                        }
                    }
                    
                    rels.clear();
                    rels.addAll( newRels.values() );
                    
                    typeIterator = rels.iterator();
                    currentTypeIterator = typeIterator.hasNext() ? typeIterator.next() : RelTypeElementIterator.EMPTY;
                    isFullyLoaded = !fromNode.hasMoreRelationshipsToLoad();
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
