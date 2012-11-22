/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.core.NodeImpl.LoadStatus;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdIterator;

class RelationshipIterator extends PrefetchingIterator<Relationship> implements Iterable<Relationship>
{
    private RelIdIterator[] rels;
    private int currentTypeIndex;
    private final NodeImpl fromNode;
    private final DirectionWrapper direction;
    private final NodeManager nodeManager;
    
    private boolean lastTimeILookedThereWasMoreToLoad;
    private final boolean allTypes;

    RelationshipIterator( RelIdIterator[] rels, NodeImpl fromNode,
        DirectionWrapper direction, NodeManager nodeManager, boolean hasMoreToLoad, boolean allTypes )
    {
        initializeRels( rels );
        this.lastTimeILookedThereWasMoreToLoad = hasMoreToLoad;
        this.fromNode = fromNode;
        this.direction = direction;
        this.nodeManager = nodeManager;
        this.allTypes = allTypes;
    }

    private void initializeRels( RelIdIterator[] rels )
    {
        this.rels = rels;
        this.currentTypeIndex = 0;
    }

    public Iterator<Relationship> iterator()
    {
        return this;
    }

    @Override
    protected Relationship fetchNextOrNull()
    {
        RelIdIterator currentTypeIterator = rels[currentTypeIndex];
        do
        {
            if ( currentTypeIterator.hasNext() )
            {
                long nextId = currentTypeIterator.next();
                try
                {
                    return nodeManager.newRelationshipProxyById( nextId );
                }
                catch ( NotFoundException e )
                { // ok deleted 
                }
            }
            
            LoadStatus status;
            while ( !currentTypeIterator.hasNext() )
            {
                if ( ++currentTypeIndex < rels.length )
                {
                    currentTypeIterator = rels[currentTypeIndex];
                }
                else if ( (status = fromNode.getMoreRelationships( nodeManager )).loaded()
                        // This is here to guard for that someone else might have loaded
                        // stuff in this relationship chain (and exhausted it) while I
                        // iterated over my batch of relationships. It will only happen
                        // for nodes which have more than <grab size> relationships and
                        // isn't fully loaded when starting iterating.
                        || lastTimeILookedThereWasMoreToLoad )
                {
                    lastTimeILookedThereWasMoreToLoad = status.hasMoreToLoad();
                    Map<Integer,RelIdIterator> newRels = new HashMap<Integer,RelIdIterator>();
                    for ( RelIdIterator itr : rels )
                    {
                        int type = itr.getType();
                        RelIdArray newSrc = fromNode.getRelationshipIds( type );
                        if ( newSrc != null )
                        {
                            itr = itr.updateSource( newSrc, direction );
                            itr.doAnotherRound();
                        }
                        newRels.put( type, itr );
                    }
                    
                    // If we wanted relationships of any type check if there are
                    // any new relationship types loaded for this node and if so
                    // initiate iterators for them
                    if ( allTypes )
                    {
                        ArrayMap<Integer, Collection<Long>> skipMap = nodeManager.getTransactionState().
                                getCowRelationshipRemoveMap( fromNode );
                        for ( RelIdArray ids : fromNode.getRelationshipIds() )
                        {
                            int type = ids.getType();
                            RelIdIterator itr = newRels.get( type );
                            if ( itr == null )
                            {
                                Collection<Long> remove = skipMap != null ? skipMap.get( type ) : null;
                                itr = remove == null ? ids.iterator( direction ) :
                                        RelIdArray.from( ids, null, remove ).iterator( direction );
                                newRels.put( type, itr );
                            }
                            else
                            {
                                itr = itr.updateSource( ids, direction );
                                newRels.put( type, itr );
                            }
                        }
                    }
                    
                    initializeRels( newRels.values().toArray( new RelIdIterator[newRels.size()] ) );
                    currentTypeIterator = rels[currentTypeIndex];
                }
                else
                {
                    break;
                }
            }
        } while ( currentTypeIterator.hasNext() );
        // no next element found
        return null;
    }
}
