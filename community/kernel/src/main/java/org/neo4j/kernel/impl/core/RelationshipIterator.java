/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.store.CacheUpdateListener;
import org.neo4j.kernel.impl.core.NodeImpl.LoadStatus;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdIterator;

class RelationshipIterator implements PrimitiveLongIterator
{
    private RelIdIterator[] rels;
    private int currentTypeIndex;
    private final NodeImpl fromNode;
    private final DirectionWrapper direction;
    private final RelationshipLoader relationshipLoader;

    private boolean lastTimeILookedThereWasMoreToLoad;
    private final boolean allTypes;
    private final int[] types;

    private boolean nextHasBeenComputed = false;
    private boolean hasNext;
    private long nextElement;
    private final CacheUpdateListener cacheUpdateListener;

    RelationshipIterator( RelIdIterator[] rels, NodeImpl fromNode,
        DirectionWrapper direction, int[] types, RelationshipLoader relationshipLoader,
        boolean hasMoreToLoad, boolean allTypes, CacheUpdateListener cacheUpdateListener )
    {
        this.cacheUpdateListener = cacheUpdateListener;
        initializeRels( rels );
        this.lastTimeILookedThereWasMoreToLoad = hasMoreToLoad;
        this.fromNode = fromNode;
        this.direction = direction;
        this.types = types;
        this.relationshipLoader = relationshipLoader;
        this.allTypes = allTypes;
    }

    private void initializeRels( RelIdIterator[] rels )
    {
        this.rels = rels;
        this.currentTypeIndex = 0;
    }

    @Override
    public boolean hasNext()
    {
        if(!nextHasBeenComputed)
        {
            nextHasBeenComputed = true;
            computeNext();
        }
        return hasNext;
    }

    @Override
    public long next()
    {
        if(!hasNext())
        {
            throw new NoSuchElementException();
        }
        nextHasBeenComputed = false;
        return nextElement;
    }

    protected void computeNext()
    {
        RelIdIterator currentTypeIterator = rels[currentTypeIndex];
        do
        {
            if ( currentTypeIterator.hasNext() )
            {
                // There are more relationships loaded of this relationship type, let's return it
                nextElement = currentTypeIterator.next();
                hasNext = true;
                return;
            }

            LoadStatus status;
            while ( !currentTypeIterator.hasNext() )
            {
                // There aren't any more relationships loaded of this relationship type
                if ( currentTypeIndex+1 < rels.length )
                {
                    // There are other relationship types to try to get relationships from, go to the next type
                    currentTypeIterator = rels[++currentTypeIndex];
                }
                else if ( (status = fromNode.getMoreRelationships( relationshipLoader, direction, types,
                        cacheUpdateListener )).loaded()
                        // This is here to guard for that someone else might have loaded
                        // stuff in this relationship chain (and exhausted it) while I
                        // iterated over my batch of relationships. It will only happen
                        // for nodes which have more than <grab size> relationships and
                        // isn't fully loaded when starting iterating.
                        || lastTimeILookedThereWasMoreToLoad )
                {
                    // There aren't any more relationship types to try to get relationships from,
                    // but it's likely there are more relationships to load for this node,
                    // so try to go and load more relationships
                    lastTimeILookedThereWasMoreToLoad = status.hasMoreToLoad();
                    Map<Integer,RelIdIterator> newRels = new HashMap<>();
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
                        for ( RelIdArray ids : fromNode.getRelationshipIds() )
                        {
                            int type = ids.getType();
                            RelIdIterator itr = newRels.get( type );
                            if ( itr == null )
                            {
                                itr = ids.iterator( direction );
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
                    // There aren't any more relationship types to try to get relationships from
                    // and there are no more relationships to load for this node
                    break;
                }
            }
        } while ( currentTypeIterator.hasNext() );

        hasNext = false;
    }
}
