/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.util.PrimitiveIntIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

/**
 * Maintains relationships that have been added for a specific node.
 *
 * This class is not a trustworthy source of information unless you are careful - it does not, for instance, remove
 * rels if they are added and then removed in the same tx. It trusts wrapping data structures for that filtering.
 */
public class RelationshipsAddedToNode
{
    private Map<Integer /* Type */, Set<Long /* Id */>> outgoing;
    private Map<Integer /* Type */, Set<Long /* Id */>> incoming;
    private Map<Integer /* Type */, Set<Long /* Id */>> loops;

    public void addRelationship( long relId, int typeId, Direction direction )
    {
        Map<Integer, Set<Long>> relTypeToRelsMap = getTypeToRelMapForDirection( direction );

        Set<Long> rels = relTypeToRelsMap.get( typeId );
        if(rels == null)
        {
            rels = new HashSet<>();
            relTypeToRelsMap.put( typeId, rels );
        }

        rels.add( relId );
    }

    public PrimitiveLongIterator augmentRelationships( Direction direction, PrimitiveLongIterator rels )
    {
        return augmentRelationships( direction, rels, ALL_TYPES );
    }

    public PrimitiveLongIterator augmentRelationships( Direction direction, int[] types, PrimitiveLongIterator rels )
    {
        return augmentRelationships( direction, rels, typeFilter(types) );
    }

    public PrimitiveLongIterator augmentRelationships( Direction direction, PrimitiveLongIterator rels,
                                                       Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>> typeFilter  )
    {
        switch ( direction )
        {
            case INCOMING:
                if(incoming != null)
                {
                    rels = augmentPrimitiveIterator( rels, typeFilter.apply( incoming() ) );
                }
                break;
            case OUTGOING:
                if(outgoing != null)
                {
                    rels = augmentPrimitiveIterator( rels, typeFilter.apply( outgoing() ) );
                }
                break;
            case BOTH:
                if(outgoing != null)
                {
                    rels = augmentPrimitiveIterator( rels, typeFilter.apply( outgoing() ) );
                }
                if(incoming != null)
                {
                    rels = augmentPrimitiveIterator( rels, typeFilter.apply( incoming() ) );
                }
                break;
        }

        // Loops are always included
        if(loops != null)
        {
            rels = augmentPrimitiveIterator( rels, loops().values().iterator() );
        }

        return rels;
    }

    private PrimitiveLongIterator augmentPrimitiveIterator( final PrimitiveLongIterator rels,
                                                            final Iterator<Set<Long>> values )
    {
        if(!values.hasNext())
        {
            return rels;
        }

        return new PrimitiveLongIterator()
        {
            private Iterator<Long> currentSetOfAddedRels;

            @Override
            public boolean hasNext()
            {
                return rels.hasNext() || (currentSetOfAddedRels().hasNext());
            }

            private Iterator<Long> currentSetOfAddedRels()
            {
                while(values.hasNext() && (currentSetOfAddedRels == null || !currentSetOfAddedRels.hasNext()))
                {
                    currentSetOfAddedRels = values.next().iterator();
                }
                return currentSetOfAddedRels;
            }

            @Override
            public long next()
            {
                if(rels.hasNext())
                {
                    long next = rels.next();
                    return next;
                }
                else
                {
                    return currentSetOfAddedRels().next();
                }
            }
        };
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> outgoing()
    {
        if(outgoing == null)
        {
            outgoing = new HashMap<>();
        }
        return outgoing;
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> incoming()
    {
        if(incoming == null)
        {
            incoming = new HashMap<>();
        }
        return incoming;
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> loops()
    {
        if(loops == null)
        {
            loops = new HashMap<>();
        }
        return loops;
    }

    private Map<Integer, Set<Long>> getTypeToRelMapForDirection( Direction direction )
    {
        Map<Integer /* Type */, Set<Long /* Id */>> relTypeToRelsMap = null;
        switch ( direction )
        {
            case INCOMING:
                relTypeToRelsMap = incoming();
                break;
            case OUTGOING:
                relTypeToRelsMap = outgoing();
                break;
            case BOTH:
                relTypeToRelsMap = loops();
                break;
        }
        return relTypeToRelsMap;
    }

    private Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>> typeFilter( final int[] types )
    {
        return new Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>>()
        {
            @Override
            public Iterator<Set<Long>> apply( final Map<Integer, Set<Long>> relationshipsByType )
            {
                return new PrefetchingIterator<Set<Long>>()
                {
                    private PrimitiveIntIterator iterTypes = IteratorUtil.asPrimitiveIterator(types);

                    @Override
                    protected Set<Long> fetchNextOrNull()
                    {
                        while(iterTypes.hasNext())
                        {
                            Set<Long> relsByType = relationshipsByType.get(iterTypes.next());
                            if(relsByType != null)
                            {
                                return relsByType;
                            }
                        }
                        return null;
                    }
                };
            }
        };
    }

    private static final Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>> ALL_TYPES
            = new Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>>()
    {
        @Override
        public Iterator<Set<Long>> apply( Map<Integer, Set<Long>> integerSetMap )
        {
            return integerSetMap.values().iterator();
        }
    };
}
