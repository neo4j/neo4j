/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.RelationshipVisitor.Home;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.VersionedHashMap;
import org.neo4j.storageengine.api.Direction;

import static org.neo4j.collection.primitive.PrimitiveIntCollections.iterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;

/**
 * Maintains relationships that have been added for a specific node.
 * <p/>
 * This class is not a trustworthy source of information unless you are careful - it does not, for instance, remove
 * rels if they are added and then removed in the same tx. It trusts wrapping data structures for that filtering.
 */
public class RelationshipChangesForNode
{
    /**
     * Allows this data structure to work both for tracking removals and additions.
     */
    public enum DiffStrategy
    {
        REMOVE
                {
                    @Override
                    int augmentDegree( int degree, int diff )
                    {
                        return degree - diff;
                    }

                    @Override
                    RelationshipIterator augmentPrimitiveIterator( RelationshipIterator original,
                            Iterator<Set<Long>> diff, RelationshipVisitor.Home txStateRelationshipHome )
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    PrimitiveLongIterator getPrimitiveIterator( Iterator<Set<Long>> diff,
                            RelationshipVisitor.Home txStateRelationshipHome )
                    {
                        throw new UnsupportedOperationException();
                    }
                },
        ADD
                {
                    @Override
                    int augmentDegree( int degree, int diff )
                    {
                        return degree + diff;
                    }

                    @Override
                    RelationshipIterator augmentPrimitiveIterator( final RelationshipIterator original,
                            final Iterator<Set<Long>> diff, final RelationshipVisitor.Home txStateRelationshipHome )
                    {
                        if ( !diff.hasNext() )
                        {
                            return original;
                        }

                        return new RelationshipIterator()
                        {
                            private Iterator<Long> currentSetOfAddedRels;

                            @Override
                            public boolean hasNext()
                            {
                                return original.hasNext() || (currentSetOfAddedRels().hasNext());
                            }

                            private Iterator<Long> currentSetOfAddedRels()
                            {
                                while ( diff.hasNext() && (currentSetOfAddedRels == null || !currentSetOfAddedRels
                                        .hasNext()) )
                                {
                                    currentSetOfAddedRels = diff.next().iterator();
                                }
                                return currentSetOfAddedRels;
                            }

                            @Override
                            public long next()
                            {
                                return original.hasNext() ? original.next() : currentSetOfAddedRels().next();
                            }

                            @Override
                            public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                                    RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
                            {
                                RelationshipVisitor.Home home = currentSetOfAddedRels != null ?
                                        txStateRelationshipHome : original;
                                return home.relationshipVisit( relationshipId, visitor );
                            }
                        };
                    }

                    @Override
                    PrimitiveLongIterator getPrimitiveIterator( final Iterator<Set<Long>> diff,
                            RelationshipVisitor.Home txStateRelationshipHome )
                    {
                        if ( !diff.hasNext() )
                        {
                            return PrimitiveLongCollections.emptyIterator();
                        }

                        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
                        {
                            private Iterator<Long> currentSetOfAddedRels;

                            @Override
                            protected boolean fetchNext()
                            {
                                Iterator<Long> iterator = currentSetOfAddedRels();
                                return iterator.hasNext() ? next( iterator.next() ) : false;
                            }

                            private Iterator<Long> currentSetOfAddedRels()
                            {
                                while ( diff.hasNext() && (currentSetOfAddedRels == null || !currentSetOfAddedRels
                                        .hasNext()) )
                                {
                                    currentSetOfAddedRels = diff.next().iterator();
                                }
                                return currentSetOfAddedRels;
                            }
                        };
                    }
                };

        abstract int augmentDegree( int degree, int diff );

        abstract RelationshipIterator augmentPrimitiveIterator( RelationshipIterator original,
                Iterator<Set<Long>> diff, RelationshipVisitor.Home txStateRelationshipHome );

        abstract PrimitiveLongIterator getPrimitiveIterator(
                Iterator<Set<Long>> diff, RelationshipVisitor.Home txStateRelationshipHome );
    }

    private final DiffStrategy diffStrategy;
    private final Home relationshipHome;

    private Map<Integer /* Type */, Set<Long /* Id */>> outgoing;
    private Map<Integer /* Type */, Set<Long /* Id */>> incoming;
    private Map<Integer /* Type */, Set<Long /* Id */>> loops;

    private int totalOutgoing = 0;
    private int totalIncoming = 0;
    private int totalLoops = 0;

    public RelationshipChangesForNode( DiffStrategy diffStrategy, RelationshipVisitor.Home relationshipHome )
    {
        this.diffStrategy = diffStrategy;
        this.relationshipHome = relationshipHome;
    }

    public void addRelationship( long relId, int typeId, Direction direction )
    {
        Map<Integer, Set<Long>> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        Set<Long> rels = relTypeToRelsMap.get( typeId );
        if ( rels == null )
        {
            rels = Collections.newSetFromMap( new VersionedHashMap<Long, Boolean>() );
            relTypeToRelsMap.put( typeId, rels );
        }

        rels.add( relId );

        switch ( direction )
        {
            case INCOMING:
                totalIncoming++;
                break;
            case OUTGOING:
                totalOutgoing++;
                break;
            case BOTH:
                totalLoops++;
                break;
            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    public boolean removeRelationship( long relId, int typeId, Direction direction )
    {
        Map<Integer, Set<Long>> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        Set<Long> rels = relTypeToRelsMap.get( typeId );
        if ( rels != null )
        {
            if ( rels.remove( relId ) )
            {
                if ( rels.isEmpty() )
                {
                    relTypeToRelsMap.remove( typeId );
                }

                switch ( direction )
                {
                    case INCOMING:
                        totalIncoming--;
                        break;
                    case OUTGOING:
                        totalOutgoing--;
                        break;
                    case BOTH:
                        totalLoops--;
                        break;
                    default:
                        throw new IllegalArgumentException( "Unknown direction: " + direction );
                }
                return true;
            }
        }
        return false;
    }

    public RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels )
    {
        return augmentRelationships( direction, rels, ALL_TYPES );
    }

    public RelationshipIterator augmentRelationships( Direction direction, int[] types, RelationshipIterator rels )
    {
        return augmentRelationships( direction, rels, typeFilter( types ) );
    }

    public RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels,
            Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>> typeFilter )
    {
        switch ( direction )
        {
            case INCOMING:
                if ( incoming != null && !incoming.isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( incoming ),
                            relationshipHome );
                }
                break;
            case OUTGOING:
                if ( outgoing != null && !outgoing.isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( outgoing ),
                            relationshipHome );
                }
                break;
            case BOTH:
                if ( outgoing != null && !outgoing.isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( outgoing ),
                            relationshipHome );
                }
                if ( incoming != null && !incoming.isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( incoming ),
                            relationshipHome );
                }
                break;

            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        // Loops are always included
        if ( loops != null && !loops.isEmpty() )
        {
            rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( loops ), relationshipHome );
        }

        return rels;
    }

    public int augmentDegree( Direction direction, int degree )
    {
        switch ( direction )
        {
            case INCOMING:
                return diffStrategy.augmentDegree( degree, totalIncoming + totalLoops );
            case OUTGOING:
                return diffStrategy.augmentDegree( degree, totalOutgoing + totalLoops );
            default:
                return diffStrategy.augmentDegree( degree, totalIncoming + totalOutgoing + totalLoops );
        }
    }

    public int augmentDegree( Direction direction, int degree, int typeId )
    {
        switch ( direction )
        {
            case INCOMING:
                if ( incoming != null && incoming.containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, incoming.get( typeId ).size() );
                }
                break;
            case OUTGOING:
                if ( outgoing != null && outgoing.containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, outgoing.get( typeId ).size() );
                }
                break;
            case BOTH:
                if ( outgoing != null && outgoing.containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, outgoing.get( typeId ).size() );
                }
                if ( incoming != null && incoming.containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, incoming.get( typeId ).size() );
                }
                break;

            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        // Loops are always included
        if ( loops != null && loops.containsKey( typeId ) )
        {
            degree = diffStrategy.augmentDegree( degree, loops.get( typeId ).size() );
        }
        return degree;
    }

    public PrimitiveIntIterator relationshipTypes()
    {
        Set<Integer> types = new HashSet<>();
        if ( outgoing != null && !outgoing.isEmpty() )
        {
            types.addAll( outgoing.keySet() );
        }
        if ( incoming != null && !incoming.isEmpty() )
        {
            types.addAll( incoming.keySet() );
        }
        if ( loops != null && !loops.isEmpty() )
        {
            types.addAll( loops.keySet() );
        }
        return PrimitiveIntCollections.toPrimitiveIterator( types.iterator() );
    }

    public void clear()
    {
        if ( outgoing != null )
        {
            outgoing.clear();
        }
        if ( incoming != null )
        {
            incoming.clear();
        }
        if ( loops != null )
        {
            loops.clear();
        }
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> outgoing()
    {
        if ( outgoing == null )
        {
            outgoing = new VersionedHashMap<>();
        }
        return outgoing;
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> incoming()
    {
        if ( incoming == null )
        {
            incoming = new VersionedHashMap<>();
        }
        return incoming;
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> loops()
    {
        if ( loops == null )
        {
            loops = new VersionedHashMap<>();
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
            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
        return relTypeToRelsMap;
    }

    private Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>> typeFilter( final int[] types )
    {
        return relationshipsByType -> new PrefetchingIterator<Set<Long>>()
        {
            private final PrimitiveIntIterator iterTypes = iterator( types );

            @Override
            protected Set<Long> fetchNextOrNull()
            {
                while ( iterTypes.hasNext() )
                {
                    Set<Long> relsByType = relationshipsByType.get( iterTypes.next() );
                    if ( relsByType != null )
                    {
                        return relsByType;
                    }
                }
                return null;
            }
        };
    }

    private static final Function<Map<Integer, Set<Long>>, Iterator<Set<Long>>> ALL_TYPES =
            integerSetMap -> integerSetMap.values().iterator();

    private Iterator<Set<Long>> diffs( Function<Map<Integer,Set<Long>>,Iterator<Set<Long>>> filter,
            Map<Integer,Set<Long>>... maps )
    {
        Collection<Set<Long>> result = new ArrayList<>();
        for ( int i = 0; i < maps.length; i++ )
        {
            Map<Integer,Set<Long>> map = maps[i];
            if ( map != null )
            {
                Iterator<Set<Long>> diffSet = filter.apply( map );
                while ( diffSet.hasNext() )
                {
                    result.add( diffSet.next() );
                }
            }
        }
        return result.iterator();
    }

    public PrimitiveLongIterator getRelationships( Direction direction )
    {
        return getRelationships( direction, ALL_TYPES );
    }

    public PrimitiveLongIterator getRelationships( Direction direction, int[] types )
    {
        return getRelationships( direction, typeFilter( types ) );
    }

    private PrimitiveLongIterator getRelationships( Direction direction,
            Function<Map<Integer,Set<Long>>,Iterator<Set<Long>>> types )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming != null || loops != null ? diffStrategy.getPrimitiveIterator(
                    diffs( types, incoming, loops ), relationshipHome ) : emptyIterator();
        case OUTGOING:
            return outgoing != null || loops != null ? diffStrategy.getPrimitiveIterator(
                    diffs( types, outgoing, loops ), relationshipHome ) : emptyIterator();
        case BOTH:
            return outgoing != null || incoming != null || loops != null ? diffStrategy.getPrimitiveIterator(
                    diffs( types, outgoing, incoming, loops ), relationshipHome ) : emptyIterator();
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }
}
