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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.kernel.impl.newapi.RelationshipDirection;
import org.neo4j.storageengine.api.Direction;

import static org.neo4j.collection.PrimitiveLongCollections.toPrimitiveIterator;

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
                    LongIterator getPrimitiveIterator( Iterator<Set<Long>> diff )
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
                    LongIterator getPrimitiveIterator( final Iterator<Set<Long>> diff )
                    {
                        if ( !diff.hasNext() )
                        {
                            return ImmutableEmptyLongIterator.INSTANCE;
                        }

                        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
                        {
                            private Iterator<Long> currentSetOfAddedRels;

                            @Override
                            protected boolean fetchNext()
                            {
                                Iterator<Long> iterator = currentSetOfAddedRels();
                                return iterator.hasNext() && next( iterator.next() );
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

        abstract LongIterator getPrimitiveIterator( Iterator<Set<Long>> diff );
    }

    private final DiffStrategy diffStrategy;

    private Map<Integer /* Type */, Set<Long /* Id */>> outgoing;
    private Map<Integer /* Type */, Set<Long /* Id */>> incoming;
    private Map<Integer /* Type */, Set<Long /* Id */>> loops;

    private int totalOutgoing;
    private int totalIncoming;
    private int totalLoops;

    public RelationshipChangesForNode( DiffStrategy diffStrategy )
    {
        this.diffStrategy = diffStrategy;
    }

    public void addRelationship( long relId, int typeId, Direction direction )
    {
        Map<Integer, Set<Long>> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        Set<Long> rels = relTypeToRelsMap.computeIfAbsent( typeId, k -> new HashSet<>() );

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

    public int augmentDegree( RelationshipDirection direction, int degree, int typeId )
    {
        switch ( direction )
        {
        case INCOMING:
            if ( incoming != null && incoming.containsKey( typeId ) )
            {
                return diffStrategy.augmentDegree( degree, incoming.get( typeId ).size() );
            }
            break;
        case OUTGOING:
            if ( outgoing != null && outgoing.containsKey( typeId ) )
            {
                return diffStrategy.augmentDegree( degree, outgoing.get( typeId ).size() );
            }
            break;
        case LOOP:
            if ( loops != null && loops.containsKey( typeId ) )
            {
                return diffStrategy.augmentDegree( degree, loops.get( typeId ).size() );
            }
            break;

        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        return degree;
    }

    public MutableIntSet relationshipTypes()
    {
        final MutableIntSet types = new IntHashSet();
        if ( outgoing != null && !outgoing.isEmpty() )
        {
            outgoing.keySet().forEach( types::add );
        }
        if ( incoming != null && !incoming.isEmpty() )
        {
            incoming.keySet().forEach( types::add );
        }
        if ( loops != null && !loops.isEmpty() )
        {
            loops.keySet().forEach( types::add );
        }
        return types;
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
            outgoing = new HashMap<>();
        }
        return outgoing;
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> incoming()
    {
        if ( incoming == null )
        {
            incoming = new HashMap<>();
        }
        return incoming;
    }

    private Map<Integer /* Type */, Set<Long /* Id */>> loops()
    {
        if ( loops == null )
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
            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
        return relTypeToRelsMap;
    }

    private Iterator<Set<Long>> diffs( Function<Map<Integer,Set<Long>>,Iterator<Set<Long>>> filter,
            Map<Integer,Set<Long>>... maps )
    {
        Collection<Set<Long>> result = new ArrayList<>();
        for ( Map<Integer,Set<Long>> map : maps )
        {
            if ( map != null )
            {
                Iterator<Set<Long>> diffSet = filter.apply( map );
                while ( diffSet.hasNext() )
                {
                    result.add( new HashSet<>( diffSet.next() ) );
                }
            }
        }
        return result.iterator();
    }

    public LongIterator getRelationships()
    {
        return PrimitiveLongCollections.concat(
                primitiveIds( incoming ),
                primitiveIds( outgoing ),
                primitiveIds( loops ) );
    }

    public LongIterator getRelationships( RelationshipDirection direction, int type )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming != null ? primitiveIdsByType( incoming, type ) : ImmutableEmptyLongIterator.INSTANCE;
        case OUTGOING:
            return outgoing != null ? primitiveIdsByType( outgoing, type ) : ImmutableEmptyLongIterator.INSTANCE;
        case LOOP:
            return loops != null ? primitiveIdsByType( loops, type ) : ImmutableEmptyLongIterator.INSTANCE;
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    private LongIterator primitiveIds( Map<Integer, Set<Long>> map )
    {
        if ( map == null )
        {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        final int size = map.values().stream().mapToInt( Set::size ).sum();
        final Set<Long> ids = new HashSet<>( size );
        map.values().forEach( ids::addAll );
        return toPrimitiveIterator( ids.iterator() );
    }

    private LongIterator primitiveIdsByType( Map<Integer, Set<Long>> map, int type )
    {
        Set<Long> relationships = map.get( type );
        return relationships == null ? ImmutableEmptyLongIterator.INSTANCE : toPrimitiveIterator( new HashSet<>( relationships ).iterator() );
    }
}
