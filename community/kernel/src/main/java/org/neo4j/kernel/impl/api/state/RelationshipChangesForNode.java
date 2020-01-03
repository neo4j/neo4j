/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.storageengine.api.RelationshipDirection;

import static java.lang.Math.toIntExact;

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
                },
        ADD
                {
                    @Override
                    int augmentDegree( int degree, int diff )
                    {
                        return degree + diff;
                    }
                };

        abstract int augmentDegree( int degree, int diff );

    }

    private final DiffStrategy diffStrategy;

    private MutableIntObjectMap<MutableLongSet> outgoing;
    private MutableIntObjectMap<MutableLongSet> incoming;
    private MutableIntObjectMap<MutableLongSet> loops;

    public RelationshipChangesForNode( DiffStrategy diffStrategy )
    {
        this.diffStrategy = diffStrategy;
    }

    public void addRelationship( long relId, int typeId, RelationshipDirection direction )
    {
        final MutableIntObjectMap<MutableLongSet> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        final MutableLongSet rels = relTypeToRelsMap.getIfAbsentPut( typeId, LongHashSet::new );

        rels.add( relId );
    }

    public boolean removeRelationship( long relId, int typeId, RelationshipDirection direction )
    {
        final MutableIntObjectMap<MutableLongSet> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        final MutableLongSet rels = relTypeToRelsMap.get( typeId );
        if ( rels != null && rels.remove( relId ) )
        {
            if ( rels.isEmpty() )
            {
                relTypeToRelsMap.remove( typeId );
            }
            return true;
        }
        return false;
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

    private MutableIntObjectMap<MutableLongSet> outgoing()
    {
        if ( outgoing == null )
        {
            outgoing = new IntObjectHashMap<>();
        }
        return outgoing;
    }

    private MutableIntObjectMap<MutableLongSet> incoming()
    {
        if ( incoming == null )
        {
            incoming = new IntObjectHashMap<>();
        }
        return incoming;
    }

    private MutableIntObjectMap<MutableLongSet> loops()
    {
        if ( loops == null )
        {
            loops = new IntObjectHashMap<>();
        }
        return loops;
    }

    private MutableIntObjectMap<MutableLongSet> getTypeToRelMapForDirection( RelationshipDirection direction )
    {
        final MutableIntObjectMap<MutableLongSet> relTypeToRelsMap;
        switch ( direction )
        {
            case INCOMING:
                relTypeToRelsMap = incoming();
                break;
            case OUTGOING:
                relTypeToRelsMap = outgoing();
                break;
            case LOOP:
                relTypeToRelsMap = loops();
                break;
            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
        return relTypeToRelsMap;
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

    private static LongIterator primitiveIds( IntObjectMap<MutableLongSet> map )
    {
        if ( map == null )
        {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        final int size = toIntExact( map.sumOfInt( LongSet::size ) );
        final MutableLongSet ids = new LongHashSet( size );
        map.values().forEach( ids::addAll );
        return ids.longIterator();
    }

    private static LongIterator primitiveIdsByType( IntObjectMap<MutableLongSet> map, int type )
    {
        final LongSet relationships = map.get( type );
        return relationships == null ? ImmutableEmptyLongIterator.INSTANCE : relationships.freeze().longIterator();
    }
}
