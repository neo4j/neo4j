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
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.graphdb.Direction;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;

import static java.lang.Math.toIntExact;
import static org.neo4j.collection.PrimitiveLongCollections.concat;
import static org.neo4j.internal.helpers.collection.Iterators.filter;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

/**
 * Maintains relationships that have been added for a specific node.
 * <p/>
 * This class is not a trustworthy source of information unless you are careful - it does not, for instance, remove
 * rels if they are added and then removed in the same tx. It trusts wrapping data structures for that filtering.
 */
public class RelationshipChangesForNode
{
    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( RelationshipChangesForNode.class );

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
    private final MemoryTracker memoryTracker;

    private MutableIntObjectMap<MutableLongSet> outgoing;
    private MutableIntObjectMap<MutableLongSet> incoming;
    private MutableIntObjectMap<MutableLongSet> loops;

    static RelationshipChangesForNode createRelationshipChangesForNode( DiffStrategy diffStrategy, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE );
        return new RelationshipChangesForNode( diffStrategy, memoryTracker );
    }

    private RelationshipChangesForNode( DiffStrategy diffStrategy, MemoryTracker memoryTracker )
    {
        this.diffStrategy = diffStrategy;
        this.memoryTracker = memoryTracker;
    }

    public void addRelationship( long relId, int typeId, RelationshipDirection direction )
    {
        final MutableIntObjectMap<MutableLongSet> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        final MutableLongSet rels = relTypeToRelsMap.getIfAbsentPut( typeId, () -> HeapTrackingCollections.newLongSet( memoryTracker ) );

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
            return diffStrategy.augmentDegree( degree, degreeDiff( typeId, incoming ) );
        case OUTGOING:
            return diffStrategy.augmentDegree( degree, degreeDiff( typeId, outgoing ) );
        case LOOP:
            return diffStrategy.augmentDegree( degree, degreeDiff( typeId, loops ) );
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    private int degreeDiff( int type, MutableIntObjectMap<MutableLongSet> map )
    {
        if ( map != null )
        {
            MutableLongSet set = map.get( type );
            if ( set != null )
            {
                return set.size();
            }
        }
        return 0;
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
    }

    private MutableIntObjectMap<MutableLongSet> outgoing()
    {
        if ( outgoing == null )
        {
            outgoing = HeapTrackingCollections.newIntObjectHashMap( memoryTracker );
        }
        return outgoing;
    }

    private MutableIntObjectMap<MutableLongSet> incoming()
    {
        if ( incoming == null )
        {
            incoming = HeapTrackingCollections.newIntObjectHashMap( memoryTracker );
        }
        return incoming;
    }

    private MutableIntObjectMap<MutableLongSet> loops()
    {
        if ( loops == null )
        {
            loops = HeapTrackingCollections.newIntObjectHashMap( memoryTracker );
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
        return nonEmptyConcat(
                primitiveIds( incoming ),
                primitiveIds( outgoing ),
                primitiveIds( loops ) );
    }

    private LongIterator nonEmptyConcat( LongIterator... primitiveIds )
    {
        return concat( filter( ids -> ids != ImmutableEmptyLongIterator.INSTANCE, iterator( primitiveIds ) ) );
    }

    public LongIterator getRelationships( Direction direction )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming == null && loops == null ? ImmutableEmptyLongIterator.INSTANCE : nonEmptyConcat( primitiveIds( incoming ), primitiveIds( loops ) );
        case OUTGOING:
            return outgoing == null && loops == null ? ImmutableEmptyLongIterator.INSTANCE : nonEmptyConcat( primitiveIds( outgoing ), primitiveIds( loops ) );
        case BOTH:
            return getRelationships();
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    public LongIterator getRelationships( Direction direction, int type )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming == null && loops == null ? ImmutableEmptyLongIterator.INSTANCE :
                   nonEmptyConcat( primitiveIdsByType( incoming, type ), primitiveIdsByType( loops, type ) );
        case OUTGOING:
            return outgoing == null && loops == null ? ImmutableEmptyLongIterator.INSTANCE :
                   nonEmptyConcat( primitiveIdsByType( outgoing, type ), primitiveIdsByType( loops, type ) );
        case BOTH:
            return nonEmptyConcat( primitiveIdsByType( outgoing, type ), primitiveIdsByType( incoming, type ), primitiveIdsByType( loops, type ) );
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    public IntSet relationshipTypes()
    {
        MutableIntSet types = IntSets.mutable.empty();
        addRelationshipTypes( types, outgoing );
        addRelationshipTypes( types, incoming );
        addRelationshipTypes( types, loops );
        return types;
    }

    private void addRelationshipTypes( MutableIntSet types, MutableIntObjectMap<MutableLongSet> relationships )
    {
        if ( relationships != null )
        {
            types.addAll( relationships.keySet() );
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
        if ( map == null )
        {
            return ImmutableEmptyLongIterator.INSTANCE;
        }
        final LongSet relationships = map.get( type );
        return relationships == null ? ImmutableEmptyLongIterator.INSTANCE : relationships.freeze().longIterator();
    }
}
