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
package org.neo4j.kernel.impl.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.CacheLoader;
import org.neo4j.kernel.impl.api.store.CacheUpdateListener;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.xa.PropertyLoader;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdIterator;

import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;

import static org.neo4j.kernel.impl.cache.SizeOfs.REFERENCE_SIZE;
import static org.neo4j.kernel.impl.cache.SizeOfs.sizeOfArray;
import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverheadIncludingReferences;
import static org.neo4j.kernel.impl.util.RelIdArray.empty;
import static org.neo4j.kernel.impl.util.RelIdArray.wrap;

/**
 * This class currently has multiple responsibilities, and a very complex set of interrelationships with the world
 * around it. It is being refactored, such that this will become a pure cache object, and be renamed eg. CachedNode.
 *
 * Responsibilities inside this class are slowly being moved over to {@link org.neo4j.kernel.impl.api.Kernel} and its
 * friends.
 */
public class NodeImpl extends ArrayBasedPrimitive
{
    /* relationships[] being null means: not even tried to load any relationships - go ahead and load
     * relationships[] being NO_RELATIONSHIPS means: don't bother loading relationships since there aren't any */
    private static final RelIdArray[] NO_RELATIONSHIPS = new RelIdArray[0];
    static final int[] NO_RELATIONSHIP_TYPES = new int[0];

    private volatile RelIdArray[] relationships;

    // Sorted array
    private volatile int[] labels;
    /*
     * This contains the id of the next relationship to load from disk.
     */
    private volatile RelationshipLoadingPosition relChainPosition;
    private final long id;

    // newNode will only be true for NodeManager.createNode
    public NodeImpl( long id )
    {
        this.id = id;
    }

    @Override
    protected Iterator<DefinedProperty> loadProperties( PropertyLoader loader )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        loader.nodeLoadProperties( id, receiver );
        return receiver;
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        int size = super.sizeOfObjectInBytesIncludingOverhead() +
                REFERENCE_SIZE/*relationships reference*/ +
                8/*relChainPosition*/ + 8/*id*/ +
                REFERENCE_SIZE/*labels reference*/;
        if ( relationships != null && relationships.length > 0 )
        {
            size = withArrayOverheadIncludingReferences( size, relationships.length );
            for ( RelIdArray array : relationships )
            {
                size += array.sizeOfObjectInBytesIncludingOverhead();
            }
        }
        if ( labels != null && labels.length > 0 )
        {
            size += sizeOfArray( labels );
        }
        return size;
    }

    @Override
    public int hashCode()
    {
        long id = getId();
        return (int) ((id >>> 32) ^ id);
    }

    @Override
    public boolean equals( Object obj )
    {
        return this == obj || (obj instanceof NodeImpl && ((NodeImpl) obj).getId() == getId());
    }

    PrimitiveLongIterator getAllRelationships( RelationshipLoader relationshipLoader, DirectionWrapper direction,
            CacheUpdateListener cacheUpdateListener )
    {
        ensureRelationshipMapNotNull( relationshipLoader, direction, NO_RELATIONSHIP_TYPES, cacheUpdateListener );

        // We need to check if there are more relationships to load before grabbing
        // the references to the RelIdArrays since otherwise there could be
        // another concurrent thread exhausting the chain position in between the point
        // where we got an empty iterator for a type that the other thread loaded and
        // the point where we check whether or not there are more relationships to load.
        boolean hasMore = hasMoreRelationshipsToLoad( direction, NO_RELATIONSHIP_TYPES );

        int numberOfRelIdArrays = relationships.length;
        RelIdIterator[] result = new RelIdIterator[numberOfRelIdArrays];

        for ( int i = 0; i < numberOfRelIdArrays; i++ )
        {
            RelIdArray src = relationships[i];
            RelIdIterator iterator = src.iterator( direction );
            result[i] = iterator;
        }

        if ( result.length == 0 )
        {
            return PrimitiveLongCollections.emptyIterator();
        }
        return new RelationshipIterator( result, this, direction, NO_RELATIONSHIP_TYPES,
                relationshipLoader, hasMore, true, cacheUpdateListener );
    }

    PrimitiveLongIterator getAllRelationshipsOfType( RelationshipLoader relationshipLoader,
            DirectionWrapper direction, int[] types, CacheUpdateListener cacheUpdateListener )
    {
        ensureRelationshipMapNotNull( relationshipLoader, direction, types, cacheUpdateListener );

        // We need to check if there are more relationships to load before grabbing
        // the references to the RelIdArrays. Otherwise there could be
        // another concurrent thread exhausting the chain position in between the point
        // where we got an empty iterator for a type that the other thread loaded and
        // the point where we check if there are more relationships to load.
        boolean hasMore = hasMoreRelationshipsToLoad( direction, types );

        RelIdIterator[] result = new RelIdIterator[types.length];

        int actualLength = 0;
        for ( int typeId : types )
        {
            if ( typeId == TokenHolder.NO_ID || typeIn( typeId, actualLength, result ) )
            {
                continue;
            }
            result[actualLength++] = getRelationshipsIterator( direction, typeId );
        }

        if ( actualLength < result.length )
        {
            RelIdIterator[] compacted = new RelIdIterator[actualLength];
            arraycopy( result, 0, compacted, 0, actualLength );
            result = compacted;
        }
        if ( result.length == 0 )
        {
            return PrimitiveLongCollections.emptyIterator();
        }
        return new RelationshipIterator( result, this, direction, types, relationshipLoader, hasMore, false,
                cacheUpdateListener );
    }

    private boolean typeIn( int typeId, int actualLength, RelIdIterator[] result )
    {
        for ( int i = 0; i < actualLength; i++ )
        {
            if ( result[i].getType() == typeId )
            {
                return true;
            }
        }
        return false;
    }

    private RelIdIterator getRelationshipsIterator( DirectionWrapper direction, int type )
    {
        RelIdArray src = getRelIdArray( type );
        return src != null ? src.iterator( direction ) : empty( type ).iterator( direction );
    }

    public PrimitiveLongIterator getRelationships( RelationshipLoader relationshipLoader, Direction dir,
            CacheUpdateListener cacheUpdateListener )
    {
        return getAllRelationships( relationshipLoader, wrap( dir ), cacheUpdateListener );
    }

    public PrimitiveLongIterator getRelationships( RelationshipLoader relationshipLoader,
            Direction direction, int[] types, CacheUpdateListener cacheUpdateListener )
    {
        return getAllRelationshipsOfType( relationshipLoader, wrap( direction ), types, cacheUpdateListener );
    }

    /**
     * Returns this node's string representation.
     *
     * @return the string representation of this node
     */
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "#" + this.getId();
    }

    private void ensureRelationshipMapNotNull( RelationshipLoader relationshipLoader,
            DirectionWrapper direction, int[] types, CacheUpdateListener cacheUpdateListener )
    {
        if ( relationships == null )
        {
            loadInitialRelationships( relationshipLoader, direction, types, cacheUpdateListener );
        }
    }

    private void ensureRelationshipMapNotNull( RelationshipLoader relationshipLoader,
            CacheUpdateListener cacheUpdateListener )
    {
        ensureRelationshipMapNotNull( relationshipLoader, DirectionWrapper.BOTH, NO_RELATIONSHIP_TYPES,
                cacheUpdateListener );
    }

    private void loadInitialRelationships( RelationshipLoader relationshipLoader, DirectionWrapper direction,
            int[] types, CacheUpdateListener cacheUpdateListener )
    {
        Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, RelationshipLoadingPosition> rels = null;
        synchronized ( this )
        {
            if ( relationships == null )
            {
                try
                {
                    relChainPosition = relationshipLoader.getRelationshipChainPosition( getId() );
                }
                catch ( InvalidRecordException e )
                {
                    throw new NotFoundException( "Node[" + id + "]" +
                            " concurrently deleted while loading its relationships?", e );
                }

                ArrayMap<Integer, RelIdArray> tmpRelMap = new ArrayMap<>();
                rels = getMoreRelationships( relationshipLoader, tmpRelMap, direction, types );
                this.relationships = toRelIdArray( tmpRelMap );
                this.relChainPosition = rels == null ? RelationshipLoadingPosition.EMPTY : rels.third();
                moreRelationshipsLoaded();
                cacheUpdateListener.newSize( this, sizeOfObjectInBytesIncludingOverhead() );
            }
        }
        if ( rels != null && rels.second().size() > 0 )
        {
            relationshipLoader.putAllInRelCache( rels.second() );
        }
    }

    private RelIdArray[] toRelIdArray( ArrayMap<Integer, RelIdArray> tmpRelMap )
    {
        RelIdArray[] result = new RelIdArray[tmpRelMap.size()];
        int i = 0;
        for ( RelIdArray array : tmpRelMap.values() )
        {
            result[i++] = array;
        }
        sort( result );
        return result;
    }

    private static final Comparator<RelIdArray> RELATIONSHIP_TYPE_COMPARATOR_FOR_SORTING = new Comparator<RelIdArray>()
    {
        @Override
        public int compare( RelIdArray o1, RelIdArray o2 )
        {
            return o1.getType() - o2.getType();
        }
    };

    /* This is essentially a deliberate misuse of Comparator, knowing details about Arrays#binarySearch.
     * The signature is binarySearch( T[] array, T key, Comparator<T> ), but in this case we're
     * comparing RelIdArray[] to an int as key. To avoid having to create a new object for
     * the key for each call we create a single Comparator taking the RelIdArray as first
     * argument and the key as the second, as #binarySearch does internally. Although the int
     * here will be boxed I imagine it to be slightly better, with Integer caching for low
     * integers. */
    @SuppressWarnings("rawtypes")
    private static final Comparator RELATIONSHIP_TYPE_COMPARATOR_FOR_BINARY_SEARCH = new Comparator()
    {
        @Override
        public int compare( Object o1, Object o2 )
        {
            return ((RelIdArray) o1).getType() - (Integer) o2;
        }
    };

    private static void sort( RelIdArray[] array )
    {
        Arrays.sort( array, RELATIONSHIP_TYPE_COMPARATOR_FOR_SORTING );
    }

    private Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>,RelationshipLoadingPosition> getMoreRelationships(
            RelationshipLoader relationshipLoader, ArrayMap<Integer, RelIdArray> tmpRelMap, DirectionWrapper direction,
            int[] types )
    {
        if ( !hasMoreRelationshipsToLoad( direction, types ) )
        {
            return Triplet.of( null, Collections.<RelationshipImpl>emptyList(), relChainPosition );
        }

        Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>,RelationshipLoadingPosition> rels =
                loadMoreRelationships( relationshipLoader, direction, types );

        ArrayMap<Integer, RelIdArray> addMap = rels.first();
        if ( addMap.size() == 0 )
        {
            return rels;
        }
        for ( Integer type : addMap.keySet() )
        {
            RelIdArray addRels = addMap.get( type );
            RelIdArray srcRels = tmpRelMap.get( type );
            if ( srcRels == null )
            {
                tmpRelMap.put( type, addRels );
            }
            else
            {
                RelIdArray newSrcRels = srcRels.addAll( addRels );
                // This can happen if srcRels gets upgraded to a RelIdArrayWithLoops
                if ( newSrcRels != srcRels )
                {
                    tmpRelMap.put( type, newSrcRels );
                }
            }
        }

        return rels;
    }

    protected boolean hasMoreRelationshipsToLoad()
    {
        return relChainPosition != null && relChainPosition.hasMore( DirectionWrapper.BOTH, NO_RELATIONSHIP_TYPES );
    }

    boolean hasMoreRelationshipsToLoad( DirectionWrapper direction, int[] types )
    {
        return relChainPosition != null && relChainPosition.hasMore( direction, types );
    }

    static enum LoadStatus
    {
        NOTHING( false, false ),
        LOADED_END( true, false ),
        LOADED_MORE( true, true );

        private final boolean loaded;
        private final boolean more;

        private LoadStatus( boolean loaded, boolean more )
        {
            this.loaded = loaded;
            this.more = more;
        }

        public boolean loaded()
        {
            return this.loaded;
        }

        public boolean hasMoreToLoad()
        {
            return this.more;
        }
    }

    LoadStatus getMoreRelationships( RelationshipLoader relationshipLoader, DirectionWrapper direction,
            int[] types, CacheUpdateListener cacheUpdateListener )
    {
        Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>,RelationshipLoadingPosition> rels;
        if ( !hasMoreRelationshipsToLoad( direction, types ) )
        {
            return LoadStatus.NOTHING;
        }
        boolean more;
        synchronized ( this )
        {
            if ( !hasMoreRelationshipsToLoad( direction, types ) )
            {
                return LoadStatus.NOTHING;
            }
            rels = loadMoreRelationships( relationshipLoader, direction, types );
            ArrayMap<Integer, RelIdArray> addMap = rels.first();
            if ( addMap.size() == 0 )
            {
                return LoadStatus.NOTHING;
            }
            for ( int type : addMap.keySet() )
            {
                RelIdArray addRels = addMap.get( type );
                RelIdArray srcRels = getRelIdArray( type );
                if ( srcRels == null )
                {
                    putRelIdArray( addRels );
                }
                else
                {
                    RelIdArray newSrcRels = srcRels.addAll( addRels );
                    // This can happen if srcRels gets upgraded to a RelIdArrayWithLoops
                    if ( newSrcRels != srcRels )
                    {
                        putRelIdArray( newSrcRels );
                    }
                }
            }
            relChainPosition = rels.third();
            moreRelationshipsLoaded();
            more = hasMoreRelationshipsToLoad( direction, types );
            cacheUpdateListener.newSize( this, sizeOfObjectInBytesIncludingOverhead() );

        }
        relationshipLoader.putAllInRelCache( rels.second() );
        return more ? LoadStatus.LOADED_MORE : LoadStatus.LOADED_END;
    }

    private Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>,RelationshipLoadingPosition>
            loadMoreRelationships( RelationshipLoader relationshipLoader, DirectionWrapper direction,
                    int[] types )
    {
        try
        {
            return relationshipLoader.getMoreRelationships( this, direction, types );
        }
        catch ( InvalidRecordException e )
        {
            throw new NotFoundException( "Unable to load one or more relationships from Node[" + id + "]" +
                    ". This usually happens when relationships are deleted by someone else just as we are about to " +
                    "load them. Please try again.", e );
        }
    }

    @SuppressWarnings("unchecked")
    private RelIdArray getRelIdArray( int type )
    {
        RelIdArray[] localRelationships = relationships;
        int index = Arrays.binarySearch( localRelationships, type, RELATIONSHIP_TYPE_COMPARATOR_FOR_BINARY_SEARCH );
        return index < 0 ? null : localRelationships[index];
    }

    private void putRelIdArray( RelIdArray addRels )
    {
        // we don't do size update here, instead performed
        // when calling commitRelationshipMaps and in getMoreRelationships

        // precondition: called under synchronization

        // make a local reference to the array to avoid multiple read barrier hits
        RelIdArray[] array = relationships;
        // Try to overwrite it if it's already set
        int expectedType = addRels.getType();
        for ( int i = 0; i < array.length; i++ )
        {
            if ( array[i].getType() == expectedType )
            {
                array[i] = addRels;
                return;
            }
        }
        // no previous entry of the given type - extend the array
        array = Arrays.copyOf( array, array.length + 1 );
        array[array.length - 1] = addRels;
        sort( array );
        relationships = array;
    }

    public void commitRelationshipMaps(
            PrimitiveIntObjectMap<RelIdArray> cowRelationshipAddMap,
            PrimitiveIntObjectMap<PrimitiveLongSet> removeMap )
    {
        if ( relationships == null )
        {
            // we will load full in some other tx
            return;
        }

        synchronized ( this )
        {
            if ( cowRelationshipAddMap != null )
            {
                PrimitiveIntIterator typeIterator = cowRelationshipAddMap.iterator();
                while ( typeIterator.hasNext() )
                {
                    int type = typeIterator.next();
                    RelIdArray add = cowRelationshipAddMap.get( type );
                    PrimitiveLongSet remove = null;
                    if ( removeMap != null )
                    {
                        remove = removeMap.get( type );
                    }
                    RelIdArray src = getRelIdArray( type );
                    putRelIdArray( RelIdArray.from( src, add, remove ) );
                }
            }
            if ( removeMap != null )
            {
                PrimitiveIntIterator typeIterator = removeMap.iterator();
                while ( typeIterator.hasNext() )
                {
                    int type = typeIterator.next();
                    if ( cowRelationshipAddMap != null &&
                            cowRelationshipAddMap.get( type ) != null )
                    {
                        continue;
                    }
                    RelIdArray src = getRelIdArray( type );
                    if ( src != null )
                    {
                        PrimitiveLongSet remove = removeMap.get( type );
                        putRelIdArray( RelIdArray.from( src, null, remove ) );
                    }
                }
            }
        }
    }

    public RelationshipLoadingPosition getRelChainPosition()
    {
        return this.relChainPosition;
    }

    // Mostly for testing
    void setRelChainPosition( RelationshipLoadingPosition position )
    {
        this.relChainPosition = position;
    }

    private void moreRelationshipsLoaded()
    {
        if ( relationships != null && !hasMoreRelationshipsToLoad( DirectionWrapper.BOTH, NO_RELATIONSHIP_TYPES ) )
        {   // precondition: must be called under synchronization
            // use local reference to avoid multiple read barriers
            RelIdArray[] array = relationships;
            // Done loading - Shrink arrays
            for ( int i = 0; i < array.length; i++ )
            {
                array[i].shrink();
            }
            relChainPosition = RelationshipLoadingPosition.EMPTY;
        }
    }

    RelIdArray getRelationshipIds( int type )
    {
        return getRelIdArray( type );
    }

    RelIdArray[] getRelationshipIds()
    {
        return relationships;
    }

    @Override
    PropertyContainer asProxy( NodeManager nm )
    {
        return nm.newNodeProxyById( getId() );
    }

    public int[] getLabels( CacheLoader<int[]> loader ) throws EntityNotFoundException
    {
        if ( labels == null )
        {
            synchronized ( this )
            {
                if ( labels == null )
                {
                    labels = loader.load( getId() );
                }
            }
        }
        return labels;
    }

    public boolean hasLabel( int labelId, CacheLoader<int[]> loader ) throws EntityNotFoundException
    {
        int[] labels = getLabels( loader );
        return binarySearch( labels, labelId ) >= 0;
    }

    public synchronized void commitLabels( int[] labels )
    {
        this.labels = labels;
    }

    @Override
    protected Property noProperty( int key )
    {
        return Property.noNodeProperty( getId(), key );
    }

    private void ensureAllRelationshipsAreLoaded( RelationshipLoader relationshipLoader,
            CacheUpdateListener cacheUpdateListener )
    {
        ensureRelationshipMapNotNull( relationshipLoader, cacheUpdateListener );
        while ( hasMoreRelationshipsToLoad() )
        {
            getMoreRelationships( relationshipLoader, DirectionWrapper.BOTH, NO_RELATIONSHIP_TYPES,
                    cacheUpdateListener );
        }
    }

    public int getDegree( RelationshipLoader relationshipLoader )
    {
        return relationshipLoader.getRelationshipCount( getId(), -1, DirectionWrapper.BOTH );
    }

    public int getDegree( RelationshipLoader relationshipLoader, int type,
            CacheUpdateListener cacheUpdateListener )
    {
        return getDegree( relationshipLoader, type, Direction.BOTH, cacheUpdateListener );
    }

    public int getDegree( RelationshipLoader relationshipLoader, Direction direction,
            CacheUpdateListener cacheUpdateListener )
    {
        if ( direction == Direction.BOTH )
        {
            return getDegree( relationshipLoader );
        }
        return getDegreeByDirection( relationshipLoader, wrap( direction ), cacheUpdateListener );
    }

    private int getDegreeByDirection( RelationshipLoader relationshipLoader, DirectionWrapper direction,
            CacheUpdateListener cacheUpdateListener )
    {
        ensureAllRelationshipsAreLoaded( relationshipLoader, cacheUpdateListener );
        int count = 0;
        if ( relationships != null )
        {
            for ( RelIdArray ids : relationships )
            {
                count += ids.length( direction );
            }
        }
        return count;
    }

    public int getDegree( RelationshipLoader relationshipLoader, int typeId, Direction direction,
            CacheUpdateListener cacheUpdateListener )
    {
        ensureAllRelationshipsAreLoaded( relationshipLoader, cacheUpdateListener );
        RelIdArray ids = getRelationshipIds( typeId );
        return ids != null ? ids.length( wrap( direction ) ) : 0;
    }

    public Iterator<Integer> getRelationshipTypes( RelationshipLoader relationshipLoader,
            CacheUpdateListener cacheUpdateListener )
    {
        ensureAllRelationshipsAreLoaded( relationshipLoader, cacheUpdateListener );
        Set<Integer> types = new HashSet<>();
        for ( RelIdArray ids : relationships )
        {
            types.add( ids.getType() );
        }
        return types.iterator();
    }
}
