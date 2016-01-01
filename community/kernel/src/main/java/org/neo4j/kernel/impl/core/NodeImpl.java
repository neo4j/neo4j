/**
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
package org.neo4j.kernel.impl.core;

import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;

import static org.neo4j.kernel.impl.cache.SizeOfs.REFERENCE_SIZE;
import static org.neo4j.kernel.impl.cache.SizeOfs.sizeOfArray;
import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverheadIncludingReferences;
import static org.neo4j.kernel.impl.util.RelIdArray.empty;
import static org.neo4j.kernel.impl.util.RelIdArray.wrap;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
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
import org.neo4j.kernel.impl.core.WritableTransactionState.CowEntityElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.SetAndDirectionCounter;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelationshipFilter;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdIterator;

/**
 * This class currently has multiple responsibilities, and a very complex set of interrelationships with the world
 * around it. It is being refactored, such that this will become a pure cache object, and be renamed eg. CachedNode.
 *
 * Responsibilities inside this class are slowly being moved over to {@link org.neo4j.kernel.impl.api.Kernel} and its
 * friends.
 *
 * As for the cache parts of this class the following can be said:
 *
 * The node cache is a combination of the node state itself together with state information regarding the
 * completeness of what is cached. The cached state is kept for performance reasons and the uncached state
 * information is necessary for completeness/correctness so that the missing parts can and will be fetched
 * from the disk store on demand.
 *
 * The cached state for a node is carried in the NodeImpl class and apart from node specific state,
 * it also carries some high-level properties of its related relationships, like relationship id, type and direction.
 * This is carried in the associated RelIdArray{WithLoops} classes.
 *
 * The uncached state information is kept by a set of relationship chain loading positions.
 * All other relationship metadata is carried in the relationship cache and not detailed here.
 *
 * Updating the node cache
 * -----------------------
 *
 * The node cache should be updated when changes are performed. Avoiding updates can lead to two problems:
 * <ul>
 * <li>lesser performance</li>
 * <li>incorrect state</li>
 * </ul>
 * The updates must cover the correctness both of the cached as well as the uncached state.
 * Most of the complexities in node cache updating are due to relationship changes, since they are much more dynamic
 * (can be added/removed) and also since only parts of the relationship state may be cached.
 * Failure to update properly can lead to cache poisoning, such as missing or duplicate relationships, f.ex. due to:
 * <ul>
 * <li>not adding to cached state</li>
 * <li>not updating uncached state information correctly</li>
 * <li>mismatch between cached state and uncached state info</li>
 * </ul>
 *
 * The node cache is updated when a:
 * <ul>
 * <li>transaction is committed, see {@link WritableTransactionState}</li>
 * <li>reader loads relationships (as part of getRelationships)</li>
 * </ul>
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

    public NodeImpl( long id )
    {
        this( id, false );
    }

    // newNode will only be true for NodeManager.createNode
    public NodeImpl( long id, boolean newNode )
    {
        super( newNode );
        this.id = id;
        if ( newNode )
        {
            relationships = NO_RELATIONSHIPS;
            relChainPosition = RelationshipLoadingPosition.EMPTY;
        }
    }

    @Override
    protected Iterator<DefinedProperty> loadProperties( NodeManager nodeManager )
    {
        return nodeManager.loadProperties( this, false );
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

    PrimitiveLongIterator getAllRelationships( NodeManager nodeManager, DirectionWrapper direction )
    {
        ensureRelationshipMapNotNull( nodeManager, direction, NO_RELATIONSHIP_TYPES );

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
                nodeManager, hasMore, true );
    }

    PrimitiveLongIterator getAllRelationshipsOfType( NodeManager nodeManager,
                                                      DirectionWrapper direction, int[] types )
    {
        ensureRelationshipMapNotNull( nodeManager, direction, types );

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
        return new RelationshipIterator( result, this, direction, types, nodeManager, hasMore, false );
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

    public PrimitiveLongIterator getRelationships( NodeManager nodeManager, Direction dir )
    {
        return getAllRelationships( nodeManager, wrap( dir ) );
    }

    public PrimitiveLongIterator getRelationships( NodeManager nodeManager,
                                                    Direction direction, int[] types )
    {
        return getAllRelationshipsOfType( nodeManager, wrap( direction ), types );
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

    private void ensureRelationshipMapNotNull( NodeManager nodeManager, DirectionWrapper direction,
            int[] types )
    {
        if ( relationships == null || (relationships.length == 0 && relChainPosition.hasMore( direction, types ) ) )
        {
            loadInitialRelationships( nodeManager, direction, types );
        }
    }

    private void ensureRelationshipMapNotNull( NodeManager nm )
    {
        ensureRelationshipMapNotNull( nm, DirectionWrapper.BOTH, NO_RELATIONSHIP_TYPES );
    }

    private void loadInitialRelationships( NodeManager nodeManager, DirectionWrapper direction,
            int[] types )
    {
        Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, RelationshipLoadingPosition> rels = null;
        synchronized ( this )
        {
            if ( relationships == null ||
                 (relationships.length == 0 && relChainPosition.hasMore( direction, types )) )
            {
                try
                {
                    relChainPosition = nodeManager.getRelationshipChainPosition( this );
                }
                catch ( InvalidRecordException e )
                {
                    throw new NotFoundException( asProxy( nodeManager ) +
                                                 " concurrently deleted while loading its relationships?", e );
                }

                ArrayMap<Integer, RelIdArray> tmpRelMap = new ArrayMap<>();
                rels = getMoreRelationships( nodeManager, tmpRelMap, direction, types );
                this.relationships = toRelIdArray( tmpRelMap );
                this.relChainPosition = rels == null ? RelationshipLoadingPosition.EMPTY : rels.third();
                moreRelationshipsLoaded();
                updateSize( nodeManager );
            }
        }
        if ( rels != null && rels.second().size() > 0 )
        {
            nodeManager.putAllInRelCache( rels.second() );
        }
    }

    private void updateSize( NodeManager nodeManager )
    {
        nodeManager.updateCacheSize( this, sizeOfObjectInBytesIncludingOverhead() );
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
            NodeManager nodeManager, ArrayMap<Integer, RelIdArray> tmpRelMap, DirectionWrapper direction,
            int[] types )
    {
        if ( !hasMoreRelationshipsToLoad( direction, types ) )
        {
            return Triplet.of( null, Collections.<RelationshipImpl>emptyList(), relChainPosition );
        }

        Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>,RelationshipLoadingPosition> rels =
                loadMoreRelationshipsFromNodeManager( nodeManager, direction, types );

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

    LoadStatus getMoreRelationships( NodeManager nodeManager, DirectionWrapper direction, int[] types )
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
            rels = loadMoreRelationshipsFromNodeManager( nodeManager, direction, types );
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
            updateSize( nodeManager );
        }
        nodeManager.putAllInRelCache( rels.second() );
        return more ? LoadStatus.LOADED_MORE : LoadStatus.LOADED_END;
    }

    private Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>,RelationshipLoadingPosition>
            loadMoreRelationshipsFromNodeManager( NodeManager nodeManager, DirectionWrapper direction,
                                                  int[] types )
    {
        try
        {
            return nodeManager.getMoreRelationships( this, direction, types );
        }
        catch ( InvalidRecordException e )
        {
            throw new NotFoundException( "Unable to load one or more relationships from " + asProxy( nodeManager ) +
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

    protected boolean isDense()
    {
        return false;
    }

    /**
     * @param dense {@code true} if this node, including the changes in this change set, should be dense,
     * otherwise {@code false}.
     * @return {@code true} if this node should be evicted, due to upgrade to dense node for example,
     * otherwise {@code false}.
     */
    protected synchronized boolean commitRelationshipMaps(
            ArrayMap<Integer, RelIdArray> cowRelationshipAddMap,
            ArrayMap<Integer, SetAndDirectionCounter> relationshipRemoveMap,
            FirstRelationshipIds firstRelationshipIds, boolean dense )
    {
        if ( dense != isDense() )
        {
            return true;
        }

        if ( relationships == null )
        {
            // we will load full in some other tx
            return false;
        }

        if ( cowRelationshipAddMap != null )
        {
            // Instantiate the filter which additions go through. Must be instantiated before we start
            // adding any relationships as part of this change set.
            RelationshipFilter filter = filterForAddingRelationships( firstRelationshipIds, relChainPosition );

            for ( int type : cowRelationshipAddMap.keySet() )
            {
                RelIdArray add = cowRelationshipAddMap.get( type );
                SetAndDirectionCounter remove = null;
                if ( relationshipRemoveMap != null )
                {
                    remove = relationshipRemoveMap.get( type );
                }
                RelIdArray src = getRelIdArray( type );
                putRelIdArray( RelIdArray.from( src, add, remove != null ? remove.set : null, filter ) );
            }
        }
        if ( relationshipRemoveMap != null )
        {
            for ( int type : relationshipRemoveMap.keySet() )
            {
                if ( cowRelationshipAddMap != null &&
                        cowRelationshipAddMap.get( type ) != null )
                {
                    continue;
                }
                RelIdArray src = getRelIdArray( type );
                if ( src != null )
                {
                    SetAndDirectionCounter remove = relationshipRemoveMap.get( type );
                    putRelIdArray( RelIdArray.from( src, null, remove != null ? remove.set : null ) );
                }
            }
        }
        return false;
    }

    /**
     * Instantiates a filter which relationships that committers apply to this cached node must pass
     * before being added. This filter is needed since there is no synchronization between committers and
     * regular readers which loads relationships.
     *
     * Specifically this filter guards for the following scenario:
     * <ol>
     * <li>COMMITTER: updates records as part of commit, and completes that work.</li>
     * <li>READER: comes in before COMMITTER applies its changes to cache and either loads the node fresh if it
     * has been evicted, or starts loading relationships if no relationships have been loaded yet. It will then see
     * the new changes made by COMMITTER. By the way, relationship chain additions are atomically visible,
     * as a result of the order that records are committed. Also, for clarification, loading relationships
     * implies also adding them to the relationship id cache on each node.</li>
     * <li>COMMITTER: adds the created relationships to the cached nodes and no duplication detection is made,
     * since that would be expensive and wouldn't scale.</li>
     * <li>At this point the relationships that COMMITTER created now exists two times in the cache.</li>
     * </ol>
     *
     * This all assumes that applying a commit means writing record changes to store first,
     * and then updating the cache. Doing it the other way around would have solved some tricky races that
     * we guard for here below, but fails in the face of random cache eviction.
     *
     * Solution presented here is to make the COMMITTER aware of this problem and let it have the ability to
     * not add its created relationships to cache where any READER has already loaded the very same relationships.
     * There's further documentation in each specific check within the implementation of this filter.
     *
     * @param firstRelationshipIdsToCommit object that is able to provide information about which relationship
     * ids will be the first in each changed relationship chain, as part of this commit.
     */
    protected RelationshipFilter filterForAddingRelationships( final FirstRelationshipIds firstRelationshipIdsToCommit,
            RelationshipLoadingPosition relChainPosition )
    {
        // Before we start to add any relationships as part of this commit, find and keep all first ids for every chain
        final PrimitiveLongSet cachedFirstIds = gatherFirstIds();
        return new RelationshipFilter()
        {
            @Override
            public boolean accept( int type, DirectionWrapper direction, long firstCachedId )
            {
                long firstIdToCommit = firstRelationshipIdsToCommit.firstIdOf( type, direction );
                assert firstIdToCommit != Record.NO_NEXT_RELATIONSHIP.intValue() :
                        "About to add relationships of " + type + " " + direction +
                        " to node " + getId() + ", but apparently the tx state says that no such relationships " +
                        "are to be added " + firstRelationshipIdsToCommit;

                // For sparse nodes there's a mismatch between how the relationship chain is laid out on disk
                // and how it's represented in the cache. On disk all relationships sits in one long chain,
                // but in cache relationships are split up by type and direction. The information about which
                // relationship is the first in the chain is lost after relationships have been loaded, and
                // we need to check all cached chains and if any of them has that id as its first one then
                // we can tell that a READER has already loaded the relationship(s) we're about to commit.
                return !cachedFirstIds.contains( firstIdToCommit );
            }
        };
    }

    protected PrimitiveLongSet gatherFirstIds()
    {
        PrimitiveLongSet result = Primitive.longSet( relationships.length * 3 );
        for ( RelIdArray ids : relationships )
        {
            for ( DirectionWrapper direction : DirectionWrapper.values() )
            {
                long firstId = direction.firstId( ids );
                if ( firstId != Record.NO_NEXT_RELATIONSHIP.intValue() )
                {
                    result.add( firstId );
                }
            }
        }
        return result;
    }

    RelationshipLoadingPosition getRelChainPosition()
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
    public CowEntityElement getEntityElement( PrimitiveElement element, boolean create )
    {
        return element.nodeElement( getId(), create );
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

    private void ensureAllRelationshipsAreLoaded( NodeManager nm )
    {
        ensureRelationshipMapNotNull( nm );
        while ( hasMoreRelationshipsToLoad() )
        {
            getMoreRelationships( nm, DirectionWrapper.BOTH, NO_RELATIONSHIP_TYPES );
        }
    }

    public int getDegree( NodeManager nm )
    {
        return nm.getRelationshipCount( this, -1, DirectionWrapper.BOTH );
    }

    public int getDegree( NodeManager nm, int type )
    {
        return getDegree( nm, type, Direction.BOTH );
    }

    public int getDegree( NodeManager nm, Direction direction )
    {
        if ( direction == Direction.BOTH )
        {
            return getDegree( nm );
        }
        return getDegreeByDirection( nm, wrap( direction ) );
    }

    private int getDegreeByDirection( NodeManager nm, DirectionWrapper direction )
    {
        ensureAllRelationshipsAreLoaded( nm );
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

    public int getDegree( NodeManager nm, int typeId, Direction direction )
    {
        ensureAllRelationshipsAreLoaded( nm );
        RelIdArray ids = getRelationshipIds( typeId );
        return ids != null ? ids.length( wrap( direction ) ) : 0;
    }

    public Iterator<Integer> getRelationshipTypes( final NodeManager nm )
    {
        ensureAllRelationshipsAreLoaded( nm );
        Set<Integer> types = new HashSet<>();
        for ( RelIdArray ids : relationships )
        {
            types.add( ids.getType() );
        }
        return types.iterator();
    }
}
