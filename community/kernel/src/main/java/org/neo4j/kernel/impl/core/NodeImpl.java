/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverheadIncludingReferences;
import static org.neo4j.kernel.impl.util.RelIdArray.empty;
import static org.neo4j.kernel.impl.util.RelIdArray.wrap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.impl.cache.SizeOfs;
import org.neo4j.kernel.impl.core.WritableTransactionState.CowEntityElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.CombinedRelIdIterator;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdIterator;

public class NodeImpl extends ArrayBasedPrimitive
{
    private static final RelIdArray[] NO_RELATIONSHIPS = new RelIdArray[0];

    private volatile RelIdArray[] relationships;

    private volatile long relChainPosition = Record.NO_NEXT_RELATIONSHIP.intValue();
    private final long id;

    NodeImpl( long id, long firstRel, long firstProp )
    {
        this( id, firstRel, firstProp, false );
    }

    // newNode will only be true for NodeManager.createNode
    NodeImpl( long id, long firstRel, long firstProp, boolean newNode )
    {
        /* TODO firstRel/firstProp isn't used yet due to some unresolved issue with clearing
         * of cache and keeping those first ids in the node instead of loading on demand.
         */
        super( newNode );
        this.id = id;
        if ( newNode ) relationships = NO_RELATIONSHIPS;
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public int size()
    {
        int size = super.size() + SizeOfs.REFERENCE_SIZE/*relationships reference*/ + 8/*relChainPosition*/ + 8/*id*/;
        if ( relationships != null )
        {
            size = withArrayOverheadIncludingReferences( size, relationships.length );
            for ( RelIdArray array : relationships )
                size += array.size();
        }
        return size;
    }

    @Override
    public int hashCode()
    {
        long id = getId();
        return (int) (( id >>> 32 ) ^ id );
    }

    @Override
    public boolean equals( Object obj )
    {
        return this == obj || ( obj instanceof NodeImpl && ( (NodeImpl) obj ).getId() == getId() );
    }

    @Override
    protected PropertyData changeProperty( NodeManager nodeManager,
            PropertyData property, Object value, TransactionState tx )
    {
        return nodeManager.nodeChangeProperty( this, property, value, tx );
    }

    @Override
    protected PropertyData addProperty( NodeManager nodeManager, PropertyIndex index, Object value )
    {
        return nodeManager.nodeAddProperty( this, index, value );
    }

    @Override
    protected void removeProperty( NodeManager nodeManager,
            PropertyData property, TransactionState tx )
    {
        nodeManager.nodeRemoveProperty( this, property, tx );
    }

    @Override
    protected ArrayMap<Integer, PropertyData> loadProperties(
            NodeManager nodeManager, boolean light )
    {
        return nodeManager.loadProperties( this, light );
    }

    Iterable<Relationship> getAllRelationships( NodeManager nodeManager, DirectionWrapper direction )
    {
        ensureRelationshipMapNotNull( nodeManager );
        
        // We need to check if there are more relationships to load before grabbing
        // the references to the RelIdArrays since otherwise there could be
        // another concurrent thread exhausting the chain position in between the point
        // where we got an empty iterator for a type that the other thread loaded and
        // the point where we check whether or not there are more relationships to load.
        boolean hasMore = hasMoreRelationshipsToLoad();
        
        RelIdArray[] localRelationships = relationships;
        RelIdIterator[] result = new RelIdIterator[localRelationships.length];
        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer, RelIdArray> addMap = null;
        ArrayMap<Integer, Collection<Long>> skipMap = null;
        if ( tx.hasChanges() )
        {
            addMap = tx.getCowRelationshipAddMap( this );
            skipMap = tx.getCowRelationshipRemoveMap( this );
        }

        for ( int i = 0; i < localRelationships.length; i++ )
        {
            RelIdArray src = localRelationships[i];
            int type = src.getType();
            RelIdIterator iterator = null;
            if ( addMap != null || skipMap != null )
            {
                iterator = new CombinedRelIdIterator( type, direction, src,
                        addMap != null ? addMap.get( type ) : null,
                        skipMap != null ? skipMap.get( type ) : null );
            }
            else
            {
                iterator = src.iterator( direction );
            }
            result[i] = iterator;
        }
        
        // New relationship types for this node which hasn't been committed yet,
        // but exists only as transactional state.
        if ( addMap != null )
        {
            RelIdIterator[] additional = new RelIdIterator[addMap.size() /*worst case size*/];
            int additionalSize = 0;
            for ( int type : addMap.keySet() )
            {
                if ( getRelIdArray( type ) == null )
                {
                    RelIdArray add = addMap.get( type );
                    additional[additionalSize++] = new CombinedRelIdIterator( type, direction, null, add,
                            skipMap != null ? skipMap.get( type ) : null );
                }
            }
            RelIdIterator[] newResult = new RelIdIterator[result.length+additionalSize];
            arraycopy( result, 0, newResult, 0, result.length );
            arraycopy( additional, 0, newResult, result.length, additionalSize );
            result = newResult;
        }
        if ( result.length == 0 )
            return Collections.emptyList();
        return new RelationshipIterator( result, this, direction, nodeManager, hasMore, true );
    }

    Iterable<Relationship> getAllRelationshipsOfType( NodeManager nodeManager,
        DirectionWrapper direction, RelationshipType... types)
    {
        ensureRelationshipMapNotNull( nodeManager );
        
        // We need to check if there are more relationships to load before grabbing
        // the references to the RelIdArrays since otherwise there could be
        // another concurrent thread exhausting the chain position in between the point
        // where we got an empty iterator for a type that the other thread loaded and
        // the point where we check whether or not there are more relationships to load.
        boolean hasMore = hasMoreRelationshipsToLoad();
        
        RelIdIterator[] result = new RelIdIterator[types.length];
        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer, RelIdArray> addMap = null;
        ArrayMap<Integer, Collection<Long>> skipMap = null;
        if ( tx.hasChanges() )
        {
            addMap = tx.getCowRelationshipAddMap( this );
            skipMap = tx.getCowRelationshipRemoveMap( this );
        }
        int actualLength = 0;
        for ( int i = 0; i < types.length; i++ )
        {
            int typeId;
            try
            {
                typeId = nodeManager.getRelationshipTypeIdFor( types[i] );
            }
            catch ( KeyNotFoundException e )
            {
                // This relationship type doesn't even exist in this database
                continue;
            }
            
            result[actualLength++] = getRelationshipsIterator( nodeManager, direction,
                    addMap != null ? addMap.get( typeId ) : null,
                    skipMap != null ? skipMap.get( typeId ) : null, typeId );
        }
        
        if ( actualLength < result.length )
        {
            RelIdIterator[] compacted = new RelIdIterator[actualLength];
            arraycopy( result, 0, compacted, 0, actualLength );
            result = compacted;
        }
        if ( result.length == 0 )
            return Collections.emptyList();
        return new RelationshipIterator( result, this, direction, nodeManager, hasMore, false );
    }
    
    private RelIdIterator getRelationshipsIterator( NodeManager nodeManager, DirectionWrapper direction,
            RelIdArray add, Collection<Long> remove, int type )
    {
        RelIdArray src = getRelIdArray( type );
        RelIdIterator iterator = null;
        if ( add != null || remove != null )
        {
            iterator = new CombinedRelIdIterator( type, direction, src, add, remove );
        }
        else
        {
            iterator = src != null ? src.iterator( direction ) : empty( type ).iterator( direction );
        }
        return iterator;
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager )
    {
        return getAllRelationships( nodeManager, DirectionWrapper.BOTH );
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager, Direction dir )
    {
        return getAllRelationships( nodeManager, wrap( dir ) );
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager, RelationshipType type )
    {
        return getAllRelationshipsOfType( nodeManager, DirectionWrapper.BOTH, new RelationshipType[] { type } );
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager,
            RelationshipType... types )
    {
        return getAllRelationshipsOfType( nodeManager, DirectionWrapper.BOTH, types );
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager,
            Direction direction, RelationshipType... types )
    {
        return getAllRelationshipsOfType( nodeManager, wrap( direction ), types );
    }

    public Relationship getSingleRelationship( NodeManager nodeManager, RelationshipType type,
        Direction dir )
    {
        Iterator<Relationship> rels = getAllRelationshipsOfType( nodeManager, wrap( dir ),
                new RelationshipType[] { type } ).iterator();
        if ( !rels.hasNext() )
        {
            return null;
        }
        Relationship rel = rels.next();
        if ( rels.hasNext() )
        {
            throw new NotFoundException( "More than one relationship[" +
                type + ", " + dir + "] found for " + this );
        }
        return rel;
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager, RelationshipType type,
        Direction dir )
    {
        return getAllRelationshipsOfType( nodeManager, wrap( dir ), new RelationshipType[] { type } );
    }

    public void addLabel( Label label )
    {

    }

    public void delete( NodeManager nodeManager, Node proxy )
    {
        boolean success = false;
        TransactionState tx = nodeManager.getTransactionState();
        tx.acquireWriteLock( proxy );
        try
        {
            ArrayMap<Integer,PropertyData> skipMap = tx.getOrCreateCowPropertyRemoveMap( this );
            ArrayMap<Integer,PropertyData> removedProps = nodeManager.deleteNode( this, tx );
            if ( removedProps.size() > 0 )
            {
                for ( Integer index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
            }
            success = true;
        }
        finally
        {
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    /**
     * Returns this node's string representation.
     *
     * @return the string representation of this node
     */
    @Override
    public String toString()
    {
        return "NodeImpl#" + this.getId();
    }

    private void ensureRelationshipMapNotNull( NodeManager nodeManager )
    {
        if ( relationships == null )
        {
            loadInitialRelationships( nodeManager );
        }
    }

    private void loadInitialRelationships( NodeManager nodeManager )
    {
        Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, Long> rels = null;
        synchronized ( this )
        {
            if ( relationships == null )
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
                
                ArrayMap<Integer,RelIdArray> tmpRelMap = new ArrayMap<Integer,RelIdArray>();
                rels = getMoreRelationships( nodeManager, tmpRelMap );
                this.relationships = toRelIdArray( tmpRelMap );
                if ( rels != null )
                {
                    setRelChainPosition( rels.third() );
                }
                updateSize( nodeManager );
            }
        }
        if ( rels != null )
        {
            nodeManager.putAllInRelCache( rels.second() );
        }
    }

    @Override
    protected void updateSize( NodeManager nodeManager )
    {
        nodeManager.updateCacheSize( this, size() );
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
    @SuppressWarnings( "rawtypes" )
    private static final Comparator RELATIONSHIP_TYPE_COMPARATOR_FOR_BINARY_SEARCH = new Comparator()
    {
        @Override
        public int compare( Object o1, Object o2 )
        {
            return ((RelIdArray)o1).getType() - ((Integer) o2).intValue();
        }
    };
    
    private static void sort( RelIdArray[] array )
    {
        Arrays.sort( array, RELATIONSHIP_TYPE_COMPARATOR_FOR_SORTING );
    }

    private Triplet<ArrayMap<Integer,RelIdArray>,List<RelationshipImpl>,Long> getMoreRelationships(
            NodeManager nodeManager, ArrayMap<Integer,RelIdArray> tmpRelMap )
    {
        if ( !hasMoreRelationshipsToLoad() )
        {
            return null;
        }
        Triplet<ArrayMap<Integer,RelIdArray>,List<RelationshipImpl>,Long> rels;

        rels = loadMoreRelationshipsFromNodeManager( nodeManager );

        ArrayMap<Integer,RelIdArray> addMap = rels.first();
        if ( addMap.size() == 0 )
        {
            return null;
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
        // nodeManager.putAllInRelCache( pair.other() );
    }

    boolean hasMoreRelationshipsToLoad()
    {
        return getRelChainPosition() != Record.NO_NEXT_RELATIONSHIP.intValue();
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

    LoadStatus getMoreRelationships( NodeManager nodeManager )
    {
        Triplet<ArrayMap<Integer,RelIdArray>,List<RelationshipImpl>,Long> rels;
        if ( !hasMoreRelationshipsToLoad() )
        {
            return LoadStatus.NOTHING;
        }
        boolean more = false;
        synchronized ( this )
        {
            if ( !hasMoreRelationshipsToLoad() )
            {
                return LoadStatus.NOTHING;
            }
            rels = loadMoreRelationshipsFromNodeManager(nodeManager);
            ArrayMap<Integer,RelIdArray> addMap = rels.first();
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
            setRelChainPosition( rels.third() );
            more = hasMoreRelationshipsToLoad();
            updateSize( nodeManager );
        }
        nodeManager.putAllInRelCache( rels.second() );
        return more ? LoadStatus.LOADED_MORE : LoadStatus.LOADED_END;
    }

    private Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, Long>
        loadMoreRelationshipsFromNodeManager( NodeManager nodeManager )
    {
        try
        {
            return nodeManager.getMoreRelationships( this );
        }
        catch(InvalidRecordException e)
        {
            throw new NotFoundException( "Unable to load one or more relationships from " + asProxy( nodeManager ) +
                    ". This usually happens when relationships are deleted by someone else just as we are about to load them. Please try again.", e );
        }
    }

    @SuppressWarnings( "unchecked" )
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

    public Relationship createRelationshipTo( NodeManager nodeManager, Node thisProxy,
        Node otherNode, RelationshipType type )
    {
        return nodeManager.createRelationship( thisProxy, this, otherNode, type );
    }

    public boolean hasRelationship( NodeManager nodeManager )
    {
        return getRelationships( nodeManager ).iterator().hasNext();
    }

    public boolean hasRelationship( NodeManager nodeManager, RelationshipType... types )
    {
        return getRelationships( nodeManager, types ).iterator().hasNext();
    }

    public boolean hasRelationship( NodeManager nodeManager, Direction direction,
            RelationshipType... types )
    {
        return getRelationships( nodeManager, direction, types ).iterator().hasNext();
    }

    public boolean hasRelationship( NodeManager nodeManager, Direction dir )
    {
        return getRelationships( nodeManager, dir ).iterator().hasNext();
    }

    public boolean hasRelationship( NodeManager nodeManager, RelationshipType type, Direction dir )
    {
        return getRelationships( nodeManager, type, dir ).iterator().hasNext();
    }

    protected void commitRelationshipMaps(
        ArrayMap<Integer,RelIdArray> cowRelationshipAddMap,
        ArrayMap<Integer,Collection<Long>> cowRelationshipRemoveMap, long firstRel, NodeManager nodeManager )
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
                for ( int type : cowRelationshipAddMap.keySet() )
                {
                    RelIdArray add = cowRelationshipAddMap.get( type );
                    Collection<Long> remove = null;
                    if ( cowRelationshipRemoveMap != null )
                    {
                        remove = cowRelationshipRemoveMap.get( type );
                    }
                    RelIdArray src = getRelIdArray( type );
                    putRelIdArray( RelIdArray.from( src, add, remove ) );
                }
            }
            if ( cowRelationshipRemoveMap != null )
            {
                for ( int type : cowRelationshipRemoveMap.keySet() )
                {
                    if ( cowRelationshipAddMap != null &&
                        cowRelationshipAddMap.get( type ) != null )
                    {
                        continue;
                    }
                    RelIdArray src = getRelIdArray( type );
                    if ( src != null )
                    {
                        Collection<Long> remove = cowRelationshipRemoveMap.get( type );
                        putRelIdArray( RelIdArray.from( src, null, remove ) );
                    }
                }
            }
            updateSize( nodeManager );
        }
    }

    long getRelChainPosition()
    {
        return relChainPosition;
    }

    void setRelChainPosition( long position )
    { // precondition: must be called under synchronization
        relChainPosition = position;
        // use local reference to avoid multiple read barriers
        RelIdArray[] array = relationships;
        if ( !hasMoreRelationshipsToLoad() && array != null )
        {
            // Done loading - Shrink arrays
            for ( int i = 0; i < array.length; i++ )
            {
                array[i] = array[i].shrink();
            }
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
        return nm.newNodeProxyById(getId());
    }
}
