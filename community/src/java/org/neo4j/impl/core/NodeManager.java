/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.TransactionManager;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.cache.AdaptiveCacheManager;
import org.neo4j.impl.cache.Cache;
import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.cache.SoftLruCache;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.transaction.IllegalResourceException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.traversal.InternalTraverserFactory;
import org.neo4j.impl.util.ArrayMap;
import org.neo4j.impl.util.IntArray;

public class NodeManager
{
    private static Logger log = Logger.getLogger( NodeManager.class.getName() );

    private int referenceNodeId = 0;
    
    private final Cache<Integer,NodeImpl> nodeCache;
    private final Cache<Integer,RelationshipImpl> relCache;
    private final AdaptiveCacheManager cacheManager;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;
    private final LockReleaser lockReleaser;
    private final PropertyIndexManager propertyIndexManager;
    private final InternalTraverserFactory traverserFactory;
    private final RelationshipTypeHolder relTypeHolder;
    private final PersistenceManager persistenceManager;
    private final IdGenerator idGenerator;

    private boolean useAdaptiveCache = true;
    private float adaptiveCacheHeapRatio = 0.77f;
    private int minNodeCacheSize = 0;
    private int minRelCacheSize = 0;
    private int maxNodeCacheSize = 1500;
    private int maxRelCacheSize = 3500;
    
    private static final int LOCK_STRIPE_COUNT = 5;
    private final ReentrantLock loadLocks[] = 
        new ReentrantLock[LOCK_STRIPE_COUNT]; 

    NodeManager( AdaptiveCacheManager cacheManager, LockManager lockManager, 
        LockReleaser lockReleaser, TransactionManager transactionManager, 
        PersistenceManager persistenceManager, IdGenerator idGenerator, 
        boolean useNewCaches )
    {
        this.cacheManager = cacheManager;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.propertyIndexManager = new PropertyIndexManager(
            transactionManager, persistenceManager, idGenerator );
        this.lockReleaser = lockReleaser; 
        lockReleaser.setNodeManager( this );
        lockReleaser.setPropertyIndexManager( propertyIndexManager );
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
        this.traverserFactory = new InternalTraverserFactory();
        this.relTypeHolder = new RelationshipTypeHolder( transactionManager,
            persistenceManager, idGenerator );
        if ( useNewCaches )
        {
            nodeCache = new SoftLruCache<Integer,NodeImpl>( 
                "NodeCache" );
            relCache = new SoftLruCache<Integer,RelationshipImpl>( 
                "RelationshipCache" );
        }
        else
        {
            nodeCache = new LruCache<Integer,NodeImpl>( "NodeCache", 1500,
                this.cacheManager );
            relCache = new LruCache<Integer,RelationshipImpl>( 
                "RelationshipCache", 3500, this.cacheManager );
        }
        for ( int i = 0; i < loadLocks.length; i++ )
        {
            loadLocks[i] = new ReentrantLock();
        }
    }

    private void parseParams( Map<Object,Object> params )
    {
        if ( params.containsKey( "use_adaptive_cache" ) )
        {
            String value = (String) params.get( "use_adaptive_cache" );
            if ( value.toLowerCase().equals( "yes" ) )
            {
                useAdaptiveCache = true;
            }
            else if ( value.toLowerCase().equals( "no" ) )
            {
                useAdaptiveCache = false;
            }
            else
            {
                log.warning( "Unable to parse use_adaptive_cache=" + value );
            }
        }
        if ( params.containsKey( "adaptive_cache_heap_ratio" ) )
        {
            Object value = params.get( "adaptive_cache_heap_ratio" );
            try
            {
                adaptiveCacheHeapRatio = Float.parseFloat( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( "Unable to parse adaptive_cache_heap_ratio "
                    + value );
            }
            if ( adaptiveCacheHeapRatio < 0.1f )
            {
                adaptiveCacheHeapRatio = 0.1f;
            }
            if ( adaptiveCacheHeapRatio > 0.95f )
            {
                adaptiveCacheHeapRatio = 0.95f;
            }
        }
        if ( params.containsKey( "min_node_cache_size" ) )
        {
            Object value = params.get( "min_node_cache_size" );
            try
            {
                minNodeCacheSize = Integer.parseInt( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( "Unable to parse min_node_cache_size " + value );
            }
        }
        if ( params.containsKey( "min_relationship_cache_size" ) )
        {
            Object value = params.get( "min_relationship_cache_size" );
            try
            {
                minRelCacheSize = Integer.parseInt( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( "Unable to parse min_relationship_cache_size "
                    + value );
            }
        }
        if ( params.containsKey( "max_node_cache_size" ) )
        {
            Object value = params.get( "max_node_cache_size" );
            try
            {
                maxNodeCacheSize = Integer.parseInt( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( "Unable to parse max_node_cache_size " + value );
            }
        }
        if ( params.containsKey( "max_relationship_cache_size" ) )
        {
            Object value = params.get( "max_relationship_cache_size" );
            try
            {
                maxRelCacheSize = Integer.parseInt( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( "Unable to parse max_relationship_cache_size "
                    + value );
            }
        }
    }

    public void start( Map<Object,Object> params )
    {
        parseParams( params );
        nodeCache.resize( maxNodeCacheSize );
        relCache.resize( maxRelCacheSize );
        if ( useAdaptiveCache )
        {
            cacheManager.registerCache( nodeCache, adaptiveCacheHeapRatio,
                minNodeCacheSize );
            cacheManager.registerCache( relCache, adaptiveCacheHeapRatio,
                minRelCacheSize );
            cacheManager.start( params );
        }
    }

    public void stop()
    {
        if ( useAdaptiveCache )
        {
            cacheManager.stop();
            cacheManager.unregisterCache( nodeCache );
            cacheManager.unregisterCache( relCache );
        }
        relTypeHolder.clear();
    }

    public Node createNode()
    {
        int id = idGenerator.nextId( Node.class );
        NodeImpl node = new NodeImpl( id, true, this );
        acquireLock( node, LockType.WRITE );
        boolean success = false;
        try
        {
            persistenceManager.nodeCreate( id );
            nodeCache.put( (int) node.getId(), node );
            success = true;
            return new NodeProxy( id, this );
        }
        finally
        {
            releaseLock( node, LockType.WRITE );
            if ( !success )
            {
                setRollbackOnly();
            }
        }
    }

    public Relationship createRelationship( Node startNode, Node endNode,
        RelationshipType type )
    {
        if ( startNode == null || endNode == null || type == null )
        {
            throw new IllegalArgumentException( "Null parameter, startNode="
                + startNode + ", endNode=" + endNode + ", type=" + type );
        }
        
        if ( !relTypeHolder.isValidRelationshipType( type ) )
        {
            relTypeHolder.addValidRelationshipType( type.name(), true );
        }
        int startNodeId = (int) startNode.getId();
        NodeImpl firstNode = getLightNode( startNodeId );
        if ( firstNode == null )
        {
            setRollbackOnly();
            throw new RuntimeException( "First node[" + startNode.getId()
                + "] deleted" );
        }
        int endNodeId = (int) endNode.getId();
        NodeImpl secondNode = getLightNode( endNodeId );
        if ( secondNode == null )
        {
            setRollbackOnly();
            throw new RuntimeException( "Second node[" + endNode.getId()
                + "] deleted" );
        }
        int id = idGenerator.nextId( Relationship.class );
        RelationshipImpl rel = new RelationshipImpl( id, startNodeId,
            endNodeId, type, true, this );
        boolean firstNodeTaken = false;
        boolean secondNodeTaken = false;
        acquireLock( rel, LockType.WRITE );
        boolean success = false;
        try
        {
            acquireLock( firstNode, LockType.WRITE );
            firstNodeTaken = true;
            acquireLock( secondNode, LockType.WRITE );
            secondNodeTaken = true;
            int typeId = getRelationshipTypeIdFor( type );
            persistenceManager.relationshipCreate( id, typeId, startNodeId,
                endNodeId );
            firstNode.addRelationship( type, id );
            secondNode.addRelationship( type, id );
            relCache.put( (int) rel.getId(), rel );
            success = true;
            return new RelationshipProxy( id, this );
        }
        finally
        {
            boolean releaseFailed = false;
            if ( firstNodeTaken )
            {
                try
                {
                    releaseLock( firstNode, LockType.WRITE );
                }
                catch ( Exception e )
                {
                    releaseFailed = true;
                    e.printStackTrace();
                    log.severe( "Failed to release lock" );
                }
            }
            if ( secondNodeTaken )
            {
                try
                {
                    releaseLock( secondNode, LockType.WRITE );
                }
                catch ( Exception e )
                {
                    releaseFailed = true;
                    e.printStackTrace();
                    log.severe( "Failed to release lock" );
                }
            }
            releaseLock( rel, LockType.WRITE );
            if ( !success )
            {
                setRollbackOnly();
            }
            if ( releaseFailed )
            {
                throw new RuntimeException( "Unable to release locks ["
                    + startNode + "," + endNode + "] in relationship create->"
                    + rel );
            }
        }
    }

    private ReentrantLock lockId( int id )
    {
        ReentrantLock lock = loadLocks[id % LOCK_STRIPE_COUNT ];
        lock.lock();
        return lock;
    }
    
    public Node getNodeById( int nodeId ) throws NotFoundException
    {
        if ( nodeId < 0 )
        {
            throw new IllegalArgumentException( "Negative node id " + nodeId );
        }
        
        NodeImpl node = nodeCache.get( nodeId );
        if ( node != null )
        {
            return new NodeProxy( nodeId, this );
        }
        node = new NodeImpl( nodeId, this );
        ReentrantLock loadLock = lockId( nodeId );
        try
        {
            if ( nodeCache.get( nodeId ) != null )
            {
                return new NodeProxy( nodeId, this );
            }
            if ( !persistenceManager.loadLightNode( nodeId ) )
            {
                throw new NotFoundException( "Node[" + nodeId + "]" );
            }
            nodeCache.put( nodeId, node );
            return new NodeProxy( nodeId, this );
        }
        finally
        {
            loadLock.unlock();
        }
    }

    NodeImpl getLightNode( int nodeId )
    {
        NodeImpl node = nodeCache.get( nodeId );
        if ( node != null )
        {
            return node;
        }
        node = new NodeImpl( nodeId, this );
        ReentrantLock loadLock = lockId( nodeId );
        try
        {
            if ( nodeCache.get( nodeId ) != null )
            {
                node = nodeCache.get( nodeId );
                return node;
            }
            if ( !persistenceManager.loadLightNode( nodeId ) )
            {
                return null;
            }
            nodeCache.put( nodeId, node );
            return node;
        }
        finally
        {
            loadLock.unlock();
        }
    }

    NodeImpl getNodeForProxy( int nodeId )
    {
        NodeImpl node = nodeCache.get( nodeId );
        if ( node != null )
        {
            return node;
        }
        node = new NodeImpl( nodeId, this );
        ReentrantLock loadLock = lockId( nodeId );
        try
        {
            if ( nodeCache.get( nodeId ) != null )
            {
                node = nodeCache.get( nodeId );
                return node;
            }
            if ( !persistenceManager.loadLightNode( nodeId ) )
            {
                throw new NotFoundException( "Node[" + nodeId + "] not found." );
            }
            nodeCache.put( nodeId, node );
            return node;
        }
        finally
        {
            loadLock.unlock();
        }
    }

    public Node getReferenceNode() throws NotFoundException
    {
        if ( referenceNodeId == -1 )
        {
            throw new NotFoundException( "No reference node set" );
        }
        return getNodeById( referenceNodeId );
    }

    void setReferenceNodeId( int nodeId )
    {
        this.referenceNodeId = nodeId;
    }

    public Relationship getRelationshipById( int relId )
        throws NotFoundException
    {
        if ( relId < 0 )
        {
            throw new IllegalArgumentException( "Negative id " + relId );
        }
        
        RelationshipImpl relationship = relCache.get( relId );
        if ( relationship != null )
        {
            return new RelationshipProxy( relId, this );
        }
        relationship = new RelationshipImpl( relId, this );
        ReentrantLock loadLock = lockId( relId );
        try
        {
            if ( relCache.get( relId ) != null )
            {
                relationship = relCache.get( relId );
                return new RelationshipProxy( relId, this );
            }
            RelationshipData data = persistenceManager.loadLightRelationship( 
                relId );
            if ( data == null )
            {
                throw new NotFoundException( "Relationship[" + relId
                    + "] not found" );
            }
            int typeId = data.relationshipType(); 
            RelationshipType type = getRelationshipTypeById( typeId ); 
            if ( type == null )
            {
                throw new RuntimeException( "Relationship[" + data.getId()
                    + "] exist but relationship type[" + typeId
                    + "] not found." );
            }
            final int startNodeId = data.firstNode();
            final int endNodeId = data.secondNode();
            relationship = new RelationshipImpl( relId, startNodeId, endNodeId,
                type, false, this );
            relCache.put( relId, relationship );
            return new RelationshipProxy( relId, this );
        }
        finally
        {
            loadLock.unlock();
        }
    }

    RelationshipType getRelationshipTypeById( int id )
    {
        return relTypeHolder.getRelationshipType( id );
    }

    Relationship getRelForProxy( int relId )
    {
        RelationshipImpl relationship = relCache.get( relId );
        if ( relationship != null )
        {
            return relationship;
        }
        relationship = new RelationshipImpl( relId, this );
        ReentrantLock loadLock = lockId( relId );
        try
        {
            if ( relCache.get( relId ) != null )
            {
                relationship = relCache.get( relId );
                return relationship;
            }
            RelationshipData data = persistenceManager.loadLightRelationship( 
                relId );
            if ( data == null )
            {
                throw new NotFoundException( "Relationship[" + relId
                    + "] not found." );
            }
            int typeId = data.relationshipType();
            RelationshipType type = getRelationshipTypeById( typeId );
            if ( type == null )
            {
                throw new RuntimeException( "Relationship[" + data.getId()
                    + "] exist but relationship type[" + typeId
                    + "] not found." );
            }
            relationship = new RelationshipImpl( relId, data.firstNode(),
                data.secondNode(), type, false, this );
            relCache.put( relId, relationship );
            return relationship;
        }
        finally
        {
            loadLock.unlock();
        }
    }

    public void removeNodeFromCache( int nodeId )
    {
        nodeCache.remove( nodeId );
    }

    public void removeRelationshipFromCache( int id )
    {
        relCache.remove( id );
    }

    Object loadPropertyValue( int id )
    {
        return persistenceManager.loadPropertyValue( id );
    }

    ArrayMap<String,IntArray> loadRelationships( NodeImpl node )
    {
        try
        {
            Iterable<RelationshipData> rels = 
                persistenceManager.loadRelationships( (int) node.getId() );
            ArrayMap<String,IntArray> newRelationshipMap = 
                new ArrayMap<String,IntArray>();
            for ( RelationshipData rel : rels )
            {
                int relId = rel.getId();
                RelationshipImpl relImpl = relCache.get( relId );
                RelationshipType type = null;
                if ( relImpl == null )
                {
                    type = getRelationshipTypeById( rel.relationshipType() );
                    assert type != null;
                    relImpl = new RelationshipImpl( relId, rel.firstNode(),
                        rel.secondNode(), type, false, this );
                    relCache.put( relId, relImpl );
                }
                else
                {
                    type = relImpl.getType();
                }
                IntArray relationshipSet = newRelationshipMap.get( 
                    type.name() );
                if ( relationshipSet == null )
                {
                    relationshipSet = new IntArray();
                    newRelationshipMap.put( type.name(), relationshipSet );
                }
                relationshipSet.add( relId );
            }
            return newRelationshipMap;
        }
        catch ( Exception e )
        {
            log.severe( "Failed loading relationships for node[" + node.getId()
                + "]" );
            throw new RuntimeException( e );
        }
    }

    ArrayMap<Integer,PropertyData> loadProperties( NodeImpl node )
    {
        try
        {
            return persistenceManager.loadNodeProperties( (int) node.getId() );
        }
        catch ( Exception e )
        {
            log.severe( "Failed loading properties for node[" + node.getId()
                + "]" );
            throw new RuntimeException( e );
        }
    }

    ArrayMap<Integer,PropertyData> loadProperties( RelationshipImpl relationship )
    {
        try
        {
            return persistenceManager.loadRelProperties( 
                (int) relationship.getId() );
        }
        catch ( Exception e )
        {
            log.severe( "Failed loading properties for relationship["
                + relationship.getId() + "]" );
            throw new RuntimeException( e );
        }
    }

    int getNodeMaxCacheSize()
    {
        // return nodeCache.maxSize();
        return -1;
    }

    int getRelationshipMaxCacheSize()
    {
        // return relCache.maxSize();
        return -1;
    }

    public void clearCache()
    {
        nodeCache.clear();
        relCache.clear();
    }

    void setRollbackOnly()
    {
        try
        {
            transactionManager.setRollbackOnly();
        }
        catch ( IllegalStateException e )
        {
            // this exception always get generated in a finally block and 
            // when it happens another exception has already been thrown
            // (most likley NotInTransactionException)
            log.log( Level.FINE, "Failed to set transaction rollback only", e );
        }
        catch ( javax.transaction.SystemException se )
        {
            // our TM never throws this exception
            log.log( Level.SEVERE, "Failed to set transaction rollback only", 
                se );
        }
    }

    void acquireLock( Object resource, LockType lockType )
    {
        try
        {
            if ( lockType == LockType.READ )
            {
                lockManager.getReadLock( resource );
            }
            else if ( lockType == LockType.WRITE )
            {
                lockManager.getWriteLock( resource );
            }
            else
            {
                throw new RuntimeException( "Unknown lock type: " + lockType );
            }
        }
        catch ( IllegalResourceException e )
        {
            throw new RuntimeException( e );
        }
    }

    void releaseLock( Object resource, LockType lockType )
    {
        if ( lockType == LockType.READ )
        {
            lockManager.releaseReadLock( resource );
        }
        else if ( lockType == LockType.WRITE )
        {
            lockReleaser.addLockToTransaction( resource, lockType );
        }
        else
        {
            throw new RuntimeException( "Unknown lock type: " + lockType );
        }
    }

    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        return idGenerator.getHighestPossibleIdInUse( clazz );
    }

    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        return idGenerator.getNumberOfIdsInUse( clazz );
    }

    public void removeRelationshipTypeFromCache( int id )
    {
        relTypeHolder.removeRelType( id );
    }

    Traverser createTraverser( Order traversalOrder, NodeImpl node,
        RelationshipType relationshipType, Direction direction,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator )
    {
        return traverserFactory.createTraverser( traversalOrder, new NodeProxy(
            (int) node.getId(), this ), relationshipType, direction,
            stopEvaluator, returnableEvaluator );
    }

    Traverser createTraverser( Order traversalOrder, NodeImpl node,
        RelationshipType[] types, Direction[] dirs,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator )
    {
        return traverserFactory.createTraverser( traversalOrder, new NodeProxy(
            (int) node.getId(), this ), types, dirs, stopEvaluator,
            returnableEvaluator );
    }

    void addPropertyIndexes( PropertyIndexData[] propertyIndexes )
    {
        propertyIndexManager.addPropertyIndexes( propertyIndexes );
    }

    void setHasAllpropertyIndexes( boolean hasAll )
    {
        propertyIndexManager.setHasAll( hasAll );
    }

    void clearPropertyIndexes()
    {
        propertyIndexManager.clear();
    }

    PropertyIndex getIndexFor( int keyId )
    {
        return propertyIndexManager.getIndexFor( keyId );
    }

    Iterable<PropertyIndex> index( String key )
    {
        return propertyIndexManager.index( key );
    }

    boolean hasAllPropertyIndexes()
    {
        return propertyIndexManager.hasAll();
    }

    boolean hasIndexFor( int keyId )
    {
        return propertyIndexManager.hasIndexFor( keyId );
    }

    PropertyIndex createPropertyIndex( String key )
    {
        return propertyIndexManager.createPropertyIndex( key );
    }

    int getRelationshipTypeIdFor( RelationshipType type )
    {
        return relTypeHolder.getIdFor( type );
    }

    void addRawRelationshipTypes( RelationshipTypeData[] relTypes )
    {
        relTypeHolder.addRawRelationshipTypes( relTypes );
    }

    Iterable<RelationshipType> getRelationshipTypes()
    {
        return relTypeHolder.getRelationshipTypes();
    }

    void deleteNode( NodeImpl node )
    {
        int nodeId = (int) node.getId();
        persistenceManager.nodeDelete( nodeId );
        // remove from node cache done via event
    }

    int nodeAddProperty( NodeImpl node, PropertyIndex index, Object value )
    {
        int nodeId = (int) node.getId();
        return persistenceManager.nodeAddProperty( nodeId, index, value );
    }

    void nodeChangeProperty( NodeImpl node, int propertyId, Object value )
    {
        int nodeId = (int) node.getId();
        persistenceManager.nodeChangeProperty( nodeId, propertyId, value );
    }

    void nodeRemoveProperty( NodeImpl node, int propertyId )
    {
        int nodeId = (int) node.getId();
        persistenceManager.nodeRemoveProperty( nodeId, propertyId );
    }

    public void deleteRelationship( RelationshipImpl rel )
    {
        int relId = (int) rel.getId();
        persistenceManager.relDelete( relId );
        // remove in rel cache done via event
    }

    int relAddProperty( RelationshipImpl rel, PropertyIndex index, 
        Object value )
    {
        int relId = (int) rel.getId();
        return persistenceManager.relAddProperty( relId, index, value );
    }

    void relChangeProperty( RelationshipImpl rel, int propertyId, Object value )
    {
        int relId = (int) rel.getId();
        persistenceManager.relChangeProperty( relId, propertyId, value );
    }

    void relRemoveProperty( RelationshipImpl rel, int propertyId )
    {
        int relId = (int) rel.getId();
        persistenceManager.relRemoveProperty( relId, propertyId );
    }

    public IntArray getCowRelationshipRemoveMap( NodeImpl node, String type )
    {
        return lockReleaser.getCowRelationshipRemoveMap( node, type );
    }

    public IntArray getCowRelationshipRemoveMap( NodeImpl node, String type,
        boolean create )
    {
        return lockReleaser.getCowRelationshipRemoveMap( node, type, create );
    }

    public ArrayMap<String,IntArray> getCowRelationshipAddMap( NodeImpl node )
    {
        return lockReleaser.getCowRelationshipAddMap( node );
    }

    public IntArray getCowRelationshipAddMap( NodeImpl node, String string )
    {
        return lockReleaser.getCowRelationshipAddMap( node, string );
    }

    public IntArray getCowRelationshipAddMap( NodeImpl node, String string,
        boolean create )
    {
        return lockReleaser.getCowRelationshipAddMap( node, string, create );
    }

    public NodeImpl getNodeIfCached( int nodeId )
    {
        return nodeCache.get( nodeId );
    }

    public RelationshipImpl getRelIfCached( int nodeId )
    {
        return relCache.get( nodeId );
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyRemoveMap(
        NeoPrimitive primitive )
    {
        return lockReleaser.getCowPropertyRemoveMap( primitive );
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        NeoPrimitive primitive )
    {
        return lockReleaser.getCowPropertyAddMap( primitive );
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        NeoPrimitive primitive, boolean create )
    {
        return lockReleaser.getCowPropertyAddMap( primitive, create );
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyRemoveMap(
        NeoPrimitive primitive, boolean create )
    {
        return lockReleaser.getCowPropertyRemoveMap( primitive, create );
    }

    LockReleaser getLockReleaser()
    {
        return this.lockReleaser;
    }
}