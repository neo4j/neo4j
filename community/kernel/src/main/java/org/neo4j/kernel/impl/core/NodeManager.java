/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.TransactionManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.impl.cache.GCResistantCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.NoCache;
import org.neo4j.kernel.impl.cache.SoftLruCache;
import org.neo4j.kernel.impl.cache.StrongReferenceCache;
import org.neo4j.kernel.impl.cache.WeakLruCache;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.LockException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class NodeManager
    implements Lifecycle
{
    public static class Configuration
    {
        public static final GraphDatabaseSetting.BooleanSetting use_adaptive_cache = GraphDatabaseSettings.use_adaptive_cache;
        public static final GraphDatabaseSetting.FloatSetting adaptive_cache_heap_ratio = GraphDatabaseSettings.adaptive_cache_heap_ratio;
        public static final GraphDatabaseSetting.IntegerSetting min_node_cache_size = GraphDatabaseSettings.min_node_cache_size;

        public static final GraphDatabaseSetting.IntegerSetting min_relationship_cache_size = GraphDatabaseSettings.min_relationship_cache_size;

        public static final GraphDatabaseSetting.IntegerSetting max_node_cache_size = GraphDatabaseSettings.max_node_cache_size;

        public static final GraphDatabaseSetting.IntegerSetting max_relationship_cache_size = GraphDatabaseSettings.max_relationship_cache_size;

        public static final GraphDatabaseSetting.StringSetting node_cache_size = GraphDatabaseSettings.node_cache_size;
        public static final GraphDatabaseSetting.StringSetting relationship_cache_size = GraphDatabaseSettings.relationship_cache_size;
        public static final GraphDatabaseSetting.FloatSetting node_cache_array_fraction = GraphDatabaseSettings.node_cache_array_fraction;
        public static final GraphDatabaseSetting.FloatSetting relationship_cache_array_fraction = GraphDatabaseSettings.relationship_cache_array_fraction;
        public static final GraphDatabaseSetting.StringSetting array_cache_min_log_interval = GraphDatabaseSettings.array_cache_min_log_interval;
    }
    
    private static Logger log = Logger.getLogger( NodeManager.class.getName() );

    private long referenceNodeId = 0;

    private Config config;

    private final GraphDatabaseService graphDbService;
    private final Cache<NodeImpl> nodeCache;
    private final Cache<RelationshipImpl> relCache;

    private final CacheType cacheType;

    private final LockManager lockManager;
    private final TransactionManager transactionManager;
    private final LockReleaser lockReleaser;
    private final PropertyIndexManager propertyIndexManager;
    private final RelationshipTypeHolder relTypeHolder;
    private final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;

    private final NodeProxy.NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relationshipLookups;

    private final List<PropertyTracker<Node>> nodePropertyTrackers;
    private final List<PropertyTracker<Relationship>> relationshipPropertyTrackers;

    private boolean useAdaptiveCache;

    private static final int INDEX_COUNT = 2500;

    private static final int LOCK_STRIPE_COUNT = 32;
    private final ReentrantLock loadLocks[] =
        new ReentrantLock[LOCK_STRIPE_COUNT];
    private GraphProperties graphProperties;

    public NodeManager( Config config, GraphDatabaseService graphDb, LockManager lockManager,
            LockReleaser lockReleaser, TransactionManager transactionManager,
            PersistenceManager persistenceManager, EntityIdGenerator idGenerator,
            RelationshipTypeHolder relationshipTypeHolder, CacheType cacheType, PropertyIndexManager propertyIndexManager,
            NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups, StringLogger logger, 
            DiagnosticsManager diagnostics )
    {
        this.config = config;
        this.graphDbService = graphDb;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.propertyIndexManager = propertyIndexManager;
        this.lockReleaser = lockReleaser;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
        this.relTypeHolder = relationshipTypeHolder;

        this.cacheType = cacheType;
        this.nodeCache = diagnostics.tryAppendProvider( cacheType.node( logger, config ) );
        this.relCache =  diagnostics.tryAppendProvider( cacheType.relationship( logger, config ) );
        for ( int i = 0; i < loadLocks.length; i++ )
        {
            loadLocks[i] = new ReentrantLock();
        }
        nodePropertyTrackers = new LinkedList<PropertyTracker<Node>>();
        relationshipPropertyTrackers = new LinkedList<PropertyTracker<Relationship>>();
        this.graphProperties = instantiateGraphProperties();
    }

    public GraphDatabaseService getGraphDbService()
    {
        return graphDbService;
    }

    public CacheType getCacheType()
    {
        return this.cacheType;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start( )
    {
        // load and verify from PS
        NameData[] relTypes = null;
        NameData[] propertyIndexes = null;
        // beginTx();
        relTypes = persistenceManager.loadAllRelationshipTypes();
        propertyIndexes = persistenceManager.loadPropertyIndexes( INDEX_COUNT );
        // commitTx();
        addRawRelationshipTypes( relTypes );
        addPropertyIndexes( propertyIndexes );
        if ( propertyIndexes.length < INDEX_COUNT )
        {
            setHasAllpropertyIndexes( true );
        }

//        useAdaptiveCache = config.use_adaptive_cache(false);
//        float adaptiveCacheHeapRatio = config.adaptive_cache_heap_ratio( 0.77f, 0.1f, 0.95f );
//        int minNodeCacheSize = config.min_node_cache_size( 0 );
//        int minRelCacheSize = config.min_relationship_cache_size( 0 );
//        int maxNodeCacheSize = config.max_node_cache_size( 1500 );
//        int maxRelCacheSize = config.max_relationship_cache_size( 3500 );

    }

    @Override
    public void stop()
    {
        clearCache();
    }

    @Override
    public void shutdown()
    {
        if ( nodeCache instanceof GCResistantCache )
        {
            ((GCResistantCache<NodeImpl>) nodeCache).printStatistics();
        }
        if ( relCache instanceof GCResistantCache )
        {
            ((GCResistantCache<RelationshipImpl>) relCache).printStatistics();
        }
        nodeCache.clear();
        relCache.clear();
    }

    public Node createNode()
    {
        long id = idGenerator.nextId( Node.class );
        NodeImpl node = new NodeImpl( id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue(), true );
        NodeProxy proxy = new NodeProxy( id, nodeLookup );
        acquireLock( proxy, LockType.WRITE );
        boolean success = false;
        try
        {
            persistenceManager.nodeCreate( id );
            // nodeCache.put( id, node );
            nodeCache.put( node );
            success = true;
            return proxy;
        }
        finally
        {
            releaseLock( proxy, LockType.WRITE );
            if ( !success )
            {
                setRollbackOnly();
            }
        }
    }

    public NodeProxy newNodeProxyById( long id )
    {
        return new NodeProxy( id, nodeLookup );
    }

    public Relationship createRelationship( Node startNodeProxy, NodeImpl startNode, Node endNode,
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
        long startNodeId = startNode.getId();
        long endNodeId = endNode.getId();
        NodeImpl secondNode = getLightNode( endNodeId );
        if ( secondNode == null )
        {
            setRollbackOnly();
            throw new NotFoundException( "Second node[" + endNode.getId()
                + "] deleted" );
        }
        long id = idGenerator.nextId( Relationship.class );
        int typeId = getRelationshipTypeIdFor( type );
        RelationshipImpl rel = newRelationshipImpl( id, startNodeId, endNodeId, type, typeId, true );
        boolean firstNodeTaken = false;
        boolean secondNodeTaken = false;
        RelationshipProxy proxy = new RelationshipProxy( id, relationshipLookups );
        acquireLock( proxy, LockType.WRITE );
        boolean success = false;
        try
        {
            acquireLock( startNodeProxy, LockType.WRITE );
            firstNodeTaken = true;
            acquireLock( endNode, LockType.WRITE );
            secondNodeTaken = true;
            persistenceManager.relationshipCreate( id, typeId, startNodeId,
                endNodeId );
            if ( startNodeId == endNodeId )
            {
                startNode.addRelationship( this, type, id, DirectionWrapper.BOTH );
            }
            else
            {
                startNode.addRelationship( this, type, id, DirectionWrapper.OUTGOING );
                secondNode.addRelationship( this, type, id, DirectionWrapper.INCOMING );
            }
            // relCache.put( rel.getId(), rel );
            relCache.put( rel );
            success = true;
            return proxy;
        }
        finally
        {
            boolean releaseFailed = false;
            if ( firstNodeTaken )
            {
                try
                {
                    releaseLock( startNodeProxy, LockType.WRITE );
                }
                catch ( Exception e )
                {
                    releaseFailed = true;
                    log.log( Level.SEVERE, "Failed to release lock", e );
                }
            }
            if ( secondNodeTaken )
            {
                try
                {
                    releaseLock( endNode, LockType.WRITE );
                }
                catch ( Exception e )
                {
                    releaseFailed = true;
                    log.log( Level.SEVERE, "Failed to release lock", e );
                }
            }
            releaseLock( proxy, LockType.WRITE );
            if ( !success )
            {
                setRollbackOnly();
            }
            if ( releaseFailed )
            {
                throw new LockException( "Unable to release locks ["
                    + startNode + "," + endNode + "] in relationship create->"
                    + rel );
            }
        }
    }

    private RelationshipImpl newRelationshipImpl( long id, long startNodeId, long endNodeId,
            RelationshipType type, int typeId, boolean newRel )
    {
//        int rest = (int)(((startNodeId|endNodeId)&0xFFFFC0000000L)>>30);
//        if ( rest == 0 && typeId < 16 )
//        {
//            return new SuperLowRelationshipImpl( id, startNodeId, endNodeId, typeId, newRel );
//        }
//        return rest <= 3 ?
//                new LowRelationshipImpl( id, startNodeId, endNodeId, type, newRel ) :
//                new HighRelationshipImpl( id, startNodeId, endNodeId, type, newRel );
        return new RelationshipImpl( id, startNodeId, endNodeId, typeId, newRel );
    }

    private ReentrantLock lockId( long id )
    {
        // TODO: Change stripe mod for new 4B+
        int stripe = (int) (id / 32768) % LOCK_STRIPE_COUNT;
        if ( stripe < 0 )
        {
            stripe *= -1;
        }
        ReentrantLock lock = loadLocks[stripe];
        lock.lock();
        return lock;
    }

    protected Node getNodeByIdOrNull( long nodeId )
    {
        NodeImpl node = nodeCache.get( nodeId );
        if ( node != null )
        {
            return new NodeProxy( nodeId, nodeLookup );
        }
        ReentrantLock loadLock = lockId( nodeId );
        try
        {
            if ( nodeCache.get( nodeId ) != null )
            {
                return new NodeProxy( nodeId, nodeLookup );
            }
            NodeRecord record = persistenceManager.loadLightNode( nodeId );
            if ( record == null ) return null;
            node = new NodeImpl( nodeId, record.getCommittedNextRel(), record.getCommittedNextProp() );
            nodeCache.put( node );
            return new NodeProxy( nodeId, nodeLookup );
        }
        finally
        {
            loadLock.unlock();
        }
    }
    
    public Node getNodeById( long nodeId ) throws NotFoundException
    {
        Node node = getNodeByIdOrNull( nodeId );
        if ( node == null )
        {
            throw new NotFoundException( "Node[" + nodeId + "]" );
        }
        return node;
    }

    public RelationshipProxy newRelationshipProxyById( long id )
    {
        return new RelationshipProxy( id, relationshipLookups);
    }


    public Iterator<Node> getAllNodes()
    {
        final long highId = getHighestPossibleIdInUse( Node.class );
        return new PrefetchingIterator<Node>()
        {
            private long currentId;
            
            @Override
            protected Node fetchNextOrNull()
            {
                while ( currentId <= highId )
                {
                    try
                    {
                        Node node = getNodeByIdOrNull( currentId );
                        if ( node != null )
                        {
                            return node;
                        }
                    }
                    finally
                    {
                        currentId++;
                    }
                }
                return null;
            }
        };
    }

    NodeImpl getLightNode( long nodeId )
    {
        NodeImpl node = nodeCache.get( nodeId );
        if ( node != null )
        {
            return node;
        }
        ReentrantLock loadLock = lockId( nodeId );
        try
        {
            node = nodeCache.get( nodeId );
            if ( node != null )
            {
                return node;
            }
            NodeRecord record = persistenceManager.loadLightNode( nodeId );
            if ( record == null ) return null;
            node = new NodeImpl( nodeId, record.getCommittedNextRel(), record.getCommittedNextProp() );
//            nodeCache.put( nodeId, node );
            nodeCache.put( node );
            return node;
        }
        finally
        {
            loadLock.unlock();
        }
    }

    public NodeImpl getNodeForProxy( long nodeId, LockType lock )
    {
        if ( lock != null )
            acquireTxBoundLock( new NodeProxy( nodeId, nodeLookup ), lock );
        NodeImpl node = getLightNode( nodeId );
        if ( node == null ) throw new NotFoundException( "Node[" + nodeId + "] not found." );
        return node;
    }

    public Node getReferenceNode() throws NotFoundException
    {
        if ( referenceNodeId == -1 )
        {
            throw new NotFoundException( "No reference node set" );
        }
        return getNodeById( referenceNodeId );
    }

    public void setReferenceNodeId( long nodeId )
    {
        this.referenceNodeId = nodeId;
    }

    protected Relationship getRelationshipByIdOrNull( long relId )
    {
        RelationshipImpl relationship = relCache.get( relId );
        if ( relationship != null )
        {
            return new RelationshipProxy( relId, relationshipLookups );
        }
        ReentrantLock loadLock = lockId( relId );
        try
        {
            relationship = relCache.get( relId );
            if ( relationship != null )
            {
                return new RelationshipProxy( relId, relationshipLookups );
            }
            RelationshipRecord data = persistenceManager.loadLightRelationship( relId );
            if ( data == null )
            {
                return null;
            }
            int typeId = data.getType();
            RelationshipType type = getRelationshipTypeById( typeId );
            if ( type == null )
            {
                throw new NotFoundException( "Relationship[" + data.getId()
                    + "] exist but relationship type[" + typeId
                    + "] not found." );
            }
            final long startNodeId = data.getFirstNode();
            final long endNodeId = data.getSecondNode();
            relationship = newRelationshipImpl( relId, startNodeId, endNodeId, type, typeId, false );
            relCache.put( relationship );
            return new RelationshipProxy( relId, relationshipLookups );
        }
        finally
        {
            loadLock.unlock();
        }
    }
    
    public Relationship getRelationshipById( long id ) throws NotFoundException
    {
        Relationship relationship = getRelationshipByIdOrNull( id );
        if ( relationship == null )
        {
            throw new NotFoundException( "Relationship[" + id + "]" );
        }
        return relationship;
    }

    public Iterator<Relationship> getAllRelationships()
    {
        final long highId = getHighestPossibleIdInUse( Relationship.class );
        return new PrefetchingIterator<Relationship>()
        {
            private long currentId;
            
            @Override
            protected Relationship fetchNextOrNull()
            {
                while ( currentId <= highId )
                {
                    try
                    {
                        Relationship relationship = getRelationshipByIdOrNull( currentId );
                        if ( relationship != null )
                        {
                            return relationship;
                        }
                    }
                    finally
                    {
                        currentId++;
                    }
                }
                return null;
            }
        };
    }
    
    RelationshipType getRelationshipTypeById( int id )
    {
        return relTypeHolder.getRelationshipType( id );
    }

    public RelationshipImpl getRelationshipForProxy( long relId, LockType lock )
    {
        if ( lock != null )
            acquireTxBoundLock( new RelationshipProxy( relId, relationshipLookups ), lock );
        RelationshipImpl relationship = relCache.get( relId );
        if ( relationship != null ) return relationship;
        ReentrantLock loadLock = lockId( relId );
        try
        {
            relationship = relCache.get( relId );
            if ( relationship != null )
            {
                return relationship;
            }
            RelationshipRecord data = persistenceManager.loadLightRelationship( relId );
            if ( data == null )
            {
                throw new NotFoundException( "Relationship[" + relId + "] not found." );
            }
            int typeId = data.getType();
            RelationshipType type = getRelationshipTypeById( typeId );
            if ( type == null )
            {
                throw new NotFoundException( "Relationship[" + data.getId()
                    + "] exist but relationship type[" + typeId
                    + "] not found." );
            }
            relationship = newRelationshipImpl( relId, data.getFirstNode(), data.getSecondNode(),
                    type, typeId, false );
            // relCache.put( relId, relationship );
            relCache.put( relationship );
            return relationship;
        }
        finally
        {
            loadLock.unlock();
        }
    }

    public void removeNodeFromCache( long nodeId )
    {
        nodeCache.remove( nodeId );
    }

    public void removeRelationshipFromCache( long relId )
    {
        relCache.remove( relId );
    }

    Object loadPropertyValue( PropertyData property )
    {
        return persistenceManager.loadPropertyValue( property );
    }

    long getRelationshipChainPosition( NodeImpl node )
    {
        return persistenceManager.getRelationshipChainPosition( node.getId() );
    }

    // Triplet<ArrayMap<String,RelIdArray>,Map<Long,RelationshipImpl>,Long> getMoreRelationships( NodeImpl node )
    Triplet<ArrayMap<String,RelIdArray>,List<RelationshipImpl>,Long> getMoreRelationships( NodeImpl node )
    {
        long nodeId = node.getId();
        long position = node.getRelChainPosition();
        Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> rels =
            persistenceManager.getMoreRelationships( nodeId, position );
        ArrayMap<String,RelIdArray> newRelationshipMap =
            new ArrayMap<String,RelIdArray>();
        // Map<Long,RelationshipImpl> relsMap = new HashMap<Long,RelationshipImpl>( 150 );
        List<RelationshipImpl> relsList = new ArrayList<RelationshipImpl>( 150 );

        Iterable<RelationshipRecord> loops = rels.first().get( DirectionWrapper.BOTH );
        boolean hasLoops = loops != null;
        if ( hasLoops )
        {
            receiveRelationships( loops, newRelationshipMap, relsList, DirectionWrapper.BOTH, true );
        }
        receiveRelationships( rels.first().get( DirectionWrapper.OUTGOING ), newRelationshipMap,
                relsList, DirectionWrapper.OUTGOING, hasLoops );
        receiveRelationships( rels.first().get( DirectionWrapper.INCOMING ), newRelationshipMap,
                relsList, DirectionWrapper.INCOMING, hasLoops );

        // relCache.putAll( relsMap );
        return Triplet.of( newRelationshipMap, relsList, rels.other() );
    }

//    private void receiveRelationships(
//            Iterable<RelationshipRecord> rels, ArrayMap<String, RelIdArray> newRelationshipMap,
//            Map<Long, RelationshipImpl> relsMap, DirectionWrapper dir, boolean hasLoops )
    private void receiveRelationships(
            Iterable<RelationshipRecord> rels, ArrayMap<String, RelIdArray> newRelationshipMap,
            List<RelationshipImpl> relsList, DirectionWrapper dir, boolean hasLoops )
    {
        for ( RelationshipRecord rel : rels )
        {
            long relId = rel.getId();
            RelationshipImpl relImpl = relCache.get( relId );
            RelationshipType type = null;
            if ( relImpl == null )
            {
                type = getRelationshipTypeById( rel.getType() );
                assert type != null;
                relImpl = newRelationshipImpl( relId, rel.getFirstNode(), rel.getSecondNode(), type,
                        rel.getType(), false );
//                relsMap.put( relId, relImpl );
                relsList.add( relImpl );
            }
            else
            {
                type = getRelationshipTypeById( relImpl.getTypeId());
            }
            RelIdArray relationshipSet = newRelationshipMap.get( type.name() );
            if ( relationshipSet == null )
            {
                relationshipSet = hasLoops ? new RelIdArrayWithLoops( type.name() ) : new RelIdArray( type.name() );
                newRelationshipMap.put( type.name(), relationshipSet );
            }
            relationshipSet.add( relId, dir );
        }
    }

//    void putAllInRelCache( Map<Long,RelationshipImpl> map )
//    {
//         relCache.putAll( map );
//    }

    void putAllInRelCache( Collection<RelationshipImpl> relationships  )
    {
         relCache.putAll( relationships );
    }
    
    ArrayMap<Integer, PropertyData> loadGraphProperties( boolean light )
    {
        return persistenceManager.graphLoadProperties( light );
    }

    ArrayMap<Integer, PropertyData> loadProperties( NodeImpl node, boolean light )
    {
        return persistenceManager.loadNodeProperties( node.getId(), light );
    }

    ArrayMap<Integer,PropertyData> loadProperties(
            RelationshipImpl relationship, boolean light )
    {
        return persistenceManager.loadRelProperties( relationship.getId(), light );
    }

    public void clearCache()
    {
        nodeCache.clear();
        relCache.clear();
        graphProperties = instantiateGraphProperties();
    }

    @SuppressWarnings( "unchecked" )
    public Iterable<? extends Cache<?>> caches()
    {
        return Arrays.asList( nodeCache, relCache );
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

    public <T extends PropertyContainer> T indexPutIfAbsent( Index<T> index, T entity, String key, Object value )
    {
        T existing = index.get( key, value ).getSingle();
        if ( existing != null ) return existing;

        // Grab lock
        IndexLock lock = new IndexLock( index.getName(), key );
        LockType.WRITE.acquire( lock, lockManager );
        try
        {
            // Check again
            existing = index.get( key, value ).getSingle();
            if ( existing != null )
            {
                LockType.WRITE.release( lock, lockManager );
                return existing;
            }

            // Add
            index.add( entity, key, value );
            return null;
        }
        finally
        {
            if ( existing == null ) LockType.WRITE.unacquire( lock, lockManager, lockReleaser );
        }
    }

    void acquireLock( Primitive resource, LockType lockType )
    {
        lockType.acquire( resource.asProxy( this ), lockManager );
    }
    
    void acquireLock( PropertyContainer resource, LockType lockType )
    {
        lockType.acquire( resource, lockManager );
    }

    void acquireTxBoundLock( PropertyContainer resource, LockType lockType )
    {
        lockType.acquire( resource, lockManager );
        lockType.unacquire( resource, lockManager, lockReleaser );
    }

    void acquireIndexLock( String index, String key, LockType lockType )
    {
        lockType.acquire( new IndexLock( index, key ), lockManager );
    }

    void releaseLock( Primitive resource, LockType lockType )
    {
        lockType.unacquire( resource.asProxy( this ), lockManager, lockReleaser );
    }

    void releaseLock( PropertyContainer resource, LockType lockType )
    {
        lockType.unacquire( resource, lockManager, lockReleaser );
    }

    void releaseIndexLock( String index, String key, LockType lockType )
    {
        lockType.unacquire( new IndexLock( index, key ), lockManager, lockReleaser );
    }

    public static class IndexLock
    {
        private final String index;
        private final String key;

        public IndexLock( String index, String key )
        {
            this.index = index;
            this.key = key;
        }

        public String getIndex()
        {
            return index;
        }

        public String getKey()
        {
            return key;
        }

        @Override
        public int hashCode()
        {   // Auto-generated
            final int prime = 31;
            int result = 1;
            result = prime * result + ((index == null) ? 0 : index.hashCode());
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {   // Auto-generated
            if ( this == obj )
                return true;
            if ( obj == null )
                return false;
            if ( getClass() != obj.getClass() )
                return false;
            IndexLock other = (IndexLock) obj;
            if ( index == null )
            {
                if ( other.index != null )
                    return false;
            }
            else if ( !index.equals( other.index ) )
                return false;
            if ( key == null )
            {
                if ( other.key != null )
                    return false;
            }
            else if ( !key.equals( other.key ) )
                return false;
            return true;
        }

        @Override
        public String toString()
        {
            return "IndexLock[" + index + ":" + key + "]";
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

    void addPropertyIndexes( NameData[] propertyIndexes )
    {
        propertyIndexManager.addPropertyIndexes( propertyIndexes );
    }

    void setHasAllpropertyIndexes( boolean hasAll )
    {
        propertyIndexManager.setHasAll( hasAll );
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

    void addRawRelationshipTypes( NameData[] relTypes )
    {
        relTypeHolder.addRawRelationshipTypes( relTypes );
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return relTypeHolder.getRelationshipTypes();
    }

    ArrayMap<Integer,PropertyData> deleteNode( NodeImpl node )
    {
        deletePrimitive( node );
        return persistenceManager.nodeDelete( node.getId() );
        // remove from node cache done via event
    }

    PropertyData nodeAddProperty( NodeImpl node, PropertyIndex index, Object value )
    {
        if ( !nodePropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Node> nodePropertyTracker : nodePropertyTrackers )
            {
                nodePropertyTracker.propertyAdded( getNodeById( node.getId() ),
                        index.getKey(), value );
            }
        }
        return persistenceManager.nodeAddProperty( node.getId(), index, value );
    }

    PropertyData nodeChangeProperty( NodeImpl node, PropertyData property,
            Object value )
    {
        if ( !nodePropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Node> nodePropertyTracker : nodePropertyTrackers )
            {
                nodePropertyTracker.propertyChanged(
                        getNodeById( node.getId() ),
                        getIndexFor( property.getIndex() ).getKey(),
                        property.getValue(), value );
            }
        }
        return persistenceManager.nodeChangeProperty( node.getId(), property,
                                                      value );
    }

    void nodeRemoveProperty( NodeImpl node, PropertyData property )
    {
        if ( !nodePropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Node> nodePropertyTracker : nodePropertyTrackers )
            {
                nodePropertyTracker.propertyRemoved(
                        getNodeById( node.getId() ),
                        getIndexFor( property.getIndex() ).getKey(),
                        property.getValue() );
            }
        }
        persistenceManager.nodeRemoveProperty( node.getId(), property );
    }

    PropertyData graphAddProperty( PropertyIndex index, Object value )
    {
        return persistenceManager.graphAddProperty( index, value );
    }

    PropertyData graphChangeProperty( PropertyData property, Object value )
    {
        return persistenceManager.graphChangeProperty( property, value );
    }

    void graphRemoveProperty( PropertyData property )
    {
        persistenceManager.graphRemoveProperty( property );
    }
    
    ArrayMap<Integer,PropertyData> deleteRelationship( RelationshipImpl rel )
    {
        deletePrimitive( rel );
        return persistenceManager.relDelete( rel.getId() );
        // remove in rel cache done via event
    }

    PropertyData relAddProperty( RelationshipImpl rel, PropertyIndex index,
        Object value )
    {
        if ( !relationshipPropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Relationship> relPropertyTracker : relationshipPropertyTrackers )
            {
                relPropertyTracker.propertyAdded(
                        getRelationshipById( rel.getId() ), index.getKey(),
                        value );
            }
        }
        return persistenceManager.relAddProperty( rel.getId(), index, value );
    }

    PropertyData relChangeProperty( RelationshipImpl rel,
            PropertyData property, Object value )
    {
        if ( !relationshipPropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Relationship> relPropertyTracker : relationshipPropertyTrackers )
            {
                relPropertyTracker.propertyChanged(
                        getRelationshipById( rel.getId() ),
                        getIndexFor( property.getIndex() ).getKey(),
                        property.getValue(), value );
            }
        }
        return persistenceManager.relChangeProperty( rel.getId(), property,
                                                     value );
    }

    void relRemoveProperty( RelationshipImpl rel, PropertyData property )
    {
        if ( !relationshipPropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Relationship> relPropertyTracker : relationshipPropertyTrackers )
            {
                relPropertyTracker.propertyRemoved(
                        getRelationshipById( rel.getId() ),
                        getIndexFor( property.getIndex() ).getKey(),
                        property.getValue() );
            }
        }
        persistenceManager.relRemoveProperty( rel.getId(), property );
    }

    public Collection<Long> getCowRelationshipRemoveMap( NodeImpl node, String type )
    {
        return lockReleaser.getCowRelationshipRemoveMap( node, type );
    }

    public Collection<Long> getOrCreateCowRelationshipRemoveMap( NodeImpl node, String type )
    {
        return lockReleaser.getOrCreateCowRelationshipRemoveMap( node, type );
    }

    public ArrayMap<String,RelIdArray> getCowRelationshipAddMap( NodeImpl node )
    {
        return lockReleaser.getCowRelationshipAddMap( node );
    }

    public RelIdArray getCowRelationshipAddMap( NodeImpl node, String string )
    {
        return lockReleaser.getCowRelationshipAddMap( node, string );
    }

    public RelIdArray getOrCreateCowRelationshipAddMap( NodeImpl node, String string )
    {
        return lockReleaser.getOrCreateCowRelationshipAddMap( node, string );
    }

    public NodeImpl getNodeIfCached( long nodeId )
    {
        return nodeCache.get( nodeId );
    }

    public RelationshipImpl getRelIfCached( long nodeId )
    {
        return relCache.get( nodeId );
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyRemoveMap(
        Primitive primitive )
    {
        return lockReleaser.getCowPropertyRemoveMap( primitive );
    }

    private void deletePrimitive( Primitive primitive )
    {
        lockReleaser.deletePrimitive( primitive );
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        Primitive primitive )
    {
        return lockReleaser.getCowPropertyAddMap( primitive );
    }

    public ArrayMap<Integer,PropertyData> getOrCreateCowPropertyAddMap(
        Primitive primitive )
    {
        return lockReleaser.getOrCreateCowPropertyAddMap( primitive );
    }

    public ArrayMap<Integer,PropertyData> getOrCreateCowPropertyRemoveMap(
        Primitive primitive )
    {
        return lockReleaser.getOrCreateCowPropertyRemoveMap( primitive );
    }

    LockReleaser getLockReleaser()
    {
        return this.lockReleaser;
    }
    
    LockManager getLockManager()
    {
        return this.lockManager;
    }

    void addRelationshipType( NameData type )
    {
        relTypeHolder.addRawRelationshipType( type );
    }

    void addPropertyIndex( NameData index )
    {
        propertyIndexManager.addPropertyIndex( index );
    }

    public TransactionData getTransactionData()
    {
        return lockReleaser.getTransactionData();
    }

    RelIdArray getCreatedNodes()
    {
        return persistenceManager.getCreatedNodes();
    }

    boolean nodeCreated( long nodeId )
    {
        return persistenceManager.isNodeCreated( nodeId );
    }

    boolean relCreated( long relId )
    {
        return persistenceManager.isRelationshipCreated( relId );
    }

    public String getKeyForProperty( PropertyData property )
    {
        // int keyId = persistenceManager.getKeyIdForProperty( property );
        return propertyIndexManager.getIndexFor( property.getIndex() ).getKey();
    }

    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return this.relTypeHolder;
    }

    public static enum CacheType
    {
        weak( "weak reference cache" )
        {
            @Override
            Cache<NodeImpl> node( StringLogger logger, Config config )
            {
                return new WeakLruCache<NodeImpl>( NODE_CACHE_NAME );
            }

            @Override
            Cache<RelationshipImpl> relationship( StringLogger logger, Config config )
            {
                return new WeakLruCache<RelationshipImpl>( RELATIONSHIP_CACHE_NAME );
            }
        },
        soft( "soft reference cache" )
        {
            @Override
            Cache<NodeImpl> node( StringLogger logger, Config config )
            {
                return new SoftLruCache<NodeImpl>( NODE_CACHE_NAME );
            }

            @Override
            Cache<RelationshipImpl> relationship( StringLogger logger, Config config )
            {
                return new SoftLruCache<RelationshipImpl>( RELATIONSHIP_CACHE_NAME );
            }
        },
        none( "no cache" )
        {
            @Override
            Cache<NodeImpl> node( StringLogger logger, Config config )
            {
                return new NoCache<NodeImpl>( NODE_CACHE_NAME );
            }

            @Override
            Cache<RelationshipImpl> relationship( StringLogger logger, Config config )
            {
                return new NoCache<RelationshipImpl>( RELATIONSHIP_CACHE_NAME );
            }
        },
        strong( "strong reference cache" )
        {
            @Override
            Cache<NodeImpl> node( StringLogger logger, Config config )
            {
                return new StrongReferenceCache<NodeImpl>( NODE_CACHE_NAME );
            }

            @Override
            Cache<RelationshipImpl> relationship( StringLogger logger, Config config )
            {
                return new StrongReferenceCache<RelationshipImpl>( RELATIONSHIP_CACHE_NAME );
            }
        },
        gcr( "GC resistant cache" )
        {
            @Override
            Cache<NodeImpl> node( StringLogger logger, Config config )
            {
                long available = Runtime.getRuntime().maxMemory();
                long defaultMem = ( available / 4);
                long node = config.isSet( Configuration.node_cache_size ) ? config.getSize( Configuration.node_cache_size ) : defaultMem;
                long rel = config.isSet( Configuration.relationship_cache_size ) ? config.getSize(Configuration.relationship_cache_size) : defaultMem;
                checkMemToUse( logger, node, rel, available );
                return new GCResistantCache<NodeImpl>( node, config.getFloat( Configuration.node_cache_array_fraction ), config.getDuration( Configuration.array_cache_min_log_interval ),
                        NODE_CACHE_NAME, logger );
            }

            @Override
            Cache<RelationshipImpl> relationship( StringLogger logger, Config config )
            {
                long available = Runtime.getRuntime().maxMemory();
                long defaultMem = ( available / 4);
                long node = config.isSet( Configuration.node_cache_size ) ? config.getSize( Configuration.node_cache_size ) : defaultMem;
                long rel = config.isSet( Configuration.relationship_cache_size ) ? config.getSize(Configuration.relationship_cache_size) : defaultMem;
                checkMemToUse( logger, node, rel, available );
                return new GCResistantCache<RelationshipImpl>( rel, config.getFloat( Configuration.relationship_cache_array_fraction ), config.getDuration( Configuration.array_cache_min_log_interval ),
                        RELATIONSHIP_CACHE_NAME, logger );
            }

            @SuppressWarnings( "boxing" )
            private void checkMemToUse( StringLogger logger, long node, long rel, long available )
            {
                long advicedMax = available / 2;
                long total = 0;
                node = Math.max( GCResistantCache.MIN_SIZE, node );
                total += node;
                rel = Math.max( GCResistantCache.MIN_SIZE, rel );
                total += rel;
                if ( total > available )
                    throw new IllegalArgumentException(
                                                        String.format( "Configured cache memory limits (node=%s, relationship=%s, total=%s) exceeds available heap space (%s)",
                                                                       node, rel, total, available ) );
                if ( total > advicedMax )
                    logger.logMessage( String.format( "Configured cache memory limits(node=%s, relationship=%s, total=%s) exceeds recommended limit (%s)",
                                                      node, rel, total, advicedMax ) );
            }
        };


        private static final String NODE_CACHE_NAME = "NodeCache";
        private static final String RELATIONSHIP_CACHE_NAME = "RelationshipCache";

        private final String description;

        private CacheType( String description )
        {
            this.description = description;
        }

        abstract Cache<NodeImpl> node( StringLogger logger, Config config );

        abstract Cache<RelationshipImpl> relationship( StringLogger logger, Config config );

        public String getDescription()
        {
            return this.description;
        }
    }

    public void addNodePropertyTracker(
            PropertyTracker<Node> nodePropertyTracker )
    {
        nodePropertyTrackers.add( nodePropertyTracker );
    }

    public void removeNodePropertyTracker(
            PropertyTracker<Node> nodePropertyTracker )
    {
        nodePropertyTrackers.remove( nodePropertyTracker );
    }

    public void addRelationshipPropertyTracker(
            PropertyTracker<Relationship> relationshipPropertyTracker )
    {
        relationshipPropertyTrackers.add( relationshipPropertyTracker );
    }

    public void removeRelationshipPropertyTracker(
            PropertyTracker<Relationship> relationshipPropertyTracker )
    {
        relationshipPropertyTrackers.remove( relationshipPropertyTracker );
    }
    
    PersistenceManager getPersistenceManager()
    {
        return persistenceManager;
    }
    
    private GraphProperties instantiateGraphProperties()
    {
        return new GraphProperties( this );
    }
    
    public GraphProperties getGraphProperties()
    {
        return graphProperties;
    }

    public void removeGraphPropertiesFromCache()
    {
        graphProperties = instantiateGraphProperties();
    }

    void updateCacheSize( NodeImpl node, int newSize )
    {
        nodeCache.updateSize( node, newSize );
    }

    void updateCacheSize( RelationshipImpl rel, int newSize )
    {
        relCache.updateSize( rel, newSize );
    }
}
