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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.LockStripedCache;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class NodeManager
        implements Lifecycle
{
    private long referenceNodeId = 0;

    private StringLogger logger;
    private final GraphDatabaseService graphDbService;
    private final LockStripedCache<NodeImpl> nodeCache;
    private final LockStripedCache<RelationshipImpl> relCache;

    private final CacheProvider cacheProvider;

    private final AbstractTransactionManager transactionManager;
    private final PropertyIndexManager propertyIndexManager;
    private final RelationshipTypeHolder relTypeHolder;
    private final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;
    private final XaDataSourceManager xaDsm;
    private final ThreadToStatementContextBridge statementCtxProvider;

    private final NodeProxy.NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relationshipLookups;

    private final List<PropertyTracker<Node>> nodePropertyTrackers;
    private final List<PropertyTracker<Relationship>> relationshipPropertyTrackers;

    private static final int LOCK_STRIPE_COUNT = 32;
    private GraphProperties graphProperties;

    private NodeManagerDatasourceListener dataSourceListener;

    private final LockStripedCache.Loader<NodeImpl> nodeLoader = new LockStripedCache.Loader<NodeImpl>()
    {
        @Override
        public NodeImpl loadById( long id )
        {
            NodeRecord record = persistenceManager.loadLightNode( id );
            if ( record == null )
            {
                return null;
            }
            return new NodeImpl( id, record.getCommittedNextRel(), record.getCommittedNextProp() );
        }
    };

    private final LockStripedCache.Loader<RelationshipImpl> relLoader = new LockStripedCache.Loader<RelationshipImpl>()
    {
        @Override
        public RelationshipImpl loadById( long id )
        {
            RelationshipRecord data = persistenceManager.loadLightRelationship( id );
            if ( data == null )
            {
                return null;
            }
            int typeId = data.getType();
            RelationshipType type;
            try
            {
                type = getRelationshipTypeById( typeId );
            }
            catch ( KeyNotFoundException e )
            {
                throw new NotFoundException( "Relationship[" + data.getId()
                        + "] exist but relationship type[" + typeId
                        + "] not found." );
            }
            final long startNodeId = data.getFirstNode();
            final long endNodeId = data.getSecondNode();
            return newRelationshipImpl( id, startNodeId, endNodeId, type, typeId, false );
        }
    };

    public NodeManager( Config config, StringLogger logger, GraphDatabaseService graphDb,
                        AbstractTransactionManager transactionManager,
                        PersistenceManager persistenceManager, EntityIdGenerator idGenerator,
                        RelationshipTypeHolder relationshipTypeHolder, CacheProvider cacheProvider,
                        PropertyIndexManager propertyIndexManager, NodeProxy.NodeLookup nodeLookup,
                        RelationshipProxy.RelationshipLookups relationshipLookups, Cache<NodeImpl> nodeCache,
                        Cache<RelationshipImpl> relCache, XaDataSourceManager xaDsm, ThreadToStatementContextBridge
            statementCtxProvider )
    {
        this.logger = logger;
        this.graphDbService = graphDb;
        this.transactionManager = transactionManager;
        this.propertyIndexManager = propertyIndexManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
        this.relTypeHolder = relationshipTypeHolder;

        this.cacheProvider = cacheProvider;
        this.statementCtxProvider = statementCtxProvider;
        this.nodeCache = new LockStripedCache<NodeImpl>( nodeCache, LOCK_STRIPE_COUNT, nodeLoader );
        this.relCache = new LockStripedCache<RelationshipImpl>( relCache, LOCK_STRIPE_COUNT, relLoader );
        this.xaDsm = xaDsm;
        nodePropertyTrackers = new LinkedList<PropertyTracker<Node>>();
        relationshipPropertyTrackers = new LinkedList<PropertyTracker<Relationship>>();
        this.graphProperties = instantiateGraphProperties();
    }

    public GraphDatabaseService getGraphDbService()
    {
        return graphDbService;
    }

    public CacheProvider getCacheType()
    {
        return this.cacheProvider;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        xaDsm.addDataSourceRegistrationListener( (dataSourceListener = new NodeManagerDatasourceListener()) );
    }

    @Override
    public void stop()
    {
        xaDsm.removeDataSourceRegistrationListener( dataSourceListener );
        clearCache();
        relTypeHolder.stop();
        propertyIndexManager.stop();
    }

    @Override
    public void shutdown()
    {
        nodeCache.printStatistics();
        relCache.printStatistics();
        nodeCache.clear();
        relCache.clear();
    }

    public Node createNode()
    {
        long id = idGenerator.nextId( Node.class );
        NodeImpl node = new NodeImpl( id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue(),
                true );
        NodeProxy proxy = new NodeProxy( id, nodeLookup, statementCtxProvider );
        TransactionState transactionState = getTransactionState();
        transactionState.acquireWriteLock( proxy );
        boolean success = false;
        try
        {
            persistenceManager.nodeCreate( id );
            nodeCache.put( node );
            success = true;
            return proxy;
        }
        finally
        {
            if ( !success )
            {
                setRollbackOnly();
            }
        }
    }

    public NodeProxy newNodeProxyById( long id )
    {
        return new NodeProxy( id, nodeLookup, statementCtxProvider );
    }

    public Relationship createRelationship( Node startNodeProxy, NodeImpl startNode, Node endNode,
                                            RelationshipType type )
    {
        if ( startNode == null || endNode == null || type == null )
        {
            throw new IllegalArgumentException( "Null parameter, startNode="
                    + startNode + ", endNode=" + endNode + ", type=" + type );
        }

        int typeId = relTypeHolder.getOrCreateId( type.name() );
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
        RelationshipImpl rel = newRelationshipImpl( id, startNodeId, endNodeId, type, typeId, true );
        RelationshipProxy proxy = new RelationshipProxy( id, relationshipLookups );
        TransactionState transactionState = getTransactionState();
        transactionState.acquireWriteLock( proxy );
        boolean success = false;
        TransactionState tx = transactionState;
        try
        {
            transactionState.acquireWriteLock( startNodeProxy );
            transactionState.acquireWriteLock( endNode );
            persistenceManager.relationshipCreate( id, typeId, startNodeId, endNodeId );
            if ( startNodeId == endNodeId )
            {
                tx.getOrCreateCowRelationshipAddMap( startNode, typeId ).add( id, DirectionWrapper.BOTH );
            }
            else
            {
                tx.getOrCreateCowRelationshipAddMap( startNode, typeId ).add( id, DirectionWrapper.OUTGOING );
                tx.getOrCreateCowRelationshipAddMap( secondNode, typeId ).add( id, DirectionWrapper.INCOMING );
            }
            // relCache.put( rel.getId(), rel );
            relCache.put( rel );
            success = true;
            return proxy;
        }
        finally
        {
            if ( !success )
            {
                setRollbackOnly();
            }
        }
    }

    private RelationshipImpl newRelationshipImpl( long id, long startNodeId, long endNodeId,
                                                  RelationshipType type, int typeId, boolean newRel )
    {
        return new RelationshipImpl( id, startNodeId, endNodeId, typeId, newRel );
    }

    protected Node getNodeByIdOrNull( long nodeId )
    {
        NodeImpl node = getLightNode( nodeId );
        return node != null ? new NodeProxy( nodeId, nodeLookup, statementCtxProvider ) : null;
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

    NodeImpl getLightNode( long nodeId )
    {
        return nodeCache.get( nodeId );
    }

    public RelationshipProxy newRelationshipProxyById( long id )
    {
        return new RelationshipProxy( id, relationshipLookups );
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

    public NodeImpl getNodeForProxy( long nodeId, LockType lock )
    {
        if ( lock != null )
        {
            lock.acquire( getTransactionState(), new NodeProxy( nodeId, nodeLookup, statementCtxProvider ) );
        }
        NodeImpl node = getLightNode( nodeId );
        if ( node == null )
        {
            throw new NotFoundException( "Node[" + nodeId + "] not found." );
        }
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
        return relationship != null ? new RelationshipProxy( relId, relationshipLookups ) : null;
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

    RelationshipType getRelationshipTypeById( int id ) throws KeyNotFoundException
    {
        return relTypeHolder.getKeyById( id );
    }

    public RelationshipImpl getRelationshipForProxy( long relId, LockType lock )
    {
        if ( lock != null )
        {
            lock.acquire( getTransactionState(), new RelationshipProxy( relId, relationshipLookups ) );
        }
        return relCache.get( relId );
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

    Triplet<ArrayMap<Integer,RelIdArray>,List<RelationshipImpl>,Long> getMoreRelationships( NodeImpl node )
    {
        long nodeId = node.getId();
        long position = node.getRelChainPosition();
        Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> rels =
            persistenceManager.getMoreRelationships( nodeId, position );
        ArrayMap<Integer,RelIdArray> newRelationshipMap =
            new ArrayMap<Integer,RelIdArray>();

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

    private void receiveRelationships(
            Iterable<RelationshipRecord> rels, ArrayMap<Integer, RelIdArray> newRelationshipMap,
            List<RelationshipImpl> relsList, DirectionWrapper dir, boolean hasLoops )
    {
        for ( RelationshipRecord rel : rels )
        {
            long relId = rel.getId();
            RelationshipImpl relImpl = relCache.get( relId );
            RelationshipType type = null;
            int typeId;
            if ( relImpl == null )
            {
                typeId = rel.getType();
                try
                {
                    type = getRelationshipTypeById( typeId );
                }
                catch ( KeyNotFoundException e )
                {
                    throw new AssertionError( "Type of loaded relationships unknown" );
                }
                relImpl = newRelationshipImpl( relId, rel.getFirstNode(), rel.getSecondNode(), type,
                        typeId, false );
                relsList.add( relImpl );
            }
            else
            {
                typeId = relImpl.getTypeId();
            }
            RelIdArray relationshipSet = newRelationshipMap.get( typeId );
            if ( relationshipSet == null )
            {
                relationshipSet = hasLoops ? new RelIdArrayWithLoops( typeId ) : new RelIdArray( typeId );
                newRelationshipMap.put( typeId, relationshipSet );
            }
            relationshipSet.add( relId, dir );
        }
    }

//    void putAllInRelCache( Map<Long,RelationshipImpl> map )
//    {
//         relCache.putAll( map );
//    }

    void putAllInRelCache( Collection<RelationshipImpl> relationships )
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

    ArrayMap<Integer, PropertyData> loadProperties(
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

    @SuppressWarnings("unchecked")
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
            logger.debug( "Failed to set transaction rollback only", e );
        }
        catch ( javax.transaction.SystemException se )
        {
            // our TM never throws this exception
            logger.error( "Failed to set transaction rollback only", se );
        }
    }

    public <T extends PropertyContainer> T indexPutIfAbsent( Index<T> index, T entity, String key, Object value )
    {
        T existing = index.get( key, value ).getSingle();
        if ( existing != null )
            return existing;

        // Grab lock
        IndexLock lock = new IndexLock( index.getName(), key );
        TransactionState state = getTransactionState();
        LockElement writeLock = state.acquireWriteLock( lock );
        
        // Check again -- now holding the lock
        existing = index.get( key, value ).getSingle();
        if ( existing != null )
        {
            // Someone else created this entry, release the lock as we won't be needing it
            writeLock.release();
            return existing;
        }

        // Add
        index.add( entity, key, value );
        return null;
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
            {
                return true;
            }
            if ( obj == null )
            {
                return false;
            }
            if ( getClass() != obj.getClass() )
            {
                return false;
            }
            IndexLock other = (IndexLock) obj;
            if ( index == null )
            {
                if ( other.index != null )
                {
                    return false;
                }
            }
            else if ( !index.equals( other.index ) )
            {
                return false;
            }
            if ( key == null )
            {
                if ( other.key != null )
                {
                    return false;
                }
            }
            else if ( !key.equals( other.key ) )
            {
                return false;
            }
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
        relTypeHolder.removeKeyEntry( id );
    }

    void addPropertyIndexes( NameData[] propertyIndexes )
    {
        propertyIndexManager.addKeyEntries( propertyIndexes );
    }

    PropertyIndex getPropertyIndex( int keyId ) throws KeyNotFoundException
    {
        return propertyIndexManager.getKeyById( keyId );
    }

    PropertyIndex getPropertyIndexOrNull( int keyId )
    {
        return propertyIndexManager.getKeyByIdOrNull( keyId );
    }
    
    PropertyIndex[] index( String key )
    {
        return propertyIndexManager.index( key );
    }

    boolean hasIndexFor( int keyId )
    {
        return propertyIndexManager.hasKeyById( keyId );
    }

    int getOrCreatePropertyIndex( String key )
    {
        return propertyIndexManager.getOrCreateId( key );
    }
    
    int getRelationshipTypeIdFor( RelationshipType type ) throws KeyNotFoundException
    {
        return relTypeHolder.getIdByKey( type );
    }

    void addRawRelationshipTypes( NameData[] relTypes )
    {
        relTypeHolder.addKeyEntries( relTypes );
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return relTypeHolder.getAllKeys();
    }

    private <T extends PropertyContainer> void deleteFromTrackers( Primitive primitive, List<PropertyTracker<T>>
            trackers ) {
        if ( !trackers.isEmpty() )
        {
            Iterable<String> propertyKeys = primitive.getPropertyKeys( this );
            T proxy = (T) primitive.asProxy( this );

            for ( String key : propertyKeys )
            {
                Object value = primitive.getProperty( this, key );
                for ( PropertyTracker<T> tracker : trackers )
                {
                    tracker.propertyRemoved( proxy, key, value );
                }
            }
        }

    }

    ArrayMap<Integer, PropertyData> deleteNode( NodeImpl node, TransactionState tx )
    {
        deleteFromTrackers( node, nodePropertyTrackers );

        tx.deletePrimitive( node );
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
            Object value, TransactionState tx )
    {
        if ( !nodePropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Node> nodePropertyTracker : nodePropertyTrackers )
            {
                nodePropertyTracker.propertyChanged(
                        getNodeById( node.getId() ),
                        getPropertyIndexOrNull( property.getIndex() ).getKey(),
                        property.getValue(), value );
            }
        }
        return persistenceManager.nodeChangeProperty( node.getId(), property,
                value );
    }

    void nodeRemoveProperty( NodeImpl node, PropertyData property, TransactionState tx )
    {
        if ( !nodePropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Node> nodePropertyTracker : nodePropertyTrackers )
            {
                nodePropertyTracker.propertyRemoved(
                        getNodeById( node.getId() ),
                        getPropertyIndexOrNull( property.getIndex() ).getKey(),
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

    ArrayMap<Integer,PropertyData> deleteRelationship( RelationshipImpl rel, TransactionState tx )
    {
        deleteFromTrackers( rel, relationshipPropertyTrackers );

        tx.deletePrimitive( rel );
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
            PropertyData property, Object value, TransactionState tx )
    {
        if ( !relationshipPropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Relationship> relPropertyTracker : relationshipPropertyTrackers )
            {
                relPropertyTracker.propertyChanged(
                        getRelationshipById( rel.getId() ),
                        getPropertyIndexOrNull( property.getIndex() ).getKey(),
                        property.getValue(), value );
            }
        }
        return persistenceManager.relChangeProperty( rel.getId(), property,
                value );
    }

    void relRemoveProperty( RelationshipImpl rel, PropertyData property, TransactionState tx )
    {
        if ( !relationshipPropertyTrackers.isEmpty() )
        {
            for ( PropertyTracker<Relationship> relPropertyTracker : relationshipPropertyTrackers )
            {
                relPropertyTracker.propertyRemoved(
                        getRelationshipById( rel.getId() ),
                        getPropertyIndexOrNull( property.getIndex() ).getKey(),
                        property.getValue() );
            }
        }
        persistenceManager.relRemoveProperty( rel.getId(), property );
    }

    public NodeImpl getNodeIfCached( long nodeId )
    {
        return nodeCache.getIfCached( nodeId );
    }

    public RelationshipImpl getRelIfCached( long nodeId )
    {
        return relCache.getIfCached( nodeId );
    }

    void addRelationshipType( NameData type )
    {
        relTypeHolder.addKeyEntries( type );
    }

    void addPropertyIndex( NameData index )
    {
        propertyIndexManager.addKeyEntries( index );
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
        try
        {
            return propertyIndexManager.getKeyById( property.getIndex() ).getKey();
        }
        catch ( KeyNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "The key should exist at this point" );
        }
    }

    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return this.relTypeHolder;
    }

    public void addNodePropertyTracker( PropertyTracker<Node> nodePropertyTracker )
    {
        nodePropertyTrackers.add( nodePropertyTracker );
    }

    public void removeNodePropertyTracker( PropertyTracker<Node> nodePropertyTracker )
    {
        nodePropertyTrackers.remove( nodePropertyTracker );
    }

    public void addRelationshipPropertyTracker( PropertyTracker<Relationship> relationshipPropertyTracker )
    {
        relationshipPropertyTrackers.add( relationshipPropertyTracker );
    }

    public void removeRelationshipPropertyTracker( PropertyTracker<Relationship> relationshipPropertyTracker )
    {
        relationshipPropertyTrackers.remove( relationshipPropertyTracker );
    }

    // For compatibility reasons with Cypher
    public boolean isDeleted( PropertyContainer entity )
    {
        if ( entity instanceof Node )
            return isDeleted( (Node)entity );
        else if ( entity instanceof Relationship )
            return isDeleted( (Relationship)entity );
        else
            throw new IllegalArgumentException( "Unknown entity type: " + entity + ", " + entity.getClass() );
    }
    
    public boolean isDeleted( Node resource )
    {
        return getTransactionState().isDeleted( resource );
    }
    
    public boolean isDeleted( Relationship resource )
    {
        return getTransactionState().isDeleted( resource );
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
    
    public TransactionState getTransactionState()
    {
        return transactionManager.getTransactionState();
    }
    
    private class NodeManagerDatasourceListener implements DataSourceRegistrationListener
    {
        @Override
        public void registeredDataSource( XaDataSource ds )
        {
            if ( ds.getName().equals( Config.DEFAULT_DATA_SOURCE_NAME ) )
            {
                // Load and cache all keys from persistence manager
                addRawRelationshipTypes( persistenceManager.loadAllRelationshipTypes() );
                addPropertyIndexes( persistenceManager.loadPropertyIndexes() );
            }

        }

        @Override
        public void unregisteredDataSource( XaDataSource ds )
        {
        }
    }
}
