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

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorWrapper;
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
import org.neo4j.kernel.impl.util.RelIdIterator;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class NodeManager
        implements Lifecycle
{
    private long referenceNodeId = 0;

    private final StringLogger logger;
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
    private final ReentrantLock loadLocks[] =
            new ReentrantLock[LOCK_STRIPE_COUNT];
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
        return createNode( null );
    }

    public Node createNode( Label[] labels )
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
            if ( labels != null )
            {
                for ( int i = 0; i < labels.length; i++ )
                {
                    proxy.addLabel( labels[i] );
                }
            }

            nodeCache.put( node );
            transactionState.createNode( id );
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

    public Node getNodeByIdOrNull( long nodeId )
    {
        NodeImpl node = getLightNode( nodeId );
        return node != null ? new NodeProxy( nodeId, nodeLookup, statementCtxProvider ) : null;
    }

    public Node getNodeById( long nodeId ) throws NotFoundException
    {
        Node node = getNodeByIdOrNull( nodeId );
        if ( node == null )
        {
            throw new NotFoundException( format( "Node %d not found", nodeId ) );
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

    @SuppressWarnings("unchecked")
    public Iterator<Node> getAllNodes()
    {
        Iterator<Node> committedNodes = new PrefetchingIterator<Node>()
        {
            private long highId = getHighestPossibleIdInUse( Node.class );
            private long currentId;

            @Override
            protected Node fetchNextOrNull()
            {
                while ( true )
                {   // This outer loop is for checking if highId has changed since we started.
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

                    long newHighId = getHighestPossibleIdInUse( Node.class );
                    if ( newHighId > highId )
                    {
                        highId = newHighId;
                    }
                    else
                    {
                        break;
                    }
                }
                return null;
            }
        };

        final TransactionState txState = getTransactionState();
        if ( !txState.hasChanges() )
        {
            return committedNodes;
        }
            
        /* Created nodes are put in the cache right away, even before the transaction is committed.
         * We want this iterator to include nodes that have been created, but not yes committed in
         * this transaction. The thing with the cache is that stuff can be evicted at any point in time
         * so we can't rely on created nodes to be there during the whole life time of this iterator.
         * That's why we filter them out from the "committed/cache" iterator and add them at the end instead.*/
        final Set<Long> createdNodes = new HashSet<Long>( txState.getCreatedNodes() );
        if ( !createdNodes.isEmpty() )
        {
            committedNodes = new FilteringIterator<Node>( committedNodes, new Predicate<Node>()
            {
                @Override
                public boolean accept( Node node )
                {
                    return !createdNodes.contains( node.getId() );
                }
            } );
        }

        // Filter out nodes deleted in this transaction
        Iterator<Node> filteredRemovedNodes = new FilteringIterator<Node>( committedNodes, new Predicate<Node>()
        {
            @Override
            public boolean accept( Node node )
            {
                return !txState.nodeIsDeleted( node.getId() );
            }
        } );

        // Append nodes created in this transaction
        return new CombiningIterator<Node>( asList( filteredRemovedNodes,
                new IteratorWrapper<Node, Long>( createdNodes.iterator() )
                {
                    @Override
                    protected Node underlyingObjectToObject( Long id )
                    {
                        return getNodeById( id );
                    }
                } ) );
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
            throw new NotFoundException( format( "Node %d not found", nodeId ) );
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
            throw new NotFoundException( format( "Relationship %d not found", id ) );
        }
        return relationship;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Relationship> getAllRelationships()
    {
        Iterator<Relationship> committedRelationships = new PrefetchingIterator<Relationship>()
        {
            private long highId = getHighestPossibleIdInUse( Relationship.class );
            private long currentId;

            @Override
            protected Relationship fetchNextOrNull()
            {
                while ( true )
                {   // This outer loop is for checking if highId has changed since we started.
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

                    long newHighId = getHighestPossibleIdInUse( Node.class );
                    if ( newHighId > highId )
                    {
                        highId = newHighId;
                    }
                    else
                    {
                        break;
                    }
                }
                return null;
            }
        };

        final TransactionState txState = getTransactionState();
        if ( !txState.hasChanges() )
        {
            return committedRelationships;
        }
        
        /* Created relationships are put in the cache right away, even before the transaction is committed.
         * We want this iterator to include relationships that have been created, but not yes committed in
         * this transaction. The thing with the cache is that stuff can be evicted at any point in time
         * so we can't rely on created relationships to be there during the whole life time of this iterator.
         * That's why we filter them out from the "committed/cache" iterator and add them at the end instead.*/
        final Set<Long> createdRelationships = new HashSet<Long>( txState.getCreatedRelationships() );
        if ( !createdRelationships.isEmpty() )
        {
            committedRelationships = new FilteringIterator<Relationship>( committedRelationships,
                    new Predicate<Relationship>()
                    {
                        @Override
                        public boolean accept( Relationship relationship )
                        {
                            return !createdRelationships.contains( relationship.getId() );
                        }
                    } );
        }

        // Filter out relationships deleted in this transaction
        Iterator<Relationship> filteredRemovedRelationships =
                new FilteringIterator<Relationship>( committedRelationships, new Predicate<Relationship>()
                {
                    @Override
                    public boolean accept( Relationship relationship )
                    {
                        return !txState.relationshipIsDeleted( relationship.getId() );
                    }
                } );

        // Append relationships created in this transaction
        return new CombiningIterator<Relationship>( asList( filteredRemovedRelationships,
                new IteratorWrapper<Relationship, Long>( createdRelationships.iterator() )
                {
                    @Override
                    protected Relationship underlyingObjectToObject( Long id )
                    {
                        return getRelationshipById( id );
                    }
                } ) );
    }

    private Set<Long> asSet( RelIdIterator ids )
    {
        Set<Long> set = new HashSet<Long>();
        while ( ids.hasNext() )
        {
            set.add( ids.next() );
        }
        return set;
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
        RelationshipImpl relationship = relCache.get( relId );
        if ( relationship != null )
        {
            return relationship;
        }
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
                throw new NotFoundException( format( "Relationship %d not found", relId ) );
            }
            int typeId = data.getType();
            RelationshipType type = null;
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

    public void removeRelationshipFromCache( long id )
    {
        relCache.remove( id );
    }


    public void patchDeletedRelationshipNodes( long relId, long firstNodeId, long firstNodeNextRelId, long secondNodeId,
                                               long secondNodeNextRelId )
    {
        invalidateNode( firstNodeId, relId, firstNodeNextRelId );
        invalidateNode( secondNodeId, relId, secondNodeNextRelId );
    }

    private void invalidateNode( long nodeId, long relIdDeleted, long nextRelId )
    {
        NodeImpl node = nodeCache.getIfCached( nodeId );
        if ( node != null && node.getRelChainPosition() == relIdDeleted )
        {
            node.setRelChainPosition( nextRelId );
        }
    }

    Object loadPropertyValue( PropertyData property )
    {
        return persistenceManager.loadPropertyValue( property );
    }

    long getRelationshipChainPosition( NodeImpl node )
    {
        return persistenceManager.getRelationshipChainPosition( node.getId() );
    }

    Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, Long> getMoreRelationships( NodeImpl node )
    {
        long nodeId = node.getId();
        long position = node.getRelChainPosition();
        Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> rels =
                persistenceManager.getMoreRelationships( nodeId, position );
        ArrayMap<Integer, RelIdArray> newRelationshipMap =
                new ArrayMap<Integer, RelIdArray>();

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

    public void setRollbackOnly()
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
        {
            return existing;
        }

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
            trackers )
    {
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

    public ArrayMap<Integer, PropertyData> deleteNode( NodeImpl node, TransactionState tx )
    {
        deleteFromTrackers( node, nodePropertyTrackers );

        tx.deleteNode( node.getId() );
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

    ArrayMap<Integer, PropertyData> deleteRelationship( RelationshipImpl rel, TransactionState tx )
    {
        deleteFromTrackers( rel, relationshipPropertyTrackers );

        tx.deleteRelationship( rel.getId() );
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

    public void addRelationshipType( NameData type )
    {
        relTypeHolder.addKeyEntries( type );
    }

    public void addPropertyIndex( NameData index )
    {
        propertyIndexManager.addKeyEntries( index );
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
        {
            return isDeleted( (Node) entity );
        }
        else if ( entity instanceof Relationship )
        {
            return isDeleted( (Relationship) entity );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown entity type: " + entity + ", " + entity.getClass() );
        }
    }

    public boolean isDeleted( Node resource )
    {
        return getTransactionState().nodeIsDeleted( resource.getId() );
    }

    public boolean isDeleted( Relationship resource )
    {
        return getTransactionState().relationshipIsDeleted( resource.getId() );
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
