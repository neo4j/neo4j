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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
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
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.TokenStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.cast;

public class NodeManager implements Lifecycle, EntityFactory
{
    private final StringLogger logger;
    private final GraphDatabaseService graphDbService;
    private final AutoLoadingCache<NodeImpl> nodeCache;
    private final AutoLoadingCache<RelationshipImpl> relCache;
    private final CacheProvider cacheProvider;
    private final AbstractTransactionManager transactionManager;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relTypeHolder;
    private final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;
    private final XaDataSourceManager xaDsm;
    private final ThreadToStatementContextBridge statementCtxProvider;

    private final NodeProxy.NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relationshipLookups;

    private final List<PropertyTracker<Node>> nodePropertyTrackers;
    private final List<PropertyTracker<Relationship>> relationshipPropertyTrackers;

    private GraphPropertiesImpl graphProperties;

    private final AutoLoadingCache.Loader<NodeImpl> nodeLoader = new AutoLoadingCache.Loader<NodeImpl>()
    {
        @Override
        public NodeImpl loadById( long id )
        {
            NodeRecord record = persistenceManager.loadLightNode( id );
            if ( record == null )
            {
                return null;
            }
            return new NodeImpl( id );
        }
    };

    private final AutoLoadingCache.Loader<RelationshipImpl> relLoader = new AutoLoadingCache.Loader<RelationshipImpl>()
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
            final long startNodeId = data.getFirstNode();
            final long endNodeId = data.getSecondNode();
            return newRelationshipImpl( id, startNodeId, endNodeId, typeId, false );
        }
    };

    public NodeManager( StringLogger logger, GraphDatabaseService graphDb,
                        AbstractTransactionManager transactionManager,
                        PersistenceManager persistenceManager, EntityIdGenerator idGenerator,
                        RelationshipTypeTokenHolder relationshipTypeTokenHolder, CacheProvider cacheProvider,
                        PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                        NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups,
                        Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache,
                        XaDataSourceManager xaDsm, ThreadToStatementContextBridge statementCtxProvider )
    {
        this.logger = logger;
        this.graphDbService = graphDb;
        this.transactionManager = transactionManager;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
        this.labelTokenHolder = labelTokenHolder;
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
        this.relTypeHolder = relationshipTypeTokenHolder;

        this.cacheProvider = cacheProvider;
        this.statementCtxProvider = statementCtxProvider;
        this.nodeCache = new AutoLoadingCache<>( nodeCache, nodeLoader );
        this.relCache = new AutoLoadingCache<>( relCache, relLoader );
        this.xaDsm = xaDsm;
        nodePropertyTrackers = new LinkedList<>();
        relationshipPropertyTrackers = new LinkedList<>();
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
    {   // Nothing to initialize
    }

    @Override
    public void start()
    {
        for ( XaDataSource ds : xaDsm.getAllRegisteredDataSources() )
        {
            if ( ds.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) )
            {
                NeoStore neoStore = ((NeoStoreXaDataSource) ds).getNeoStore();

                TokenStore<?> propTokens = neoStore.getPropertyStore().getPropertyKeyTokenStore();
                TokenStore<?> labelTokens = neoStore.getLabelTokenStore();
                TokenStore<?> relTokens = neoStore.getRelationshipTypeStore();

                addRawRelationshipTypes( relTokens.getTokens( Integer.MAX_VALUE ) );
                addPropertyKeyTokens( propTokens.getTokens( Integer.MAX_VALUE ) );
                addLabelTokens( labelTokens.getTokens( Integer.MAX_VALUE ) );
            }
        }
    }

    @Override
    public void stop()
    {
        clearCache();
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
        NodeImpl node = new NodeImpl( id, true );
        NodeProxy proxy = new NodeProxy( id, nodeLookup, statementCtxProvider );
        TransactionState transactionState = getTransactionState();
        transactionState.acquireWriteLock( proxy );
        boolean success = false;
        try
        {
            persistenceManager.nodeCreate( id );
            transactionState.createNode( id );
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

    @Override
    public NodeProxy newNodeProxyById( long id )
    {
        return new NodeProxy( id, nodeLookup, statementCtxProvider );
    }

    public Relationship createRelationship( Node startNodeProxy, NodeImpl startNode, Node endNode,
                                            long relationshipTypeId )
    {
        if ( startNode == null || endNode == null || relationshipTypeId > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( "Bad parameter, startNode="
                    + startNode + ", endNode=" + endNode + ", typeId=" + relationshipTypeId );
        }

        int typeId = (int)relationshipTypeId;
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
        RelationshipImpl rel = newRelationshipImpl( id, startNodeId, endNodeId, typeId, true );
        RelationshipProxy proxy = new RelationshipProxy( id, relationshipLookups, statementCtxProvider );
        TransactionState tx = getTransactionState();
        tx.acquireWriteLock( proxy );
        boolean success = false;
        try
        {
            tx.acquireWriteLock( startNodeProxy );
            tx.acquireWriteLock( endNode );
            persistenceManager.relationshipCreate( id, typeId, startNodeId, endNodeId );
            tx.createRelationship( id );
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

    private RelationshipImpl newRelationshipImpl( long id, long startNodeId, long endNodeId, int typeId, boolean newRel)
    {
        return new RelationshipImpl( id, startNodeId, endNodeId, typeId, newRel );
    }

    public Node getNodeByIdOrNull( long nodeId )
    {
        transactionManager.assertInTransaction();
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

    @Override
    public RelationshipProxy newRelationshipProxyById( long id )
    {
        return new RelationshipProxy( id, relationshipLookups, statementCtxProvider );
    }

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
        final Set<Long> createdNodes = new HashSet<>( txState.getCreatedNodes() );
        if ( !createdNodes.isEmpty() )
        {
            committedNodes = new FilteringIterator<>( committedNodes, new Predicate<Node>()
            {
                @Override
                public boolean accept( Node node )
                {
                    return !createdNodes.contains( node.getId() );
                }
            } );
        }

        // Filter out nodes deleted in this transaction
        Iterator<Node> filteredRemovedNodes = new FilteringIterator<>( committedNodes, new Predicate<Node>()
        {
            @Override
            public boolean accept( Node node )
            {
                return !txState.nodeIsDeleted( node.getId() );
            }
        } );

        // Append nodes created in this transaction
        return new CombiningIterator<>( asList( filteredRemovedNodes,
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

    protected Relationship getRelationshipByIdOrNull( long relId )
    {
        transactionManager.assertInTransaction();
        RelationshipImpl relationship = relCache.get( relId );
        return relationship != null ? new RelationshipProxy( relId, relationshipLookups, statementCtxProvider ) : null;
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
        final Set<Long> createdRelationships = new HashSet<>( txState.getCreatedRelationships() );
        if ( !createdRelationships.isEmpty() )
        {
            committedRelationships = new FilteringIterator<>( committedRelationships,
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
                new FilteringIterator<>( committedRelationships, new Predicate<Relationship>()
                {
                    @Override
                    public boolean accept( Relationship relationship )
                    {
                        return !txState.relationshipIsDeleted( relationship.getId() );
                    }
                } );

        // Append relationships created in this transaction
        return new CombiningIterator<>( asList( filteredRemovedRelationships,
                new IteratorWrapper<Relationship, Long>( createdRelationships.iterator() )
                {
                    @Override
                    protected Relationship underlyingObjectToObject( Long id )
                    {
                        return getRelationshipById( id );
                    }
                } ) );
    }

    RelationshipType getRelationshipTypeById( int id ) throws TokenNotFoundException
    {
        return relTypeHolder.getTokenById( id );
    }

    public RelationshipImpl getRelationshipForProxy( long relId, LockType lock )
    {
        if ( lock != null )
        {
            lock.acquire( getTransactionState(),
                    new RelationshipProxy( relId, relationshipLookups, statementCtxProvider ) );
        }
        RelationshipImpl rel = relCache.get( relId );
        if ( rel == null )
        {
            throw new NotFoundException( format( "Relationship %d not found", relId ) );
        }
        return rel;
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
                new ArrayMap<>();

        List<RelationshipImpl> relsList = new ArrayList<>( 150 );

        Iterable<RelationshipRecord> loops = rels.first().get( DirectionWrapper.BOTH );
        boolean hasLoops = loops != null;
        if ( hasLoops )
        {
            populateLoadedRelationships( loops, relsList, DirectionWrapper.BOTH, true, newRelationshipMap );
        }
        populateLoadedRelationships( rels.first().get( DirectionWrapper.OUTGOING ), relsList,
                DirectionWrapper.OUTGOING, hasLoops,
                newRelationshipMap
        );
        populateLoadedRelationships( rels.first().get( DirectionWrapper.INCOMING ), relsList,
                DirectionWrapper.INCOMING, hasLoops,
                newRelationshipMap
        );

        return Triplet.of( newRelationshipMap, relsList, rels.other() );
    }

    /**
     * @param loadedRelationshipsOutputParameter
     *         This is the return value for this method. It's written like this
     *         because several calls to this method are used to gradually build up
     *         the map of RelIdArrays that are ultimately involved in the operation.
     */
    private void populateLoadedRelationships( Iterable<RelationshipRecord> loadedRelationshipRecords,
                                              List<RelationshipImpl> relsList,
                                              DirectionWrapper dir,
                                              boolean hasLoops,
                                              ArrayMap<Integer, RelIdArray> loadedRelationshipsOutputParameter )
    {
        for ( RelationshipRecord rel : loadedRelationshipRecords )
        {
            long relId = rel.getId();
            RelationshipImpl relImpl = getOrCreateRelationshipFromCache( relsList, rel, relId );

            getOrCreateRelationships( hasLoops, relImpl.getTypeId(), loadedRelationshipsOutputParameter )
                    .add( relId, dir );
        }
    }

    private RelIdArray getOrCreateRelationships( boolean hasLoops, int typeId, ArrayMap<Integer, RelIdArray> loadedRelationships )
    {
        RelIdArray relIdArray = loadedRelationships.get( typeId );
        if ( relIdArray == null )
        {
            relIdArray = hasLoops ? new RelIdArrayWithLoops( typeId ) : new RelIdArray( typeId );
            loadedRelationships.put( typeId, relIdArray );
        }
        return relIdArray;
    }

    void putAllInRelCache( Collection<RelationshipImpl> relationships )
    {
        relCache.putAll( relationships );
    }

    private RelationshipImpl getOrCreateRelationshipFromCache( List<RelationshipImpl> newlyCreatedRelationships,
                                                               RelationshipRecord rel, long relId )
    {
        RelationshipImpl relImpl = relCache.get( relId );
        if ( relImpl == null )
        {
            newlyCreatedRelationships.add( relImpl );
            relImpl = newRelationshipImpl( relId, rel.getFirstNode(), rel.getSecondNode(), rel.getType(), false );
        }
        return relImpl;
    }

    Iterator<DefinedProperty> loadGraphProperties( boolean light )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        persistenceManager.graphLoadProperties( light, receiver );
        return receiver;
    }

    Iterator<DefinedProperty> loadProperties( NodeImpl node, boolean light )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        persistenceManager.loadNodeProperties( node.getId(), light, receiver );
        return receiver;
    }

    Iterator<DefinedProperty> loadProperties( RelationshipImpl relationship, boolean light )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        persistenceManager.loadRelProperties( relationship.getId(), light, receiver );
        return receiver;
    }

    public void clearCache()
    {
        nodeCache.clear();
        relCache.clear();
        graphProperties = instantiateGraphProperties();
    }

    public Iterable<? extends Cache<?>> caches()
    {
        return asList( nodeCache, relCache );
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
            // (most likely NotInTransactionException)
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
        relTypeHolder.removeToken( id );
    }

    void addPropertyKeyTokens( Token[] propertyKeyTokens )
    {
        propertyKeyTokenHolder.addTokens( propertyKeyTokens );
    }

    void addLabelTokens( Token[] labelTokens )
    {
        labelTokenHolder.addTokens( labelTokens );
    }

    Token getPropertyKeyTokenOrNull( String key )
    {
        return propertyKeyTokenHolder.getTokenByNameOrNull( key );
    }

    int getRelationshipTypeIdFor( RelationshipType type )
    {
        return relTypeHolder.getIdByName( type.name() );
    }

    void addRawRelationshipTypes( Token[] relTypes )
    {
        relTypeHolder.addTokens( relTypes );
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return cast( relTypeHolder.getAllTokens() );
    }

    public ArrayMap<Integer, DefinedProperty> deleteNode( NodeImpl node, TransactionState tx )
    {
        tx.deleteNode( node.getId() );
        return persistenceManager.nodeDelete( node.getId() );
        // remove from node cache done via event
    }

    public ArrayMap<Integer, DefinedProperty> deleteRelationship( RelationshipImpl rel, TransactionState tx )
    {
        NodeImpl startNode;
        NodeImpl endNode;
        boolean success = false;
        try
        {
            long startNodeId = rel.getStartNodeId();
            startNode = getLightNode( startNodeId );
            if ( startNode != null )
            {
                tx.acquireWriteLock( newNodeProxyById( startNodeId ) );
            }
            long endNodeId = rel.getEndNodeId();
            endNode = getLightNode( endNodeId );
            if ( endNode != null )
            {
                tx.acquireWriteLock( newNodeProxyById( endNodeId ) );
            }
            tx.acquireWriteLock( newRelationshipProxyById( rel.getId() ) );
            // no need to load full relationship, all properties will be
            // deleted when relationship is deleted

            ArrayMap<Integer,DefinedProperty> skipMap = tx.getOrCreateCowPropertyRemoveMap( rel );

            tx.deleteRelationship( rel.getId() );
            ArrayMap<Integer,DefinedProperty> removedProps = persistenceManager.relDelete( rel.getId() );

            if ( removedProps.size() > 0 )
            {
                for ( int index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
            }
            int typeId = rel.getTypeId();
            long id = rel.getId();
            if ( startNode != null )
            {
                tx.getOrCreateCowRelationshipRemoveMap( startNode, typeId ).add( id );
            }
            if ( endNode != null )
            {
                tx.getOrCreateCowRelationshipRemoveMap( endNode, typeId ).add( id );
            }
            success = true;
            return removedProps;
        }
        finally
        {
            if ( !success )
            {
                setRollbackOnly();
            }
        }
    }

    public NodeImpl getNodeIfCached( long nodeId )
    {
        return nodeCache.getIfCached( nodeId );
    }

    public RelationshipImpl getRelIfCached( long nodeId )
    {
        return relCache.getIfCached( nodeId );
    }

    public void addRelationshipTypeToken( Token type )
    {
        relTypeHolder.addTokens( type );
    }

    public void addLabelToken( Token type )
    {
        labelTokenHolder.addTokens( type );
    }

    public void addPropertyKeyToken( Token index )
    {
        propertyKeyTokenHolder.addTokens( index );
    }

    public String getKeyForProperty( DefinedProperty property )
    {
        // int keyId = persistenceManager.getKeyIdForProperty( property );
        try
        {
            return propertyKeyTokenHolder.getTokenById( property.propertyKeyId() ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "The key should exist at this point" );
        }
    }

    public List<PropertyTracker<Node>> getNodePropertyTrackers()
    {
        return nodePropertyTrackers;
    }

    public List<PropertyTracker<Relationship>> getRelationshipPropertyTrackers()
    {
        return relationshipPropertyTrackers;
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

    private GraphPropertiesImpl instantiateGraphProperties()
    {
        return new GraphPropertiesImpl( this, statementCtxProvider );
    }

    public GraphPropertiesImpl getGraphProperties()
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
}
