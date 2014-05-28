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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.helpers.collection.Iterables.cast;
import static org.neo4j.kernel.impl.locking.ResourceTypes.legacyIndexResourceId;

public class NodeManager extends LifecycleAdapter implements EntityFactory
{
    private final GraphDatabaseService graphDbService;
    private final AutoLoadingCache<NodeImpl> nodeCache;
    private final AutoLoadingCache<RelationshipImpl> relCache;
    private final CacheProvider cacheProvider;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relTypeHolder;
    private final ThreadToStatementContextBridge threadToTransactionBridge;
    private final NodeProxy.NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relationshipLookups;
    private final RelationshipLoader relationshipLoader;
    private final List<PropertyTracker<Node>> nodePropertyTrackers;
    private final List<PropertyTracker<Relationship>> relationshipPropertyTrackers;
    private final IdGeneratorFactory idGeneratorFactory;

    private GraphPropertiesImpl graphProperties;

    private final AutoLoadingCache.Loader<NodeImpl> nodeLoader = new AutoLoadingCache.Loader<NodeImpl>()
    {
        @Override
        public NodeImpl loadById( long id )
        {
            NodeRecord record = threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true ).nodeLoadLight( id );
            if ( record == null )
            {
                return null;
            }
            return record.isCommittedDense() ? new DenseNodeImpl( id ) : new NodeImpl( id );
        }
    };
    private final AutoLoadingCache.Loader<RelationshipImpl> relLoader = new AutoLoadingCache.Loader<RelationshipImpl>()
    {
        @Override
        public RelationshipImpl loadById( long id )
        {
            RelationshipRecord data = threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true ).relLoadLight( id );
            if ( data == null )
            {
                return null;
            }
            int typeId = data.getType();
            final long startNodeId = data.getFirstNode();
            final long endNodeId = data.getSecondNode();
            return new RelationshipImpl( id, startNodeId, endNodeId, typeId, false );
        }
    };

    public NodeManager( StringLogger logger, GraphDatabaseService graphDb,
                        RelationshipTypeTokenHolder relationshipTypeTokenHolder, CacheProvider cacheProvider,
                        PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                        NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups,
                        Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache,
                        ThreadToStatementContextBridge threadToTransactionBridge,
                        IdGeneratorFactory idGeneratorFactory )
    {
        this.graphDbService = graphDb;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
        this.relTypeHolder = relationshipTypeTokenHolder;
        this.cacheProvider = cacheProvider;
        this.threadToTransactionBridge = threadToTransactionBridge;
        this.idGeneratorFactory = idGeneratorFactory;
        this.nodeCache = new AutoLoadingCache<>( nodeCache, nodeLoader );
        this.relCache = new AutoLoadingCache<>( relCache, relLoader );
        this.nodePropertyTrackers = new LinkedList<>();
        this.relationshipPropertyTrackers = new LinkedList<>();
        this.relationshipLoader = new RelationshipLoader( threadToTransactionBridge, relCache );
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

    @Override
    public NodeProxy newNodeProxyById( long id )
    {
        return new NodeProxy( id, nodeLookup, relationshipLookups, threadToTransactionBridge );
    }

    public Node getNodeByIdOrNull( long nodeId )
    {
        threadToTransactionBridge.assertInTransaction();
        NodeImpl node = getLightNode( nodeId );
        return node != null ?
                new NodeProxy( nodeId, nodeLookup, relationshipLookups, threadToTransactionBridge ) : null;
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
        return new RelationshipProxy( id, relationshipLookups, threadToTransactionBridge );
    }

    public NodeImpl getNodeForProxy( long nodeId )
    {
        NodeImpl node = getLightNode( nodeId );
        if ( node == null )
        {
            throw new NotFoundException( format( "Node %d not found", nodeId ) );
        }
        return node;
    }

    protected Relationship getRelationshipByIdOrNull( long relId )
    {
        threadToTransactionBridge.assertInTransaction();
        RelationshipImpl relationship = relCache.get( relId );
        return relationship != null ? new RelationshipProxy( relId, relationshipLookups, threadToTransactionBridge ) : null;
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

    RelationshipType getRelationshipTypeById( int id ) throws TokenNotFoundException
    {
        return relTypeHolder.getTokenById( id );
    }

    public RelationshipImpl getRelationshipForProxy( long relId )
    {
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
        if ( node != null )
        {
            RelationshipLoadingPosition position = node.getRelChainPosition();
            if ( position != null )
            {
                position.compareAndAdvance( relIdDeleted, nextRelId );
            }
        }
    }

    RelationshipLoadingPosition getRelationshipChainPosition( NodeImpl node )
    {
        return threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true )
                .getRelationshipChainPosition( node.getId() );
    }

    void putAllInRelCache( Collection<RelationshipImpl> relationships )
    {
        relCache.putAll( relationships );
    }

    Iterator<DefinedProperty> loadGraphProperties( boolean light )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true )
                .graphLoadProperties( light, receiver );
        return receiver;
    }

    Iterator<DefinedProperty> loadProperties( NodeImpl node, boolean light )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true )
                .nodeLoadProperties( node.getId(), light, receiver );
        return receiver;
    }

    Iterator<DefinedProperty> loadProperties( RelationshipImpl relationship, boolean light )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true )
                .relLoadProperties( relationship.getId(), light, receiver );
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
        threadToTransactionBridge.getTopLevelTransactionBoundToThisThread( true ).failure();
    }

    public <T extends PropertyContainer> T indexPutIfAbsent( Index<T> index, T entity, String key, Object value )
    {
        T existing = index.get( key, value ).getSingle();
        if ( existing != null )
        {
            return existing;
        }

        // Grab lock
        try(Statement statement = threadToTransactionBridge.instance())
        {
            statement.readOperations().acquireExclusive(
                    ResourceTypes.LEGACY_INDEX, legacyIndexResourceId( index.getName(), key ) );

            // Check again -- now holding the lock
            existing = index.get( key, value ).getSingle();
            if ( existing != null )
            {
                // Someone else created this entry, release the lock as we won't be needing it
                statement.readOperations().releaseExclusive(
                        ResourceTypes.LEGACY_INDEX, legacyIndexResourceId( index.getName(), key ) );
                return existing;
            }

            // Add
            index.add( entity, key, value );
            return null;
        }
    }

    public void removeRelationshipTypeFromCache( int id )
    {
        relTypeHolder.removeToken( id );
    }

    public void removeLabelFromCache( int id )
    {
        labelTokenHolder.removeToken( id );
    }

    public void removePropertyKeyFromCache( int id )
    {
        propertyKeyTokenHolder.removeToken( id );
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
        return threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true ).nodeDelete( node.getId() );
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
                tx.locks().acquireExclusive( ResourceTypes.NODE, startNodeId );
            }
            long endNodeId = rel.getEndNodeId();
            endNode = getLightNode( endNodeId );
            if ( endNode != null )
            {
                tx.locks().acquireExclusive( ResourceTypes.NODE, endNodeId );
            }
            tx.locks().acquireExclusive( ResourceTypes.RELATIONSHIP, rel.getId() );
            // no need to load full relationship, all properties will be
            // deleted when relationship is deleted

            ArrayMap<Integer,DefinedProperty> skipMap = tx.getOrCreateCowPropertyRemoveMap( rel );

            tx.deleteRelationship( rel.getId() );
            ArrayMap<Integer,DefinedProperty> removedProps =
                    threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true ).relDelete( rel.getId() );

            if ( removedProps.size() > 0 )
            {
                for ( int index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
            }
            int typeId = rel.getTypeId();
            long id = rel.getId();
            boolean loop = startNodeId == endNodeId;
            if ( startNode != null )
            {
                tx.getOrCreateCowRelationshipRemoveMap( startNode, typeId ).add( id,
                        loop ? Direction.BOTH : Direction.OUTGOING );
            }
            if ( endNode != null && !loop )
            {
                tx.getOrCreateCowRelationshipRemoveMap( endNode, typeId ).add( id, Direction.INCOMING );
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

    Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, RelationshipLoadingPosition>
            getMoreRelationships( NodeImpl node, DirectionWrapper direction, int[] types )
    {
        return relationshipLoader.getMoreRelationships( node, direction, types );
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
        return new GraphPropertiesImpl( this, threadToTransactionBridge );
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
        // TODO 2.2-future
        throw new UnsupportedOperationException( "Please implement" );
    }

    public int getRelationshipCount( NodeImpl nodeImpl, int type, DirectionWrapper direction )
    {
        return threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true )
                .getRelationshipCount( nodeImpl.getId(), type, direction );
    }

    public Iterator<Integer> getRelationshipTypes( DenseNodeImpl node )
    {
        return asList( threadToTransactionBridge.getTransactionRecordStateBoundToThisThread( true )
                .getRelationshipTypes( node.getId() ) ).iterator();
    }
}
