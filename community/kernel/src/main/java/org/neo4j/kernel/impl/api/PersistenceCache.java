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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.Thunk;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.cache.LockStripedCache;
import org.neo4j.kernel.impl.core.GraphPropertiesImpl;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.Primitive;
import org.neo4j.kernel.impl.core.RelationshipImpl;

import static org.neo4j.kernel.impl.api.CacheUpdateListener.NO_UPDATES;

/**
 * This is a cache for the {@link KernelAPI}. Currently it piggy-backs on NodeImpl/RelationshipImpl
 * to gather all caching in one place.
 * 
 * NOTE:
 * NodeImpl/RelationshipImpl manages caching, locking and transaction state merging. In the future
 * they might disappear and split up into {@link CachingStatementOperations},
 * {@link LockingStatementOperations} and {@link StateHandlingStatementOperations}.
 * <p/>
 * The point is that we need a cache and the implementation is a bit temporary, but might end
 * up being the cache to replace the data within NodeImpl/RelationshipImpl.
 */
public class PersistenceCache
{
    private final CacheUpdateListener NODE_CACHE_SIZE_LISTENER = new CacheUpdateListener()
    {
        @Override
        public void newSize( Primitive entity, int size )
        {
            nodeCache.updateSize( (NodeImpl) entity, size );
        }
    };
    private final CacheUpdateListener RELATIONSHIP_CACHE_SIZE_LISTENER = new CacheUpdateListener()
    {
        @Override
        public void newSize( Primitive entity, int size )
        {
            relationshipCache.updateSize( (RelationshipImpl) entity, size );
        }
    };
    private final LockStripedCache<NodeImpl> nodeCache;
    private final LockStripedCache<RelationshipImpl> relationshipCache;
    private final Thunk<GraphPropertiesImpl> graphProperties;

    public PersistenceCache(
            LockStripedCache<NodeImpl> nodeCache,
            LockStripedCache<RelationshipImpl> relationshipCache,
            Thunk<GraphPropertiesImpl> graphProperties )
    {
        this.nodeCache = nodeCache;
        this.relationshipCache = relationshipCache;
        this.graphProperties = graphProperties;
    }

    public boolean nodeHasLabel( StatementState state, long nodeId, long labelId, CacheLoader<Set<Long>> cacheLoader )
            throws EntityNotFoundException
    {
        Set<Long> labels = getNode( nodeId ).getLabels( state, cacheLoader );
        return labels.contains( labelId );
    }
    
    public Set<Long> nodeGetLabels( StatementState state, long nodeId, CacheLoader<Set<Long>> loader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getLabels( state, loader );
    }
    
    private NodeImpl getNode( long nodeId ) throws EntityNotFoundException
    {
        NodeImpl node = nodeCache.get( nodeId );
        if ( node == null )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }
        return node;
    }

    private RelationshipImpl getRelationship( long relationshipId ) throws EntityNotFoundException
    {
        RelationshipImpl relationship = relationshipCache.get( relationshipId );
        if ( relationship == null )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
        return relationship;
    }
    
    public void apply( TxState state )
    {
        for ( NodeState stateEntity : state.nodeStates() )
        {
            NodeImpl node = nodeCache.getIfCached( stateEntity.getId() );
            if ( node == null )
            {
                continue;
            }

            node.commitLabels(
                    stateEntity.labelDiffSets().getAdded(),
                    stateEntity.labelDiffSets().getRemoved() );
        }
    }

    public void evictNode( long nodeId )
    {
        nodeCache.remove( nodeId );
    }

    public Iterator<Property> nodeGetProperties( StatementState state, long nodeId,
            CacheLoader<Iterator<Property>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getProperties( state, cacheLoader, NODE_CACHE_SIZE_LISTENER );
    }
    
    public PrimitiveLongIterator nodeGetPropertyKeys( StatementState state, long nodeId,
            CacheLoader<Iterator<Property>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getPropertyKeys( state, cacheLoader, NODE_CACHE_SIZE_LISTENER );
    }
    
    public Property nodeGetProperty( StatementState state, long nodeId, long propertyKeyId,
            CacheLoader<Iterator<Property>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getProperty( state, cacheLoader, NODE_CACHE_SIZE_LISTENER, (int) propertyKeyId );
    }
    
    public Iterator<Property> relationshipGetProperties( StatementState state, long relationshipId,
            CacheLoader<Iterator<Property>> cacheLoader ) throws EntityNotFoundException
    {
        return getRelationship( relationshipId ).getProperties( state, cacheLoader,
                RELATIONSHIP_CACHE_SIZE_LISTENER );
    }

    public PrimitiveLongIterator relationshipGetPropertyKeys( StatementState state, long relationshipId,
            CacheLoader<Iterator<Property>> cacheLoader ) throws EntityNotFoundException
    {
        return getRelationship( relationshipId ).getPropertyKeys( state, cacheLoader,
                RELATIONSHIP_CACHE_SIZE_LISTENER );
    }
    
    public Property relationshipGetProperty( StatementState state, long relationshipId, long propertyKeyId,
            CacheLoader<Iterator<Property>> cacheLoader ) throws EntityNotFoundException
    {
        return getRelationship( relationshipId ).getProperty( state, cacheLoader, RELATIONSHIP_CACHE_SIZE_LISTENER,
                (int) propertyKeyId );
    }
    
    public Iterator<Property> graphGetProperties( StatementState state, CacheLoader<Iterator<Property>> cacheLoader )
    {
        return graphProperties.evaluate().getProperties( state, cacheLoader, NO_UPDATES );
    }
    
    public PrimitiveLongIterator graphGetPropertyKeys( StatementState state,
            CacheLoader<Iterator<Property>> cacheLoader )
    {
        return graphProperties.evaluate().getPropertyKeys( state, cacheLoader, NO_UPDATES );
    }

    public Property graphGetProperty( StatementState state, CacheLoader<Iterator<Property>> cacheLoader,
            long propertyKeyId )
    {
        return graphProperties.evaluate().getProperty( state, cacheLoader, NO_UPDATES, (int) propertyKeyId );
    }
}
