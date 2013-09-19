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

import org.neo4j.helpers.Thunk;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelStatement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.cache.AutoLoadingCache;
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
    private final AutoLoadingCache<NodeImpl> nodeCache;
    private final AutoLoadingCache<RelationshipImpl> relationshipCache;
    private final Thunk<GraphPropertiesImpl> graphProperties;

    public PersistenceCache(
            AutoLoadingCache<NodeImpl> nodeCache,
            AutoLoadingCache<RelationshipImpl> relationshipCache,
            Thunk<GraphPropertiesImpl> graphProperties )
    {
        this.nodeCache = nodeCache;
        this.relationshipCache = relationshipCache;
        this.graphProperties = graphProperties;
    }

    public boolean nodeHasLabel( KernelStatement state, long nodeId, int labelId, CacheLoader<int[]> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).hasLabel( state, labelId, cacheLoader );
    }

    public int[] nodeGetLabels( KernelStatement state, long nodeId, CacheLoader<int[]> loader )
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

    public Iterator<DefinedProperty> nodeGetProperties( KernelStatement state, long nodeId,
            CacheLoader<Iterator<DefinedProperty>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getProperties( state, cacheLoader, NODE_CACHE_SIZE_LISTENER );
    }

    public PrimitiveLongIterator nodeGetPropertyKeys( KernelStatement state, long nodeId,
            CacheLoader<Iterator<DefinedProperty>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getPropertyKeys( state, cacheLoader, NODE_CACHE_SIZE_LISTENER );
    }

    public Property nodeGetProperty( KernelStatement state, long nodeId, int propertyKeyId,
            CacheLoader<Iterator<DefinedProperty>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getProperty( state, cacheLoader, NODE_CACHE_SIZE_LISTENER, propertyKeyId );
    }

    public Iterator<DefinedProperty> relationshipGetProperties( KernelStatement state, long relationshipId,
            CacheLoader<Iterator<DefinedProperty>> cacheLoader ) throws EntityNotFoundException
    {
        return getRelationship( relationshipId ).getProperties( state, cacheLoader,
                RELATIONSHIP_CACHE_SIZE_LISTENER );
    }

    public Property relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId,
            CacheLoader<Iterator<DefinedProperty>> cacheLoader ) throws EntityNotFoundException
    {
        return getRelationship( relationshipId ).getProperty( state, cacheLoader, RELATIONSHIP_CACHE_SIZE_LISTENER,
                propertyKeyId );
    }

    public Iterator<DefinedProperty> graphGetProperties( KernelStatement state, CacheLoader<Iterator<DefinedProperty>> cacheLoader )
    {
        return graphProperties.evaluate().getProperties( state, cacheLoader, NO_UPDATES );
    }

    public PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state,
            CacheLoader<Iterator<DefinedProperty>> cacheLoader )
    {
        return graphProperties.evaluate().getPropertyKeys( state, cacheLoader, NO_UPDATES );
    }

    public Property graphGetProperty( KernelStatement state, CacheLoader<Iterator<DefinedProperty>> cacheLoader,
            int propertyKeyId )
    {
        return graphProperties.evaluate().getProperty( state, cacheLoader, NO_UPDATES, propertyKeyId );
    }
}
