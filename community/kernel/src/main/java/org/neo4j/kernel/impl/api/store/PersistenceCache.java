/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.api.txstate.TxStateVisitor.Adapter;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.RecordStateForCacheAccessor;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode;
import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.core.EntityFactory;
import org.neo4j.kernel.impl.core.GraphPropertiesImpl;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.Primitive;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipLoader;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.transaction.command.RelationshipHoles;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;

import static org.neo4j.kernel.impl.api.store.CacheUpdateListener.NO_UPDATES;

/**
 * This is a cache for the {@link KernelAPI}. Currently it piggy-backs on NodeImpl/RelationshipImpl
 * to gather all caching in one place.
 *
 * NOTE:
 * NodeImpl/RelationshipImpl manages caching, locking and transaction state merging. In the future
 * they might disappear and split up into {@link CacheLayer},
 * {@link org.neo4j.kernel.impl.api.LockingStatementOperations} and {@link org.neo4j.kernel.impl.api.StateHandlingStatementOperations}.
 * <p>
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
    private GraphPropertiesImpl graphProperties;
    private final RelationshipLoader relationshipLoader;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final EntityFactory entityFactory;

    public PersistenceCache(
            AutoLoadingCache<NodeImpl> nodeCache,
            AutoLoadingCache<RelationshipImpl> relationshipCache,
            EntityFactory entityFactory, RelationshipLoader relationshipLoader,
            PropertyKeyTokenHolder propertyKeyTokenHolder,
            RelationshipTypeTokenHolder relationshipTypeTokenHolder,
            LabelTokenHolder labelTokenHolder )
    {
        this.nodeCache = nodeCache;
        this.relationshipCache = relationshipCache;
        this.entityFactory = entityFactory;
        this.graphProperties = entityFactory.newGraphProperties();
        this.relationshipLoader = relationshipLoader;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
    }

    public boolean nodeHasLabel( long nodeId, int labelId, CacheLoader<int[]> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).hasLabel( labelId, cacheLoader );
    }

    public int[] nodeGetLabels( long nodeId, CacheLoader<int[]> loader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getLabels( loader );
    }

    public NodeImpl getNode( long nodeId ) throws EntityNotFoundException
    {
        NodeImpl node = nodeCache.get( nodeId );
        if ( node == null )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }
        return node;
    }

    public RelationshipImpl getRelationship( long relationshipId ) throws EntityNotFoundException
    {
        RelationshipImpl relationship = relationshipCache.get( relationshipId );
        if ( relationship == null )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
        return relationship;
    }

    /**
     * Applies changes made by a transaction that was just committed.
     *
     * TODO you know what? the argument here shouldn't be a TxState, because that means that only transactions
     * made on this machine by client code will be applied to cache. We could make PersistenceCache a
     * NeoCommandVisitor where each visited command would update the cache accordingly. But for the time being
     * we just do this and invalidate data made by "other" machines.
     */
    public void apply( ReadableTxState txState, final RecordStateForCacheAccessor recordState )
    {
        // Apply everything except labels, which is done in the other apply method. TODO sort this out later.
        txState.accept( new TxStateVisitor.Adapter()
        {
            // For now just have these methods convert their data into whatever NodeImpl (and friends)
            // expects. We can optimize later.

            @Override
            public void visitCreatedNode( long id )
            {   // Let readers cache this node later
            }

            @Override
            public void visitDeletedNode( long id )
            {
                evictNode( id );
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                evictRelationship( id );
                // TODO We would like to do the patch rel chain position here as well, but we just don't have all the
                // required information here
            }

            @Override
            public void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added,
                                                  Iterator<DefinedProperty> changed, Iterator<Integer> removed )
            {
                NodeImpl node = nodeCache.getIfCached( id );
                if ( node != null )
                {
                    node.commitPropertyMaps( translateAddedAndChangedProperties( added, changed ), removed );
                }
            }

            @Override
            public void visitNodeRelationshipChanges( long id,
                                                      RelationshipChangesForNode added,
                                                      RelationshipChangesForNode removed )
            {
                NodeImpl node = nodeCache.getIfCached( id );
                if ( node != null )
                {
                    if ( node.commitRelationshipMaps(
                            translateAddedRelationships( added ),
                            translateRemovedRelationships( removed ),
                            recordState.firstRelationshipIdsOf( id ),
                            recordState.isDense( id ) ) )
                    {
                        evictNode( id );
                    }
                }
            }

            @Override
            public void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added,
                                                 Iterator<DefinedProperty> changed, Iterator<Integer> removed )
            {
                RelationshipImpl relationship = relationshipCache.getIfCached( id );
                if ( relationship != null )
                {
                    relationship.commitPropertyMaps( translateAddedAndChangedProperties( added, changed ), removed );
                }
            }

            @Override
            public void visitGraphPropertyChanges( Iterator<DefinedProperty> added,
                                                   Iterator<DefinedProperty> changed, Iterator<Integer> removed )
            {
                graphProperties.commitPropertyMaps( translateAddedAndChangedProperties( added, changed ), removed );
            }

            // TODO Below are translators from TxState into what Primitive and friends expects.
            // Ideally there should not be any translation since it's a bit unnecessary and costly.
            private PrimitiveIntObjectMap<DefinedProperty> translateAddedAndChangedProperties(
                    Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed )
            {
                if ( added == null && changed == null )
                {
                    return null;
                }

                PrimitiveIntObjectMap<DefinedProperty> result = org.neo4j.collection.primitive.Primitive.intObjectMap();
                translateProperties( added, result );
                translateProperties( changed, result );
                return result;
            }

            private void translateProperties( Iterator<DefinedProperty> properties,
                                              PrimitiveIntObjectMap<DefinedProperty> result )
            {
                if ( properties != null )
                {
                    while ( properties.hasNext() )
                    {
                        DefinedProperty property = properties.next();
                        result.put( property.propertyKeyId(), property );
                    }
                }
            }

            private PrimitiveIntObjectMap<RelIdArray> translateAddedRelationships(
                    RelationshipChangesForNode added )
            {
                if ( added == null )
                {
                    return null;
                }

                PrimitiveIntObjectMap<RelIdArray> result = org.neo4j.collection.primitive.Primitive.intObjectMap();
                PrimitiveIntIterator types = added.getTypesChanged();
                while ( types.hasNext() )
                {
                    int type = types.next();
                    Iterator<Long> loopsChanges = added.loopsChanges( type );
                    RelIdArray ids = loopsChanges == null ? new RelIdArray( type ) :
                                     new RelIdArrayWithLoops( type );
                    addIds( ids, added.outgoingChanges( type ), DirectionWrapper.OUTGOING );
                    addIds( ids, added.incomingChanges( type ), DirectionWrapper.INCOMING );
                    addIds( ids, loopsChanges, DirectionWrapper.BOTH );
                    result.put( type, ids );
                }
                return result;
            }

            private void addIds( RelIdArray idArray, Iterator<Long> ids, DirectionWrapper direction )
            {
                if ( ids != null )
                {
                    while ( ids.hasNext() )
                    {
                        idArray.add( ids.next(), direction );
                    }
                }
            }

            private PrimitiveIntObjectMap<PrimitiveLongSet> translateRemovedRelationships(
                    RelationshipChangesForNode removed )
            {
                if ( removed == null )
                {
                    return null;
                }

                PrimitiveIntObjectMap<PrimitiveLongSet> result =
                        org.neo4j.collection.primitive.Primitive.intObjectMap();
                PrimitiveIntIterator types = removed.getTypesChanged();
                while ( types.hasNext() )
                {
                    int type = types.next();
                    PrimitiveLongSet ids = org.neo4j.collection.primitive.Primitive.longSet();
                    addIds( ids, removed.outgoingChanges( type ) );
                    addIds( ids, removed.incomingChanges( type ) );
                    addIds( ids, removed.loopsChanges( type ) );
                    result.put( type, ids );
                }
                return result;
            }

            private void addIds( PrimitiveLongSet set, Iterator<Long> ids )
            {
                if ( ids != null )
                {
                    while ( ids.hasNext() )
                    {
                        set.add( ids.next() );
                    }
                }
            }
        } );
    }

    public void apply( Collection<NodeLabelUpdate> updates )
    {
        for ( NodeLabelUpdate update : updates )
        {
            NodeImpl node = nodeCache.getIfCached( update.getNodeId() );
            if ( node != null )
            {
                // TODO: This is because the labels are still longs in WriteTransaction, this should go away once
                // we make labels be ints everywhere.
                long[] labelsAfter = update.getLabelsAfter();
                int[] labels = new int[labelsAfter.length];
                for ( int i = 0; i < labels.length; i++ )
                {
                    labels[i] = (int) labelsAfter[i];
                }
                node.commitLabels( labels );
            }
        }
    }

    public void evictNode( long nodeId )
    {
        nodeCache.remove( nodeId );
    }

    public void evictRelationship( long relId )
    {
        relationshipCache.remove( relId );
    }

    public void evictGraphProperties()
    {
        graphProperties = entityFactory.newGraphProperties();
    }

    public void evictPropertyKey( int id )
    {
        propertyKeyTokenHolder.removeToken( id );
    }

    public void evictRelationshipType( int id )
    {
        relationshipTypeTokenHolder.removeToken( id );
    }

    public void evictLabel( int id )
    {
        labelTokenHolder.removeToken( id );
    }

    public Iterator<DefinedProperty> nodeGetProperties( long nodeId,
                                                        CacheLoader<Iterator<DefinedProperty>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getProperties( cacheLoader, NODE_CACHE_SIZE_LISTENER );
    }

    public PrimitiveLongIterator nodeGetPropertyKeys( long nodeId,
                                                      CacheLoader<Iterator<DefinedProperty>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getPropertyKeys( cacheLoader, NODE_CACHE_SIZE_LISTENER );
    }

    public Property nodeGetProperty( long nodeId, int propertyKeyId,
                                     CacheLoader<Iterator<DefinedProperty>> cacheLoader )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getProperty( cacheLoader, NODE_CACHE_SIZE_LISTENER, propertyKeyId );
    }

    public Iterator<DefinedProperty> relationshipGetProperties( long relationshipId,
                                                                CacheLoader<Iterator<DefinedProperty>> cacheLoader ) throws EntityNotFoundException
    {
        return getRelationship( relationshipId ).getProperties( cacheLoader,
                                                                RELATIONSHIP_CACHE_SIZE_LISTENER );
    }

    public Property relationshipGetProperty( long relationshipId, int propertyKeyId,
                                             CacheLoader<Iterator<DefinedProperty>> cacheLoader ) throws EntityNotFoundException
    {
        return getRelationship( relationshipId ).getProperty( cacheLoader, RELATIONSHIP_CACHE_SIZE_LISTENER,
                                                              propertyKeyId );
    }

    public Iterator<DefinedProperty> graphGetProperties( CacheLoader<Iterator<DefinedProperty>> cacheLoader )
    {
        return graphProperties.getProperties( cacheLoader, NO_UPDATES );
    }

    public PrimitiveLongIterator graphGetPropertyKeys( CacheLoader<Iterator<DefinedProperty>> cacheLoader )
    {
        return graphProperties.getPropertyKeys( cacheLoader, NO_UPDATES );
    }

    public Property graphGetProperty( CacheLoader<Iterator<DefinedProperty>> cacheLoader,
                                      int propertyKeyId )
    {
        return graphProperties.getProperty( cacheLoader, NO_UPDATES, propertyKeyId );
    }

    public PrimitiveLongIterator nodeGetRelationships( long node,
            Direction direction, int[] relTypes ) throws EntityNotFoundException
    {
        return getNode( node ).getRelationships( relationshipLoader, direction, relTypes,
                                                 NODE_CACHE_SIZE_LISTENER );
    }

    public PrimitiveLongIterator nodeGetRelationships( long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getRelationships( relationshipLoader, direction,
                                                   NODE_CACHE_SIZE_LISTENER );
    }

    public int nodeGetDegree( long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getDegree( relationshipLoader, direction, NODE_CACHE_SIZE_LISTENER );
    }

    public int nodeGetDegree( long nodeId, int type, Direction direction )
            throws EntityNotFoundException
    {
        return getNode( nodeId ).getDegree( relationshipLoader, type, direction, NODE_CACHE_SIZE_LISTENER );
    }

    public boolean nodeVisitDegrees( long nodeId, DegreeVisitor visitor )
    {
        NodeImpl node = nodeCache.get( nodeId );
        if ( node != null )
        {
            node.visitDegrees( relationshipLoader, visitor, NODE_CACHE_SIZE_LISTENER );
            return true;
        }
        return false;
    }

    public PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId )
            throws EntityNotFoundException
    {
        return PrimitiveIntCollections.toPrimitiveIterator( getNode( nodeId )
                .getRelationshipTypes( relationshipLoader, NODE_CACHE_SIZE_LISTENER ) );
    }

    public void cachePropertyKey( Token token )
    {
        propertyKeyTokenHolder.addToken( token );
    }

    public void cacheRelationshipType( Token token )
    {
        relationshipTypeTokenHolder.addToken( token );
    }

    public void cacheLabel( Token token )
    {
        labelTokenHolder.addToken( token );
    }

    public void patchDeletedRelationshipNodes( long nodeId, RelationshipHoles holes )
    {
        NodeImpl node = nodeCache.getIfCached( nodeId );
        if ( node != null )
        {
            node.updateRelationshipChainPosition( holes );
        }
    }

    /**
     * Used when rolling back a transaction. Node reservations are put in cache up front, so those have
     * to be removed when rolling back.
     */
    public void invalidate( ReadableTxState txState )
    {
        txState.accept( new Adapter()
        {
            @Override
            public void visitCreatedNode( long id )
            {
                nodeCache.remove( id );
            }
        } );
    }
}
