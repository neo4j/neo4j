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

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.PropertyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.LabelToken;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.transaction.LockType;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asIterator;
import static org.neo4j.helpers.collection.IteratorUtil.contains;

/**
 * This layer interacts with committed data. It currently delegates to several of the older XXXManager-type classes.
 * This should be refactored to use a cleaner read-only interface.
 *
 * Also, caching currently lives above this layer, but it really should live *inside* the read-only abstraction that
 * this
 * thing takes.
 *
 * Cache reading and invalidation is not the concern of this part of the system, that is an optimization on top of the
 * committed data in the database, and should as such live under that abstraction.
 */
public class StoreStatementContext extends CompositeStatementContext
{
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final NodeManager nodeManager;
    private final NeoStore neoStore;
    private final IndexingService indexService;
    private final IndexReaderFactory indexReaderFactory;
    private final NodeStore nodeStore;
    private final Function<String, Long> propertyStringToId = new Function<String, Long>()
    {
        @Override
        public Long apply( String s )
        {
            try
            {
                return getPropertyKeyId( s );
            }
            catch ( PropertyKeyNotFoundException e )
            {
                throw new ThisShouldNotHappenError( "Jake", "Property key id stored in store should exist." );
            }
        }
    };
    private static final Function<UniquenessConstraintRule, UniquenessConstraint> UNIQUENESS_CONSTRAINT_TO_RULE =
            new Function<UniquenessConstraintRule, UniquenessConstraint>()
            {
                @Override
                public UniquenessConstraint apply( UniquenessConstraintRule rule )
                {
                    // We can use propertyKeyId straight up here, without reading from the record, since we have
                    // verified that it has that propertyKeyId in the predicate. And since we currently only support
                    // uniqueness on single properties, there is nothing else to pass in to UniquenessConstraint.
                    return new UniquenessConstraint( rule.getLabel(), rule.getPropertyKey() );
                }
            };
    private final SchemaStorage schemaStorage;

    public StoreStatementContext( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                                  NodeManager nodeManager, SchemaStorage schemaStorage, NeoStore neoStore,
                                  IndexingService indexService, IndexReaderFactory indexReaderFactory )
    {
        this.schemaStorage = schemaStorage;
        assert neoStore != null : "No neoStore provided";

        this.indexService = indexService;
        this.indexReaderFactory = indexReaderFactory;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.nodeManager = nodeManager;
        this.neoStore = neoStore;
        this.nodeStore = neoStore.getNodeStore();
    }

    @Override
    protected void beforeWriteOperation()
    {
        throw new UnsupportedOperationException(
                "The storage layer can not be written to directly, you have to go through a transaction." );
    }

    @Override
    public void close()
    {
        indexReaderFactory.close();
    }

    @Override
    public long getOrCreateLabelId( String label ) throws DataIntegrityKernelException
    {
        try
        {
            return labelTokenHolder.getOrCreateId( label );
        }
        catch ( TransactionFailureException e )
        {
            // Temporary workaround for the property store based label
            // implementation. Actual
            // implementation should not depend on internal kernel exception
            // messages like this.
            if ( e.getCause() != null && e.getCause() instanceof UnderlyingStorageException
                 && e.getCause().getMessage().equals( "Id capacity exceeded" ) )
            {
                throw new DataIntegrityKernelException(
                        "The maximum number of labels available has been reached, cannot create more labels.", e );
            }
            else
            {
                throw e;
            }
        }
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundKernelException
    {
        try
        {
            return labelTokenHolder.getIdByName( label );
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( label, e );
        }
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        try
        {
            return contains( getLabelsForNode( nodeId ), labelId );
        }
        catch ( InvalidRecordException e )
        {
            return false;
        }
    }

    @Override
    public Iterator<Long> getLabelsForNode( long nodeId )
    {
        try
        {
            return asIterator( nodeStore.getLabelsForNode( nodeStore.getRecord( nodeId ) ) );
        }
        catch ( InvalidRecordException e )
        {   // TODO Might hide invalid dynamic record problem. It's here because this method
            // might get called with a nodeId that doesn't exist.
            return IteratorUtil.emptyIterator();
        }
    }

    @Override
    public String getLabelName( long labelId ) throws LabelNotFoundKernelException
    {
        try
        {
            return labelTokenHolder.getTokenById( (int) labelId ).getName();
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( "Label by id " + labelId, e );
        }
    }

    @Override
    public Iterator<Long> getNodesWithLabel( final long labelId )
    {
        final NodeStore nodeStore = neoStore.getNodeStore();
        final long highestId = nodeStore.getHighestPossibleIdInUse();
        return new PrefetchingIterator<Long>()
        {
            private long id = 0;

            @Override
            protected Long fetchNextOrNull()
            {
                while ( id <= highestId )
                {
                    NodeRecord node = nodeStore.forceGetRecord( id++ );
                    if ( node.inUse() )
                    {
                        for ( long label : nodeStore.getLabelsForNode( node ) )
                        {
                            if ( label == labelId )
                            {
                                return node.getId();
                            }
                        }
                    }
                }
                return null;
            }
        };
    }

    @Override
    public Iterator<LabelToken> listLabels()
    {
        return labelTokenHolder.getAllTokens().iterator();
    }

    @Override
    public IndexDescriptor getIndex( final long labelId, final long propertyKey ) throws SchemaRuleNotFoundException
    {
        return descriptor( schemaStorage.indexRule( labelId, propertyKey ) );
    }

    private static IndexDescriptor descriptor( IndexRule ruleRecord )
    {
        return new IndexDescriptor( ruleRecord.getLabel(), ruleRecord.getPropertyKey() );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes( final long labelId )
    {
        return getIndexDescriptorsFor( indexRules( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes()
    {
        return getIndexDescriptorsFor( INDEX_RULES );
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes( final long labelId )
    {
        return getIndexDescriptorsFor( constraintIndexRules( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes()
    {
        return getIndexDescriptorsFor( CONSTRAINT_INDEX_RULES );
    }

    private static Predicate<SchemaRule> indexRules( final long labelId )
    {
        return new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule rule )
            {
                return rule.getLabel() == labelId && rule.getKind() == SchemaRule.Kind.INDEX_RULE;
            }
        };
    }

    private static Predicate<SchemaRule> constraintIndexRules( final long labelId )
    {
        return new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule rule )
            {
                return rule.getLabel() == labelId && rule.getKind() == SchemaRule.Kind.CONSTRAINT_INDEX_RULE;
            }
        };
    }

    private static final Predicate<SchemaRule> INDEX_RULES = new Predicate<SchemaRule>()
    {
        @Override
        public boolean accept( SchemaRule rule )
        {
            return rule.getKind() == SchemaRule.Kind.INDEX_RULE;
        }
    }, CONSTRAINT_INDEX_RULES = new Predicate<SchemaRule>()
    {
        @Override
        public boolean accept( SchemaRule rule )
        {
            return rule.getKind() == SchemaRule.Kind.CONSTRAINT_INDEX_RULE;
        }
    };

    private Iterator<IndexDescriptor> getIndexDescriptorsFor( Predicate<SchemaRule> filter )
    {
        Iterator<SchemaRule> filtered = filter( filter, neoStore.getSchemaStore().loadAll() );

        return map( new Function<SchemaRule, IndexDescriptor>()
        {
            @Override
            public IndexDescriptor apply( SchemaRule from )
            {
                return descriptor( (IndexRule) from );
            }
        }, filtered );
    }

    @Override
    public Long getOwningConstraint( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return schemaStorage.indexRule( index.getLabelId(), index.getPropertyKeyId() ).getOwningConstraint();
    }

    @Override
    public long getCommittedIndexId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return schemaStorage.indexRule( index.getLabelId(), index.getPropertyKeyId() ).getId();
    }

    @Override
    public InternalIndexState getIndexState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getProxyForRule( indexId( descriptor ) ).getState();
    }

    private long indexId( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        try
        {
            return schemaStorage.indexRule( descriptor.getLabelId(), descriptor.getPropertyKeyId() ).getId();
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IndexNotFoundKernelException( e.getMessage(), e );
        }
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId, final long propertyKeyId )
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, UniquenessConstraintRule.class,
                                          labelId, new Predicate<UniquenessConstraintRule>()
        {
            @Override
            public boolean accept( UniquenessConstraintRule rule )
            {
                return rule.containsPropertyKeyId( propertyKeyId );
            }
        }
        );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId )
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, UniquenessConstraintRule.class,
                                          labelId, Predicates.<UniquenessConstraintRule>TRUE() );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints()
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, SchemaRule.Kind.UNIQUENESS_CONSTRAINT,
                                          Predicates.<UniquenessConstraintRule>TRUE() );
    }

    @Override
    public long getOrCreatePropertyKeyId( String propertyKey )
    {
        return propertyKeyTokenHolder.getOrCreateId( propertyKey );
    }

    @Override
    public long getPropertyKeyId( String propertyKey ) throws PropertyKeyNotFoundException
    {
        try
        {
            return propertyKeyTokenHolder.getIdByName( propertyKey );
        }
        catch ( TokenNotFoundException e )
        {
            throw new PropertyKeyNotFoundException( propertyKey, e );
        }
    }

    @Override
    public String getPropertyKeyName( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        try
        {
            return propertyKeyTokenHolder.getTokenById( (int) propertyKeyId ).getKey();
        }
        catch ( TokenNotFoundException e )
        {
            throw new PropertyKeyIdNotFoundException( propertyKeyId, e );
        }
    }

    @Override
    public Iterator<Long> listNodePropertyKeys( long nodeId )
    {
        // TODO: This is temporary, it should be split up to handle tx state up in the correct layers, this is just
        // a first step to move it into the kernel.
        return map( propertyStringToId,
                    nodeManager.getNodeForProxy( nodeId, null ).getPropertyKeys( nodeManager ).iterator() );
    }

    @Override
    public Iterator<Long> listRelationshipPropertyKeys( long relId )
    {
        // TODO: This is temporary, it should be split up to handle tx state up in the correct layers, this is just
        // a first step to move it into the kernel.
        return map( propertyStringToId,
                    nodeManager.getRelationshipForProxy( relId, null ).getPropertyKeys( nodeManager ).iterator() );
    }

    @Override
    public Object getNodePropertyValue( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, PropertyNotFoundException, EntityNotFoundException
    {
        try
        {
            return nodeManager.getNodeForProxy( nodeId, null ).getProperty( nodeManager, (int) propertyKeyId );
        }
        catch ( IllegalStateException e )
        {
            throw new EntityNotFoundException( "Unable to load node " + nodeId + ".", e );
        }
        catch ( NotFoundException e )
        {
            throw new PropertyNotFoundException(
                    "No property with id " + propertyKeyId + " on node with id " + nodeId, e );
        }
    }

    @Override
    public boolean nodeHasProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            String propertyKey = getPropertyKeyName( propertyKeyId );
            return nodeManager.getNodeForProxy( nodeId, null ).hasProperty( nodeManager, propertyKey );
        }
        catch ( IllegalStateException e )
        {
            throw new EntityNotFoundException( "Unable to load node " + nodeId + ".", e );
        }
    }

    @Override
    public void nodeSetPropertyValue( long nodeId, long propertyKeyId, Object value )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            // TODO: Move locking to LockingStatementContext et cetera, don't create a new node proxy for every call!
            String propertyKey = getPropertyKeyName( propertyKeyId );
            NodeImpl nodeImpl = nodeManager.getNodeForProxy( nodeId, LockType.WRITE );
            NodeProxy nodeProxy = nodeManager.newNodeProxyById( nodeId );
            nodeImpl.setProperty( nodeManager, nodeProxy, propertyKey, value );
        }
        catch ( IllegalStateException e )
        {
            throw new EntityNotFoundException( "Unable to load node " + nodeId + ".", e );
        }
    }

    @Override
    public Object nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            // TODO: Move locking to LockingStatementContext et cetera, don't create a new node proxy for every call!
            String propertyKey = getPropertyKeyName( propertyKeyId );
            NodeImpl nodeImpl = nodeManager.getNodeForProxy( nodeId, LockType.WRITE );
            NodeProxy nodeProxy = nodeManager.newNodeProxyById( nodeId );
            return nodeImpl.removeProperty( nodeManager, nodeProxy, propertyKey );
        }
        catch ( IllegalStateException e )
        {
            throw new EntityNotFoundException( "Unable to load node " + nodeId + ".", e );
        }
    }

    @Override
    public Iterator<Long> exactIndexLookup( IndexDescriptor index, Object value ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory.newReader( indexId( index ) ).lookup( value );
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        throw new UnsupportedOperationException( "Schema state is not handled by the stores" );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        throw new UnsupportedOperationException( "Schema state is not handled by the stores" );
    }
}
