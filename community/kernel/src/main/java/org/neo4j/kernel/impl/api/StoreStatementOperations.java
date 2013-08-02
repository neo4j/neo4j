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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asPrimitiveIterator;
import static org.neo4j.helpers.collection.IteratorUtil.contains;
import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;

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
public class StoreStatementOperations implements
    KeyReadOperations,
    KeyWriteOperations,
    EntityReadOperations,
    EntityWriteOperations,
    SchemaReadOperations,
    AuxiliaryStoreOperations

{
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
    
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final NeoStore neoStore;
    private final IndexingService indexService;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final PropertyStore propertyStore;
    private final SchemaStorage schemaStorage;
    
    // TODO this is here since the move of properties from Primitive and friends to the Kernel API.
    // ideally we'd have StateHandlingStatementContext not delegate setProperty to this StoreStatementContext,
    // but talk to use before commit instead.
    private final PersistenceManager persistenceManager;

    public StoreStatementOperations( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                                  SchemaStorage schemaStorage, NeoStore neoStore,
                                  PersistenceManager persistenceManager,
                                  IndexingService indexService )
    {
        this.schemaStorage = schemaStorage;
        assert neoStore != null : "No neoStore provided";

        this.indexService = indexService;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.neoStore = neoStore;
        this.nodeStore = neoStore.getNodeStore();
        this.relationshipStore = neoStore.getRelationshipStore();
        this.propertyStore = neoStore.getPropertyStore();
        this.persistenceManager = persistenceManager;
    }

    private UnsupportedOperationException shouldNotManipulateStoreDirectly()
    {
        throw new UnsupportedOperationException(
                "The storage layer can not be written to directly, you have to go through a transaction." );
    }
    
    private UnsupportedOperationException shouldNotHaveReachedAllTheWayHere()
    {
        throw new UnsupportedOperationException(
                "This call should not reach all the way here" );
    }
    
    private UnsupportedOperationException shouldCallAuxiliaryInstead()
    {
        return new UnsupportedOperationException(
                "This shouldn't be called directly, but instead to an appropriate method in the " + 
                        AuxiliaryStoreOperations.class.getSimpleName() + " interface" );
    }
    
    @Override
    public long labelGetOrCreateForName( StatementState state, String label ) throws SchemaKernelException
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
                throw new TooManyLabelsException( e );
            }
            else
            {
                throw e;
            }
        }
    }

    @Override
    public long labelGetForName( StatementState state, String label ) throws LabelNotFoundKernelException
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
    public boolean nodeHasLabel( StatementState state, long nodeId, long labelId )
    {
        try
        {
            return contains( nodeGetLabels( state, nodeId ), labelId );
        }
        catch ( InvalidRecordException e )
        {
            return false;
        }
    }

    @Override
    public PrimitiveLongIterator nodeGetLabels( StatementState state, long nodeId )
    {
        try
        {
            return asPrimitiveIterator( parseLabelsField( nodeStore.getRecord( nodeId ) ).get( nodeStore ) );
        }
        catch ( InvalidRecordException e )
        {   // TODO Might hide invalid dynamic record problem. It's here because this method
            // might get called with a nodeId that doesn't exist.
            return emptyPrimitiveLongIterator();
        }
    }

    @Override
    public String labelGetName( StatementState state, long labelId ) throws LabelNotFoundKernelException
    {
        try
        {
            return labelTokenHolder.getTokenById( (int) labelId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( "Label by id " + labelId, e );
        }
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( StatementState state, final long labelId )
    {
        final NodeStore nodeStore = neoStore.getNodeStore();
        final long highestId = nodeStore.getHighestPossibleIdInUse();

        return new AbstractPrimitiveLongIterator()
        {
            private long id = 0;

            {
                computeNext();
            }

            @Override
            protected void computeNext()
            {
                while ( id <= highestId )
                {
                    NodeRecord node = nodeStore.forceGetRecord( id++ );
                    if ( node.inUse() )
                    {
                        for ( long label : parseLabelsField( node ).get( nodeStore ) )
                        {
                            if ( label == labelId )
                            {
                                nextValue = node.getId();
                                hasNext = true;
                                return;
                            }
                        }
                    }
                }
                hasNext = false;
            }
        };
    }

    @Override
    public Iterator<Token> labelsGetAllTokens( StatementState state )
    {
        return labelTokenHolder.getAllTokens().iterator();
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( StatementState state, final long labelId, final long propertyKey )
            throws SchemaRuleNotFoundException
    {
        return descriptor( schemaStorage.indexRule( labelId, propertyKey ) );
    }

    private static IndexDescriptor descriptor( IndexRule ruleRecord )
    {
        return new IndexDescriptor( ruleRecord.getLabel(), ruleRecord.getPropertyKey() );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( StatementState state, final long labelId )
    {
        return getIndexDescriptorsFor( indexRules( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( StatementState state )
    {
        return getIndexDescriptorsFor( INDEX_RULES );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( StatementState state, final long labelId )
    {
        return getIndexDescriptorsFor( constraintIndexRules( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( StatementState state )
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
        Iterator<SchemaRule> filtered = filter( filter, neoStore.getSchemaStore().loadAllSchemaRules() );

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
    public Long indexGetOwningUniquenessConstraintId( StatementState state, IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        return schemaStorage.indexRule( index.getLabelId(), index.getPropertyKeyId() ).getOwningConstraint();
    }

    @Override
    public long indexGetCommittedId( StatementState state, IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return schemaStorage.indexRule( index.getLabelId(), index.getPropertyKeyId() ).getId();
    }

    @Override
    public InternalIndexState indexGetState( StatementState state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getProxyForRule( indexId( descriptor ) ).getState();
    }
    
    @Override
    public String indexGetFailure( StatementState state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getProxyForRule( indexId( descriptor ) ).getPopulationFailure().asString();
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
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( StatementState state, 
            long labelId, final long propertyKeyId )
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
    public Iterator<UniquenessConstraint> constraintsGetForLabel( StatementState state, long labelId )
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, UniquenessConstraintRule.class,
                labelId, Predicates.<UniquenessConstraintRule>TRUE() );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll( StatementState state )
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, SchemaRule.Kind.UNIQUENESS_CONSTRAINT,
                Predicates.<UniquenessConstraintRule>TRUE() );
    }

    @Override
    public long propertyKeyGetOrCreateForName( StatementState state, String propertyKey )
    {
        return propertyKeyTokenHolder.getOrCreateId( propertyKey );
    }

    @Override
    public long propertyKeyGetForName( StatementState state, String propertyKey ) throws PropertyKeyNotFoundException
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
    public String propertyKeyGetName( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        try
        {
            return propertyKeyTokenHolder.getTokenById( (int) propertyKeyId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new PropertyKeyIdNotFoundException( propertyKeyId, e );
        }
    }
    
    @Override
    public Iterator<Property> nodeGetAllProperties( StatementState state, long nodeId ) throws EntityNotFoundException
    {
        try
        {
            return loadAllPropertiesOf( nodeStore.getRecord( nodeId ) );
        }
        catch ( InvalidRecordException e )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId, e );
        }
    }

    @Override
    public Iterator<Property> relationshipGetAllProperties( StatementState state, long relationshipId ) throws EntityNotFoundException
    {
        try
        {
            return loadAllPropertiesOf( relationshipStore.getRecord( relationshipId ) );
        }
        catch ( InvalidRecordException e )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId, e );
        }
    }
    
    @Override
    public Iterator<Property> graphGetAllProperties( StatementState state )
    {
        return loadAllPropertiesOf( neoStore.asRecord() );
    }

    private Iterator<Property> loadAllPropertiesOf( PrimitiveRecord primitiveRecord )
    {
        Collection<PropertyRecord> records = propertyStore.getPropertyRecordChain( primitiveRecord.getNextProp() );
        if ( null == records )
        {
            return IteratorUtil.emptyIterator();
        }
        List<Property> properties = new ArrayList<>();
        for ( PropertyRecord record : records )
        {
            for ( PropertyBlock block : record.getPropertyBlocks() )
            {
                properties.add( block.getType().readProperty( block.getKeyIndexId(), block, propertyStore ) );
            }
        }
        return properties.iterator();
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( StatementState state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        return state.indexReaderFactory().newReader( indexId( index ) ).lookup( value );
    }

    @Override
    public void nodeAddStoreProperty( long nodeId, Property property )
            throws PropertyNotFoundException

    {
        persistenceManager.nodeAddProperty( nodeId, (int) property.propertyKeyId(), property.value() );
    }

    @Override
    public void relationshipAddStoreProperty( long relationshipId, Property property )
            throws PropertyNotFoundException
    {
        persistenceManager.relAddProperty( relationshipId, (int) property.propertyKeyId(), property.value() );
    }
    
    @Override
    public void graphAddStoreProperty( Property property ) throws PropertyNotFoundException
    {
        persistenceManager.graphAddProperty( (int) property.propertyKeyId(), property.value() );
    }

    @Override
    public void nodeChangeStoreProperty( long nodeId, Property previousProperty, Property property )
            throws PropertyNotFoundException
    {
        // TODO this should change. We don't have the property record id here, so we PersistenceManager
        // has been changed to only accept the property key and it will find it among the property records
        // on demand. This change was made instead of cramming in record id into the Property objects,
        persistenceManager.nodeChangeProperty( nodeId, (int) property.propertyKeyId(), property.value() );
    }

    @Override
    public void relationshipChangeStoreProperty( long relationshipId, Property previousProperty, Property property )
            throws PropertyNotFoundException
    {
        // TODO this should change. We don't have the property record id here, so we PersistenceManager
        // has been changed to only accept the property key and it will find it among the property records
        // on demand. This change was made instead of cramming in record id into the Property objects,
        persistenceManager.relChangeProperty( relationshipId,
                (int) property.propertyKeyId(), property.value() );
    }
    
    @Override
    public void graphChangeStoreProperty( Property previousProperty, Property property )
            throws PropertyNotFoundException
    {
        // TODO this should change. We don't have the property record id here, so we PersistenceManager
        // has been changed to only accept the property key and it will find it among the property records
        // on demand. This change was made instead of cramming in record id into the Property objects,
        persistenceManager.graphChangeProperty( (int) property.propertyKeyId(), property.value() );
    }

    @Override
    public void nodeRemoveStoreProperty( long nodeId, Property property )
    {
        // TODO this should change. We don't have the property record id here, so we PersistenceManager
        // has been changed to only accept the property key and it will find it among the property records
        // on demand. This change was made instead of cramming in record id into the Property objects,
        persistenceManager.nodeRemoveProperty( nodeId, (int) property.propertyKeyId() );
    }

    @Override
    public void relationshipRemoveStoreProperty( long relationshipId, Property property )
    {
        // TODO this should change. We don't have the property record id here, so we PersistenceManager
        // has been changed to only accept the property key and it will find it among the property records
        // on demand. This change was made instead of cramming in record id into the Property objects,
        persistenceManager.relRemoveProperty( relationshipId, (int) property.propertyKeyId() );
    }
    
    @Override
    public void graphRemoveStoreProperty( Property property )
    {
        // TODO this should change. We don't have the property record id here, so we PersistenceManager
        // has been changed to only accept the property key and it will find it among the property records
        // on demand. This change was made instead of cramming in record id into the Property objects,
        persistenceManager.graphRemoveProperty( (int) property.propertyKeyId() );
    }

    @Override
    public Property nodeSetProperty( StatementState state, long nodeId, Property property )
    {
        throw shouldCallAuxiliaryInstead();
    }

    @Override
    public Property relationshipSetProperty( StatementState state, long relationshipId, Property property )
    {
        throw shouldCallAuxiliaryInstead();
    }
    
    @Override
    public Property graphSetProperty( StatementState state, Property property )
    {
        throw shouldCallAuxiliaryInstead();
    }
    
    @Override
    public Property nodeRemoveProperty( StatementState state, long nodeId, long propertyKeyId )
    {
        throw shouldCallAuxiliaryInstead();
    }

    @Override
    public Property relationshipRemoveProperty( StatementState state, long relationshipId, long propertyKeyId )
    {
        throw shouldCallAuxiliaryInstead();
    }
    
    @Override
    public Property graphRemoveProperty( StatementState state, long propertyKeyId )
    {
        throw shouldCallAuxiliaryInstead();
    }
    
    @Override
    public void nodeDelete( StatementState state, long nodeId )
    {
        throw shouldCallAuxiliaryInstead();
    }
    
    @Override
    public void relationshipDelete( StatementState state, long relationshipId )
    {
        throw shouldCallAuxiliaryInstead();
    }
    
    @Override
    public void nodeDelete( long nodeId )
    {
        throw shouldNotManipulateStoreDirectly();
    }

    @Override
    public void relationshipDelete( long relationshipId )
    {
        throw shouldNotManipulateStoreDirectly();
    }

    @Override
    public boolean nodeAddLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw shouldNotManipulateStoreDirectly();
    }

    @Override
    public boolean nodeRemoveLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw shouldNotManipulateStoreDirectly();
    }

    @Override
    public Property nodeGetProperty( StatementState state, long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public Property relationshipGetProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public Property graphGetProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public boolean nodeHasProperty( StatementState state, long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public boolean relationshipHasProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public boolean graphHasProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public PrimitiveLongIterator nodeGetPropertyKeys( StatementState state, long nodeId ) throws EntityNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public PrimitiveLongIterator relationshipGetPropertyKeys( StatementState state, long relationshipId ) throws EntityNotFoundException
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }

    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( StatementState state )
    {
        throw shouldNotHaveReachedAllTheWayHere();
    }
}
