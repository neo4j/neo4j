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

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.TransactionalException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintCreationKernelException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.state.TxState;

import static java.util.Collections.emptyList;

import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;

public class StateHandlingStatementContext implements
    EntityReadOperations,
    EntityWriteOperations,
    SchemaReadOperations,
    SchemaWriteOperations
{
    private final EntityReadOperations entityReadDelegate;
    private final SchemaReadOperations schemaReadDelegate;
    private final AuxiliaryStoreOperations auxStoreOps;
    private final TxState state;
    private final ConstraintIndexCreator constraintIndexCreator;

    public StateHandlingStatementContext(
            EntityReadOperations entityReadDelegate,
            SchemaReadOperations schemaReadDelegate,
            AuxiliaryStoreOperations auxStoreOps, TxState state, ConstraintIndexCreator constraintIndexCreator )
    {
        this.entityReadDelegate = entityReadDelegate;
        this.schemaReadDelegate = schemaReadDelegate;
        this.auxStoreOps = auxStoreOps;
        this.state = state;
        this.constraintIndexCreator = constraintIndexCreator;
    }

    @Override
    public void nodeDelete( long nodeId )
    {
        auxStoreOps.nodeDelete( nodeId );
        state.nodeDelete( nodeId );
    }

    @Override
    public void relationshipDelete( long relationshipId )
    {
        auxStoreOps.relationshipDelete( relationshipId );
        state.relationshipDelete( relationshipId );
    }

    @Override
    public boolean nodeHasLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        if ( state.hasChanges() )
        {
            if ( state.nodeIsDeletedInThisTx( nodeId ) )
            {
                return false;
            }

            if ( state.nodeIsAddedInThisTx( nodeId ) )
            {
                Boolean labelState = state.getLabelState( nodeId, labelId );
                return labelState != null && labelState;
            }

            Boolean labelState = state.getLabelState( nodeId, labelId );
            if ( labelState != null )
            {
                return labelState;
            }
        }

        return entityReadDelegate.nodeHasLabel( nodeId, labelId );
    }

    @Override
    public Iterator<Long> nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        if ( state.nodeIsDeletedInThisTx( nodeId ) )
        {
            return IteratorUtil.emptyIterator();
        }

        if ( state.nodeIsAddedInThisTx( nodeId ) )
        {
            return state.getNodeStateLabelDiffSets( nodeId ).getAdded().iterator();
        }

        Iterator<Long> committed = entityReadDelegate.nodeGetLabels( nodeId );
        return state.getNodeStateLabelDiffSets( nodeId ).apply( committed );
    }

    @Override
    public boolean nodeAddLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        if ( nodeHasLabel( nodeId, labelId ) )
        {
            // Label is already in state or in store, no-op
            return false;
        }

        state.nodeAddLabel( labelId, nodeId );
        return true;
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        if ( !nodeHasLabel( nodeId, labelId ) )
        {
            // Label does not exist in state nor in store, no-op
            return false;
        }

        state.nodeRemoveLabel( labelId, nodeId );

        return true;
    }

    @Override
    public Iterator<Long> nodesGetForLabel( long labelId )
    {
        Iterator<Long> committed = entityReadDelegate.nodesGetForLabel( labelId );
        if ( !state.hasChanges() )
        {
            return committed;
        }

        return state.getDeletedNodes().apply( state.getNodesWithLabelChanged( labelId ).apply( committed ) );
    }

    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.addIndexRule( rule );
        return rule;
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.addConstraintIndexRule( rule );
        return rule;
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.dropIndex( descriptor );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.dropConstraintIndex( descriptor );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        UniquenessConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );
        if ( !state.unRemoveConstraint( constraint ) )
        {
            for ( Iterator<UniquenessConstraint> it = schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                    labelId, propertyKeyId ); it.hasNext(); )
            {
                if ( it.next().equals( labelId, propertyKeyId ) )
                {
                    return constraint;
                }
            }
            
            try
            {
                long indexId = constraintIndexCreator.createUniquenessConstraintIndex(
                        this, labelId, propertyKeyId );
                state.addConstraint( constraint, indexId );
            }
            catch ( TransactionalException e )
            {
                throw new ConstraintCreationKernelException( constraint, e );
            }
            catch ( KernelException e )
            {
                throw new ConstraintCreationKernelException( constraint, e );
            }
        }
        return constraint;
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        return applyConstraintsDiff( schemaReadDelegate.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ),
                labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
    {
        return applyConstraintsDiff( schemaReadDelegate.constraintsGetForLabel( labelId ), labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        return applyConstraintsDiff( schemaReadDelegate.constraintsGetAll() );
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( Iterator<UniquenessConstraint> constraints,
                                                                 long labelId, long propertyKeyId )
    {
        DiffSets<UniquenessConstraint> diff = state.constraintsChangesForLabelAndProperty( labelId, propertyKeyId );
        if ( diff != null )
        {
            return diff.apply( constraints );
        }
        return constraints;
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( Iterator<UniquenessConstraint> constraints,
                                                                 long labelId )
    {
        DiffSets<UniquenessConstraint> diff = state.constraintsChangesForLabel( labelId );
        if ( diff != null )
        {
            return diff.apply( constraints );
        }
        return constraints;
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( Iterator<UniquenessConstraint> constraints )
    {
        DiffSets<UniquenessConstraint> diff = state.constraintsChanges();
        if ( diff != null )
        {
            return diff.apply( constraints );
        }
        return constraints;
    }

    @Override
    public void constraintDrop( UniquenessConstraint constraint )
    {
        state.dropConstraint( constraint );
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        Iterable<IndexDescriptor> committedRules;
        try
        {
            committedRules = option( schemaReadDelegate.indexesGetForLabelAndPropertyKey( labelId, propertyKey ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            committedRules = emptyList();
        }
        DiffSets<IndexDescriptor> ruleDiffSet = state.getIndexDiffSetsByLabel( labelId );
        Iterator<IndexDescriptor> rules = ruleDiffSet.apply( committedRules.iterator() );
        IndexDescriptor single = singleOrNull( rules );
        if ( single == null )
        {
            throw new SchemaRuleNotFoundException( "Index rule for label:" + labelId + " and property:" +
                                                   propertyKey + " not found" );
        }
        return single;
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        if ( checkIndexState( descriptor, state.getIndexDiffSetsByLabel( descriptor.getLabelId() ) ) )
        {
            return InternalIndexState.POPULATING;
        }
        if ( checkIndexState( descriptor, state.getConstraintIndexDiffSetsByLabel( descriptor.getLabelId() ) ) )
        {
            return InternalIndexState.POPULATING;
        }

        return schemaReadDelegate.indexGetState( descriptor );
    }

    private boolean checkIndexState( IndexDescriptor indexRule, DiffSets<IndexDescriptor> diffSet )
            throws IndexNotFoundKernelException
    {
        if ( diffSet.isAdded( indexRule ) )
        {
            return true;
        }
        if ( diffSet.isRemoved( indexRule ) )
        {
            throw new IndexNotFoundKernelException( String.format( "Index for label id %d on property id %d has been " +
                                                                   "dropped in this transaction.",
                                                                   indexRule.getLabelId(),
                                                                   indexRule.getPropertyKeyId() ) );
        }
        return false;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        return state.getIndexDiffSetsByLabel( labelId ).apply( schemaReadDelegate.indexesGetForLabel( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return state.getIndexDiffSets().apply( schemaReadDelegate.indexesGetAll() );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        return state.getConstraintIndexDiffSetsByLabel( labelId ).apply( schemaReadDelegate.uniqueIndexesGetForLabel(
                labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        return state.getConstraintIndexDiffSets().apply( schemaReadDelegate.uniqueIndexesGetAll() );
    }

    @Override
    public Iterator<Long> nodesGetFromIndexLookup( IndexDescriptor index, final Object value )
            throws IndexNotFoundKernelException
    {
        // Start with nodes where the given property has changed
        DiffSets<Long> diff = state.getNodesWithChangedProperty( index.getPropertyKeyId(), value );

        // Ensure remaining nodes have the correct label
        diff = diff.filterAdded( new HasLabelFilter( index.getLabelId() ) );

        // Include newly labeled nodes that already had the correct property
        HasPropertyFilter hasPropertyFilter = new HasPropertyFilter( index.getPropertyKeyId(), value );
        Iterator<Long> addedNodesWithLabel = state.getNodesWithLabelAdded( index.getLabelId() ).iterator();
        diff.addAll( Iterables.filter( hasPropertyFilter, addedNodesWithLabel ) );

        // Remove de-labeled nodes that had the correct value before
        Set<Long> removedNodesWithLabel = state.getNodesWithLabelChanged( index.getLabelId() ).getRemoved();
        diff.removeAll( Iterables.filter( hasPropertyFilter, removedNodesWithLabel.iterator() ) );

        // Apply to actual index lookup
        return state.getDeletedNodes().apply( diff.apply( entityReadDelegate.nodesGetFromIndexLookup( index, value ) ) );
    }

    @Override
    public Property nodeSetProperty( long nodeId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = nodeGetProperty( nodeId, property.propertyKeyId() );
            if ( existingProperty.isNoProperty() )
            {
                auxStoreOps.nodeAddStoreProperty( nodeId, property );
            }
            else
            {
                auxStoreOps.nodeChangeStoreProperty( nodeId, existingProperty, property );
            }
            state.nodeReplaceProperty( nodeId, existingProperty, property );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = relationshipGetProperty( relationshipId, property.propertyKeyId() );
            if ( existingProperty.isNoProperty() )
            {
                auxStoreOps.relationshipAddStoreProperty( relationshipId, property );
            }
            else
            {
                auxStoreOps.relationshipChangeStoreProperty( relationshipId, existingProperty, property );
            }
            state.relationshipReplaceProperty( relationshipId, existingProperty, property );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }
    
    @Override
    public Property graphSetProperty( Property property ) throws PropertyKeyIdNotFoundException
    {
        try
        {
            Property existingProperty = graphGetProperty( property.propertyKeyId() );
            if ( existingProperty.isNoProperty() )
            {
                auxStoreOps.graphAddStoreProperty( property );
            }
            else
            {
                auxStoreOps.graphChangeStoreProperty( existingProperty, property );
            }
            state.graphReplaceProperty( existingProperty, property );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = nodeGetProperty( nodeId, propertyKeyId );
            if ( !existingProperty.isNoProperty() )
            {
                auxStoreOps.nodeRemoveStoreProperty( nodeId, existingProperty );
            }
            state.nodeRemoveProperty( nodeId, existingProperty );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = relationshipGetProperty( relationshipId, propertyKeyId );
            if ( !existingProperty.isNoProperty() )
            {
                auxStoreOps.relationshipRemoveStoreProperty( relationshipId, existingProperty );
            }
            state.relationshipRemoveProperty( relationshipId, existingProperty );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }
    
    @Override
    public Property graphRemoveProperty( long propertyKeyId )
            throws PropertyKeyIdNotFoundException
    {
        try
        {
            Property existingProperty = graphGetProperty( propertyKeyId );
            if ( !existingProperty.isNoProperty() )
            {
                auxStoreOps.graphRemoveStoreProperty( existingProperty );
            }
            state.graphRemoveProperty( existingProperty );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }
    
    @Override
    public Iterator<Long> nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException
    {
        return new IteratorWrapper<Long,Property>( nodeGetAllProperties( nodeId ) )
        {
            @Override
            protected Long underlyingObjectToObject( Property property )
            {
                return property.propertyKeyId();
            }
        };
    }
    
    @Override
    public Property nodeGetProperty( long nodeId, long propertyKeyId ) throws EntityNotFoundException
    {
        Iterator<Property> properties = nodeGetAllProperties( nodeId );
        while ( properties.hasNext() )
        {
            Property property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property;
            }
        }
        return Property.noNodeProperty( nodeId, propertyKeyId );
    }
    
    @Override
    public boolean nodeHasProperty( long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        return !nodeGetProperty( nodeId, propertyKeyId ).isNoProperty();
    }

    @Override
    public Iterator<Property> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        if ( state.nodeIsAddedInThisTx( nodeId ) )
        {
            return state.getNodePropertyDiffSets( nodeId ).getAdded().iterator();
        }
        if ( state.nodeIsDeletedInThisTx( nodeId ) )
        {
            // TODO Throw IllegalStateException to conform with beans API. We may want to introduce
            // EntityDeletedException instead and use it instead of returning empty values in similar places
            throw new IllegalStateException( "Node " + nodeId + " has been deleted" );
        }
        return state.getNodePropertyDiffSets( nodeId ).apply( entityReadDelegate.nodeGetAllProperties( nodeId ) );
    }
    
    @Override
    public Iterator<Long> relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException
    {
        return new IteratorWrapper<Long,Property>( relationshipGetAllProperties( relationshipId ) )
        {
            @Override
            protected Long underlyingObjectToObject( Property property )
            {
                return property.propertyKeyId();
            }
        };
    }
    
    @Override
    public Property relationshipGetProperty( long relationshipId, long propertyKeyId )
            throws EntityNotFoundException
    {
        Iterator<Property> properties = relationshipGetAllProperties( relationshipId );
        while ( properties.hasNext() )
        {
            Property property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property;
            }
        }
        return Property.noRelationshipProperty( relationshipId, propertyKeyId );
    }
    
    @Override
    public boolean relationshipHasProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        return !relationshipGetProperty( relationshipId, propertyKeyId ).isNoProperty();
    }

    @Override
    public Iterator<Property> relationshipGetAllProperties( long relationshipId ) throws EntityNotFoundException
    {
        if ( state.relationshipIsAddedInThisTx( relationshipId ) )
        {
            return state.getRelationshipPropertyDiffSets( relationshipId ).getAdded().iterator();
        }
        if ( state.relationshipIsDeletedInThisTx( relationshipId ) )
        {
            // TODO Throw IllegalStateException to conform with beans API. We may want to introduce
            // EntityDeletedException instead and use it instead of returning empty values in similar places
            throw new IllegalStateException( "Relationship " + relationshipId + " has been deleted" );
        }
        return state.getRelationshipPropertyDiffSets( relationshipId )
                    .apply( entityReadDelegate.relationshipGetAllProperties( relationshipId ) );
    }
    
    @Override
    public Iterator<Long> graphGetPropertyKeys()
    {
        return new IteratorWrapper<Long,Property>( graphGetAllProperties() )
        {
            @Override
            protected Long underlyingObjectToObject( Property property )
            {
                return property.propertyKeyId();
            }
        };
    }
    
    @Override
    public Property graphGetProperty( long propertyKeyId )
    {
        Iterator<Property> properties = graphGetAllProperties();
        while ( properties.hasNext() )
        {
            Property property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property;
            }
        }
        return Property.noGraphProperty( propertyKeyId );
    }
    
    @Override
    public boolean graphHasProperty( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        return !graphGetProperty( propertyKeyId ).isNoProperty();
    }
    
    @Override
    public Iterator<Property> graphGetAllProperties()
    {
        return state.getGraphPropertyDiffSets().apply( entityReadDelegate.graphGetAllProperties() );
    }

    private class HasPropertyFilter implements Predicate<Long>
    {
        private final Object value;
        private final long propertyKeyId;

        public HasPropertyFilter( long propertyKeyId, Object value )
        {
            this.value = value;
            this.propertyKeyId = propertyKeyId;
        }

        @Override
        public boolean accept( Long nodeId )
        {
            try
            {
                if ( state.nodeIsDeletedInThisTx( nodeId ) )
                {
                    return false;
                }
                Property property = nodeGetProperty( nodeId, propertyKeyId );
                if ( property.isNoProperty() )
                {
                    return false;
                }
                return property.valueEquals( value );
            }
            catch ( EntityNotFoundException e )
            {
                return false;
            }
        }
    }

    private class HasLabelFilter implements Predicate<Long>
    {
        private final long labelId;

        public HasLabelFilter( long labelId )
        {
            this.labelId = labelId;
        }

        @Override
        public boolean accept( Long nodeId )
        {
            try
            {
                return nodeHasLabel( nodeId, labelId );
            }
            catch ( EntityNotFoundException e )
            {
                return false;
            }
        }
    }
    
    // === TODO Below is unnecessary delegate methods

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( index );
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return schemaReadDelegate.indexGetCommittedId( index );
    }
    
    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetFailure( descriptor );
    }
}
