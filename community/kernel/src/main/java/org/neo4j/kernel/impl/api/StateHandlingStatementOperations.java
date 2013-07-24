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
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.state.TxState;

import static java.util.Collections.emptyList;

import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;
import static org.neo4j.helpers.collection.IteratorUtil.toPrimitiveLongIterator;

public class StateHandlingStatementOperations implements
    EntityReadOperations,
    EntityWriteOperations,
    SchemaReadOperations,
    SchemaWriteOperations
{
    private final EntityReadOperations entityReadDelegate;
    private final SchemaReadOperations schemaReadDelegate;
    private final AuxiliaryStoreOperations auxStoreOps;
    private final ConstraintIndexCreator constraintIndexCreator;

    public StateHandlingStatementOperations(
            EntityReadOperations entityReadDelegate,
            SchemaReadOperations schemaReadDelegate,
            AuxiliaryStoreOperations auxStoreOps, ConstraintIndexCreator constraintIndexCreator )
    {
        this.entityReadDelegate = entityReadDelegate;
        this.schemaReadDelegate = schemaReadDelegate;
        this.auxStoreOps = auxStoreOps;
        this.constraintIndexCreator = constraintIndexCreator;
    }

    @Override
    public void nodeDelete( StatementState state, long nodeId )
    {
        auxStoreOps.nodeDelete( nodeId );
        state.txState().nodeDoDelete( nodeId );
    }

    @Override
    public void relationshipDelete( StatementState state, long relationshipId )
    {
        auxStoreOps.relationshipDelete( relationshipId );
        state.txState().relationshipDoDelete( relationshipId );
    }

    @Override
    public boolean nodeHasLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().nodeIsDeletedInThisTx( nodeId ) )
            {
                return false;
            }

            if ( state.txState().nodeIsAddedInThisTx( nodeId ) )
            {
                TxState.UpdateTriState labelState = state.txState().labelState( nodeId, labelId );
                return labelState.isTouched() && labelState.isAdded();
            }

            TxState.UpdateTriState labelState = state.txState().labelState( nodeId, labelId );
            if ( labelState.isTouched() )
            {
                return labelState.isAdded();
            }
        }

        return entityReadDelegate.nodeHasLabel( state, nodeId, labelId );
    }

    @Override
    public PrimitiveLongIterator nodeGetLabels( StatementState state, long nodeId ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().nodeIsDeletedInThisTx( nodeId ) )
            {
                return IteratorUtil.emptyPrimitiveLongIterator();
            }

            if ( state.txState().nodeIsAddedInThisTx( nodeId ) )
            {
                return
                    toPrimitiveLongIterator( state.txState().nodeStateLabelDiffSets( nodeId ).getAdded().iterator() );
            }

            return state.txState().nodeStateLabelDiffSets( nodeId ).applyPrimitiveLongIterator(
                    entityReadDelegate.nodeGetLabels( state, nodeId ) );
        }

        return entityReadDelegate.nodeGetLabels( state, nodeId );
    }

    @Override
    public boolean nodeAddLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        if ( nodeHasLabel( state, nodeId, labelId ) )
        {
            // Label is already in state or in store, no-op
            return false;
        }

        state.txState().nodeDoAddLabel( labelId, nodeId );
        return true;
    }

    @Override
    public boolean nodeRemoveLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        if ( !nodeHasLabel( state, nodeId, labelId ) )
        {
            // Label does not exist in state nor in store, no-op
            return false;
        }

        state.txState().nodeDoRemoveLabel( labelId, nodeId );

        return true;
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( StatementState state, long labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            PrimitiveLongIterator wLabelChanges =
                    state.txState().nodesWithLabelChanged( labelId ).applyPrimitiveLongIterator(
                            entityReadDelegate.nodesGetForLabel( state, labelId ) );
            return state.txState().nodesDeletedInTx().applyPrimitiveLongIterator( wLabelChanges );
        }

        return entityReadDelegate.nodesGetForLabel( state, labelId );
    }

    @Override
    public IndexDescriptor indexCreate( StatementState state, long labelId, long propertyKey )
            throws SchemaKernelException
    {
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.txState().indexRuleDoAdd( rule );
        return rule;
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( StatementState state, long labelId, long propertyKey )
            throws SchemaKernelException
    {
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.txState().constraintIndexRuleDoAdd( rule );
        return rule;
    }

    @Override
    public void indexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.txState().indexDoDrop( descriptor );
    }

    @Override
    public void uniqueIndexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.txState().constraintIndexDoDrop( descriptor );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( StatementState state, long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        UniquenessConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );
        if ( !state.txState().constraintDoUnRemove( constraint ) )
        {
            for ( Iterator<UniquenessConstraint> it = schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                    state, labelId, propertyKeyId ); it.hasNext(); )
            {
                if ( it.next().equals( labelId, propertyKeyId ) )
                {
                    return constraint;
                }
            }
            
            try
            {
                long indexId = constraintIndexCreator.createUniquenessConstraintIndex(
                        state, this, labelId, propertyKeyId );
                state.txState().constraintDoAdd( constraint, indexId );
            }
            catch ( TransactionalException | KernelException e )
            {
                throw new ConstraintCreationKernelException( constraint, e );
            }
        }
        return constraint;
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( StatementState state, long labelId, long propertyKeyId )
    {
        return applyConstraintsDiff( state, schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                state, labelId, propertyKeyId ), labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( StatementState state, long labelId )
    {
        return applyConstraintsDiff( state, schemaReadDelegate.constraintsGetForLabel( state, labelId ), labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll( StatementState state )
    {
        return applyConstraintsDiff( state, schemaReadDelegate.constraintsGetAll( state ) );
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( StatementState state,
            Iterator<UniquenessConstraint> constraints, long labelId, long propertyKeyId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            DiffSets<UniquenessConstraint> diff =
                    state.txState().constraintsChangesForLabelAndProperty( labelId, propertyKeyId );
            if ( diff != null )
            {
                return diff.apply( constraints );
            }
        }

        return constraints;
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( StatementState state,
            Iterator<UniquenessConstraint> constraints, long labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            DiffSets<UniquenessConstraint> diff = state.txState().constraintsChangesForLabel( labelId );
            if ( diff != null )
            {
                return diff.apply( constraints );
            }
        }

        return constraints;
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( StatementState state,
            Iterator<UniquenessConstraint> constraints )
    {
        if ( state.hasTxStateWithChanges() )
        {
            DiffSets<UniquenessConstraint> diff = state.txState().constraintsChanges();
            if ( diff != null )
            {
                return diff.apply( constraints );
            }
        }

        return constraints;
    }

    @Override
    public void constraintDrop( StatementState state, UniquenessConstraint constraint )
    {
        state.txState().constraintDoDrop( constraint );
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( StatementState state, long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        Iterable<IndexDescriptor> committedRules;
        try
        {
            committedRules = option( schemaReadDelegate.indexesGetForLabelAndPropertyKey( state, labelId, propertyKey ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            committedRules = emptyList();
        }
        DiffSets<IndexDescriptor> ruleDiffSet = state.txState().indexDiffSetsByLabel( labelId );

        Iterator<IndexDescriptor> rules =
            state.hasTxStateWithChanges() ? ruleDiffSet.apply( committedRules.iterator() ) : committedRules.iterator();
        IndexDescriptor single = singleOrNull( rules );
        if ( single == null )
        {
            throw new SchemaRuleNotFoundException( "Index rule for label:" + labelId + " and property:" +
                                                   propertyKey + " not found" );
        }
        return single;
    }

    @Override
    public InternalIndexState indexGetState( StatementState state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        if ( state.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor, state.txState().indexDiffSetsByLabel( descriptor.getLabelId() ) ) )
            {
                return InternalIndexState.POPULATING;
            }
            if ( checkIndexState( descriptor, state.txState().constraintIndexDiffSetsByLabel( descriptor.getLabelId() ) ) )
            {
                return InternalIndexState.POPULATING;
            }
        }

        return schemaReadDelegate.indexGetState( state, descriptor );
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
    public Iterator<IndexDescriptor> indexesGetForLabel( StatementState state, long labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().indexDiffSetsByLabel( labelId )
                    .apply( schemaReadDelegate.indexesGetForLabel( state, labelId ) );
        }

        return schemaReadDelegate.indexesGetForLabel( state, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( StatementState state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().indexChanges().apply( schemaReadDelegate.indexesGetAll( state ) );
        }

        return schemaReadDelegate.indexesGetAll( state );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( StatementState state, long labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintIndexDiffSetsByLabel( labelId )
                    .apply( schemaReadDelegate.uniqueIndexesGetForLabel( state, labelId ) );
        }

        return schemaReadDelegate.uniqueIndexesGetForLabel( state, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( StatementState state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintIndexChanges()
                    .apply( schemaReadDelegate.uniqueIndexesGetAll( state ) );
        }

        return schemaReadDelegate.uniqueIndexesGetAll( state );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( StatementState state, IndexDescriptor index, final Object value )
            throws IndexNotFoundKernelException
    {
        if ( state.hasTxStateWithChanges() )
        {
            // Start with nodes where the given property has changed
            DiffSets<Long> diff = state.txState().nodesWithChangedProperty( index.getPropertyKeyId(), value );

            // Ensure remaining nodes have the correct label
            diff = diff.filterAdded( new HasLabelFilter( state, index.getLabelId() ) );

            // Include newly labeled nodes that already had the correct property
            HasPropertyFilter hasPropertyFilter = new HasPropertyFilter( state, index.getPropertyKeyId(), value );
            Iterator<Long> addedNodesWithLabel = state.txState().nodesWithLabelAdded( index.getLabelId() ).iterator();
            diff.addAll( Iterables.filter( hasPropertyFilter, addedNodesWithLabel ) );

            // Remove de-labeled nodes that had the correct value before
            Set<Long> removedNodesWithLabel = state.txState().nodesWithLabelChanged( index.getLabelId() ).getRemoved();
            diff.removeAll( Iterables.filter( hasPropertyFilter, removedNodesWithLabel.iterator() ) );

            // Apply to actual index lookup
            PrimitiveLongIterator committed = entityReadDelegate.nodesGetFromIndexLookup( state, index, value );
            return state.txState()
                    .nodesDeletedInTx().applyPrimitiveLongIterator( diff.applyPrimitiveLongIterator( committed ) );
        }

        return entityReadDelegate.nodesGetFromIndexLookup( state, index, value );
    }

    @Override
    public Property nodeSetProperty( StatementState state, long nodeId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = nodeGetProperty( state, nodeId, property.propertyKeyId() );
            if ( existingProperty.isNoProperty() )
            {
                auxStoreOps.nodeAddStoreProperty( nodeId, property );
            }
            else
            {
                auxStoreOps.nodeChangeStoreProperty( nodeId, existingProperty, property );
            }
            state.txState().nodeDoReplaceProperty( nodeId, existingProperty, property );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }

    @Override
    public Property relationshipSetProperty( StatementState state, long relationshipId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = relationshipGetProperty( state, relationshipId, property.propertyKeyId() );
            if ( existingProperty.isNoProperty() )
            {
                auxStoreOps.relationshipAddStoreProperty( relationshipId, property );
            }
            else
            {
                auxStoreOps.relationshipChangeStoreProperty( relationshipId, existingProperty, property );
            }
            state.txState().relationshipDoReplaceProperty( relationshipId, existingProperty, property );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }
    
    @Override
    public Property graphSetProperty( StatementState state, Property property ) throws PropertyKeyIdNotFoundException
    {
        try
        {
            Property existingProperty = graphGetProperty( state, property.propertyKeyId() );
            if ( existingProperty.isNoProperty() )
            {
                auxStoreOps.graphAddStoreProperty( property );
            }
            else
            {
                auxStoreOps.graphChangeStoreProperty( existingProperty, property );
            }
            state.txState().graphDoReplaceProperty( existingProperty, property );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }

    @Override
    public Property nodeRemoveProperty( StatementState state, long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = nodeGetProperty( state, nodeId, propertyKeyId );
            if ( !existingProperty.isNoProperty() )
            {
                auxStoreOps.nodeRemoveStoreProperty( nodeId, existingProperty );
            }
            state.txState().nodeDoRemoveProperty( nodeId, existingProperty );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }

    @Override
    public Property relationshipRemoveProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        try
        {
            Property existingProperty = relationshipGetProperty( state, relationshipId, propertyKeyId );
            if ( !existingProperty.isNoProperty() )
            {
                auxStoreOps.relationshipRemoveStoreProperty( relationshipId, existingProperty );
            }
            state.txState().relationshipDoRemoveProperty( relationshipId, existingProperty );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }
    
    @Override
    public Property graphRemoveProperty( StatementState state, long propertyKeyId )
            throws PropertyKeyIdNotFoundException
    {
        try
        {
            Property existingProperty = graphGetProperty( state, propertyKeyId );
            if ( !existingProperty.isNoProperty() )
            {
                auxStoreOps.graphRemoveStoreProperty( existingProperty );
            }
            state.txState().graphDoRemoveProperty( existingProperty );
            return existingProperty;
        }
        catch ( PropertyNotFoundException e )
        {
            throw new IllegalArgumentException( "Property used for setting should not be NoProperty", e );
        }
    }
    
    @Override
    public PrimitiveLongIterator nodeGetPropertyKeys( StatementState state, long nodeId ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            return new PropertyKeyIdIterator( nodeGetAllProperties( state, nodeId ) );
        }
        
        return entityReadDelegate.nodeGetPropertyKeys( state, nodeId );
    }
    
    @Override
    public Property nodeGetProperty( StatementState state, long nodeId, long propertyKeyId )
            throws EntityNotFoundException, PropertyKeyIdNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            Iterator<Property> properties = nodeGetAllProperties( state, nodeId );
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
        
        return entityReadDelegate.nodeGetProperty( state, nodeId, propertyKeyId );
    }
    
    @Override
    public boolean nodeHasProperty( StatementState state, long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        return !nodeGetProperty( state, nodeId, propertyKeyId ).isNoProperty();
    }

    @Override
    public Iterator<Property> nodeGetAllProperties( StatementState state, long nodeId ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().nodeIsAddedInThisTx( nodeId ) )
            {
                return state.txState().nodePropertyDiffSets( nodeId ).getAdded().iterator();
            }
            if ( state.txState().nodeIsDeletedInThisTx( nodeId ) )
            {
                // TODO Throw IllegalStateException to conform with beans API. We may want to introduce
                // EntityDeletedException instead and use it instead of returning empty values in similar places
                throw new IllegalStateException( "Node " + nodeId + " has been deleted" );
            }
            return state.txState().nodePropertyDiffSets( nodeId )
                    .apply( entityReadDelegate.nodeGetAllProperties( state, nodeId ) );
        }

        return entityReadDelegate.nodeGetAllProperties( state, nodeId );
    }
    
    @Override
    public PrimitiveLongIterator relationshipGetPropertyKeys( StatementState state, long relationshipId )
            throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            return new PropertyKeyIdIterator( relationshipGetAllProperties( state, relationshipId ) );
        }
        
        return entityReadDelegate.relationshipGetPropertyKeys( state, relationshipId );
    }
    
    @Override
    public Property relationshipGetProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws EntityNotFoundException, PropertyKeyIdNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            Iterator<Property> properties = relationshipGetAllProperties( state, relationshipId );
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
        return entityReadDelegate.relationshipGetProperty( state, relationshipId, propertyKeyId );
    }
    
    @Override
    public boolean relationshipHasProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        return !relationshipGetProperty( state, relationshipId, propertyKeyId ).isNoProperty();
    }

    @Override
    public Iterator<Property> relationshipGetAllProperties( StatementState state, long relationshipId ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().relationshipIsAddedInThisTx( relationshipId ) )
            {
                return state.txState().relationshipPropertyDiffSets( relationshipId ).getAdded().iterator();
            }
            if ( state.txState().relationshipIsDeletedInThisTx( relationshipId ) )
            {
                // TODO Throw IllegalStateException to conform with beans API. We may want to introduce
                // EntityDeletedException instead and use it instead of returning empty values in similar places
                throw new IllegalStateException( "Relationship " + relationshipId + " has been deleted" );
            }
            return state.txState().relationshipPropertyDiffSets( relationshipId )
                    .apply( entityReadDelegate.relationshipGetAllProperties( state, relationshipId ) );
        }
        else
        {
            return entityReadDelegate.relationshipGetAllProperties( state, relationshipId );
        }
    }
    
    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( StatementState state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return new PropertyKeyIdIterator( graphGetAllProperties( state ) );
        }
        
        return entityReadDelegate.graphGetPropertyKeys( state );
    }
    
    @Override
    public Property graphGetProperty( StatementState state, long propertyKeyId )
    {
        Iterator<Property> properties = graphGetAllProperties( state );
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
    public boolean graphHasProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        return !graphGetProperty( state, propertyKeyId ).isNoProperty();
    }
    
    @Override
    public Iterator<Property> graphGetAllProperties( StatementState state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().graphPropertyDiffSets().apply( entityReadDelegate.graphGetAllProperties( state ) );
        }

        return entityReadDelegate.graphGetAllProperties( state );
    }

    private class HasPropertyFilter implements Predicate<Long>
    {
        private final Object value;
        private final long propertyKeyId;
        private final StatementState state;

        public HasPropertyFilter( StatementState state, long propertyKeyId, Object value )
        {
            this.state = state;
            this.value = value;
            this.propertyKeyId = propertyKeyId;
        }

        @Override
        public boolean accept( Long nodeId )
        {
            try
            {
                if ( state.hasTxStateWithChanges() && state.txState().nodeIsDeletedInThisTx( nodeId ) )
                {
                    return false;
                }
                Property property = nodeGetProperty( state, nodeId, propertyKeyId );
                return !property.isNoProperty() && property.valueEquals( value );
            }
            catch ( EntityNotFoundException | PropertyKeyIdNotFoundException e )
            {
                return false;
            }
        }
    }

    private class HasLabelFilter implements Predicate<Long>
    {
        private final long labelId;
        private final StatementState state;

        public HasLabelFilter( StatementState state, long labelId )
        {
            this.state = state;
            this.labelId = labelId;
        }

        @Override
        public boolean accept( Long nodeId )
        {
            try
            {
                return nodeHasLabel( state, nodeId, labelId );
            }
            catch ( EntityNotFoundException e )
            {
                return false;
            }
        }
    }
    
    // === TODO Below is unnecessary delegate methods

    @Override
    public Long indexGetOwningUniquenessConstraintId( StatementState state, IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( state, index );
    }

    @Override
    public long indexGetCommittedId( StatementState state, IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        return schemaReadDelegate.indexGetCommittedId( state, index );
    }
    
    @Override
    public String indexGetFailure( StatementState state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetFailure( state, descriptor );
    }
}
