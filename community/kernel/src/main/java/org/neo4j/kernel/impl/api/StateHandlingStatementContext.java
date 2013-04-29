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
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.PropertyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.state.TxState;

import static java.util.Collections.emptyList;
import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;

public class StateHandlingStatementContext extends CompositeStatementContext
{
    private final TxState state;
    private final StatementContext delegate;

    public StateHandlingStatementContext( StatementContext actual,
                                          SchemaStateOperations schemaOperations,
                                          TxState state )
    {
        // TODO: I'm not sure schema state operations should go here.. as far as I can tell, it isn't transactional,
        // and so having it here along with transactional state makes little sense to me. Reconsider and refactor.
        super( actual, schemaOperations );
        this.state = state;
        this.delegate = actual;
    }

    @Override
    public void deleteNode( long nodeId )
    {
        state.deleteNode( nodeId );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId ) throws EntityNotFoundException
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

        return delegate.isLabelSetOnNode( labelId, nodeId );
    }

    @Override
    public Iterator<Long> getLabelsForNode( long nodeId ) throws EntityNotFoundException
    {
        if ( state.nodeIsDeletedInThisTx( nodeId ) )
        {
            return IteratorUtil.emptyIterator();
        }

        if ( state.nodeIsAddedInThisTx( nodeId ) )
        {
            return state.getNodeStateLabelDiffSets( nodeId ).getAdded().iterator();
        }

        Iterator<Long> committed = delegate.getLabelsForNode( nodeId );
        return state.getNodeStateLabelDiffSets( nodeId ).apply( committed );
    }

    @Override
    public boolean addLabelToNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        if ( isLabelSetOnNode( labelId, nodeId ) )
        {
            // Label is already in state or in store, no-op
            return false;
        }

        state.addLabelToNode( labelId, nodeId );
        return true;
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        if ( !isLabelSetOnNode( labelId, nodeId ) )
        {
            // Label does not exist in state nor in store, no-op
            return false;
        }

        state.removeLabelFromNode( labelId, nodeId );

        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Long> getNodesWithLabel( long labelId )
    {
        Iterator<Long> committed = delegate.getNodesWithLabel( labelId );
        if ( !state.hasChanges() )
        {
            return committed;
        }

        return state.getDeletedNodes().apply( state.getNodesWithLabelChanged( labelId ).apply( committed ) );
    }

    @Override
    public IndexDescriptor addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.addIndexRule( rule );
        return rule;
    }

    @Override
    public void dropIndexRule( IndexDescriptor indexRule ) throws ConstraintViolationKernelException
    {
        state.dropIndexRule( indexRule );
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKeyId )
    {
        UniquenessConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );
        if (!state.unRemoveConstraint(constraint)) {
        for ( Iterator<UniquenessConstraint> it = delegate.getConstraints( labelId, propertyKeyId ); it.hasNext(); )
        {
            if ( it.next().equals( labelId, propertyKeyId ) )
            {
                return constraint;
            }
        }
        state.addConstraint( constraint );
        }
        return constraint;
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId )
    {
        Iterator<UniquenessConstraint> constraints = delegate.getConstraints( labelId, propertyKeyId );
        DiffSets<UniquenessConstraint> diff = state.constraintsForLabel( labelId );
        if ( diff != null )
        {
            return diff.apply( constraints );
        }
        return constraints;
    }

    @Override
    public void dropConstraint( UniquenessConstraint constraint )
    {
        state.dropConstraint( constraint );
    }

    @Override
    public IndexDescriptor getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        Iterable<IndexDescriptor> committedRules;
        try
        {
            committedRules = option( delegate.getIndexRule( labelId, propertyKey ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            committedRules = emptyList();
        }
        DiffSets<IndexDescriptor> ruleDiffSet = state.getIndexRuleDiffSetsByLabel( labelId );
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
    public InternalIndexState getIndexState( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        DiffSets<IndexDescriptor> diffSet = state.getIndexRuleDiffSetsByLabel( indexRule.getLabelId() );
        if ( diffSet.isAdded( indexRule ) )
        {
            return InternalIndexState.POPULATING;
        }

        if ( diffSet.isRemoved( indexRule ) )
        {
            throw new IndexNotFoundKernelException( String.format( "Index for label id %d on property id %d has been " +
                    "dropped in this transaction.", indexRule.getLabelId(), indexRule.getPropertyKeyId() ) );
        }

        return delegate.getIndexState( indexRule );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexRules( long labelId )
    {
        return state.getIndexRuleDiffSetsByLabel( labelId ).apply( delegate.getIndexRules( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexRules()
    {
        return state.getIndexRuleDiffSets().apply( delegate.getIndexRules() );
    }

    @Override
    public Iterator<Long> exactIndexLookup( IndexDescriptor index, final Object value ) throws IndexNotFoundKernelException
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
        return state.getDeletedNodes().apply( diff.apply( delegate.exactIndexLookup( index, value ) ) );
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
                return value.equals( delegate.getNodePropertyValue( nodeId, propertyKeyId ) );
            }
            catch ( PropertyNotFoundException e )
            {
                return false;
            }
            catch ( EntityNotFoundException e )
            {
                return false;
            }
            catch ( PropertyKeyIdNotFoundException e )
            {
                throw new ThisShouldNotHappenError( "Stefan/Jake", "propertyKeyId became invalid during indexQuery" );
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
                return isLabelSetOnNode( labelId, nodeId );
            }
            catch ( EntityNotFoundException e )
            {
                return false;
            }
        }
    }
}
