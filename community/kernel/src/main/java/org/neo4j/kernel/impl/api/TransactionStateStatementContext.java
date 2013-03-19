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

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.*;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

import java.util.Collection;

import static java.util.Collections.emptyList;
import static org.neo4j.helpers.collection.Iterables.*;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;

public class TransactionStateStatementContext extends CompositeStatementContext
{
    private final TxState state;
    private final StatementContext delegate;
    private final SchemaOperations schemaOperations;

    public TransactionStateStatementContext( StatementContext actual,
                                             SchemaOperations schemaOperations,
                                             TxState state )
    {
        super( actual, schemaOperations );
        this.state = state;
        this.delegate = actual;
        this.schemaOperations = schemaOperations;
    }

    public TransactionStateStatementContext( StatementContext actual, TxState state )
    {
        this( actual, actual, state);
    }

    @Override
    public Object getNodePropertyValue( long nodeId, long propertyId )
            throws PropertyNotFoundException, PropertyKeyIdNotFoundException
    {
        throw new UnsupportedOperationException( "only implemented in StoreStatementContext for now" );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        if ( state.hasChanges() )
        {
            Boolean labelState = state.getLabelState( nodeId, labelId );
            if ( labelState != null )
                return labelState.booleanValue();
        }

        return delegate.isLabelSetOnNode( labelId, nodeId );
    }

    @Override
    public Iterable<Long> getLabelsForNode( long nodeId )
    {
        Iterable<Long> committed = delegate.getLabelsForNode( nodeId );
        return state.getNodeStateLabelDiffSets( nodeId ).apply( committed );
    }

    @Override
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        if(isLabelSetOnNode( labelId, nodeId ))
        {
            // Label is already in state or in store, no-op
            return false;
        }

        state.addLabelToNode( labelId, nodeId );
        return true;
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        if(!isLabelSetOnNode( labelId, nodeId ))
        {
            // Label does not exist in state nor in store, no-op
            return false;
        }

        state.removeLabelFromNode( labelId, nodeId );

        return true;
    }
    
    @SuppressWarnings( "unchecked" )
    @Override
    public Iterable<Long> getNodesWithLabel( long labelId )
    {
        Iterable<Long> committed = delegate.getNodesWithLabel( labelId );
        if ( !state.hasChanges() )
            return committed;
        
        Iterable<Long> result = committed;
        final Collection<Long> removed = state.getNodesWithLabelRemoved( labelId );
        if ( !removed.isEmpty() )
        {
            result = filter( new Predicate<Long>()
            {
                @Override
                public boolean accept( Long item )
                {
                    return !removed.contains( item );
                }
            }, result );
        }
        
        Iterable<Long> added = state.getNodesWithLabelAdded( labelId );
        return concat( result, added );
    }
    
    @Override
    public IndexRule addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        return state.addIndexRule( labelId, propertyKey );
    }

    @Override
    public void dropIndexRule( IndexRule indexRule ) throws ConstraintViolationKernelException
    {
        state.dropIndexRule(indexRule);
    }
    
    @Override
    public IndexRule getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        Iterable<IndexRule> committedRules;
        try
        {
            committedRules = option( schemaOperations.getIndexRule( labelId, propertyKey ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            committedRules = emptyList();
        }
        DiffSets<IndexRule> ruleDiffSet = state.getIndexRuleDiffSetsByLabel( labelId );
        Iterable<IndexRule> rules = ruleDiffSet.apply( committedRules );
        IndexRule single = singleOrNull( rules );
        if ( single == null )
            throw new SchemaRuleNotFoundException( "Index rule for label:" + labelId + " and property:" +
                    propertyKey + " not found" );
        return single;
    }


    @Override
    public InternalIndexState getIndexState( IndexRule indexRule ) throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        DiffSets<IndexRule> diffSet = state.getIndexRuleDiffSetsByLabel( indexRule.getLabel() );
        if( diffSet.isAdded( indexRule) )
        {
            return InternalIndexState.POPULATING;
        }

        if( diffSet.isRemoved( indexRule ))
        {
            throw new IndexNotFoundKernelException( String.format( "Index for label id %d on property id %d has been " +
                    "dropped in this transaction.", indexRule.getLabel(), indexRule.getPropertyKey() ) );
        }

        return delegate.getIndexState( indexRule );
    }

    @Override
    public Iterable<IndexRule> getIndexRules( long labelId )
    {
        return state.getIndexRuleDiffSetsByLabel( labelId ).apply( delegate.getIndexRules( labelId ) );
    }

    @Override
    public Iterable<IndexRule> getIndexRules()
    {
        return state.getIndexRuleDiffSets().apply( delegate.getIndexRules() );
    }

    @Override
    public Iterable<Long> exactIndexLookup( long indexId, final Object value ) throws IndexNotFoundKernelException
    {
        IndexDescriptor idx = delegate.getIndexDescriptor( indexId );

        // Start with nodes where the given property has changed
        DiffSets<Long> diff = state.getNodesWithChangedProperty( idx.getPropertyKeyId(), value );

        // Filter out deleted nodes
        diff.removeAll( state.getDeletedNodes() );

        // Ensure remaining nodes have the correct label
        diff = diff.filterAdded( new HasLabelFilter( idx.getLabelId() ) );

        // Include newly labeled nodes that already had the correct property
        HasPropertyFilter hasPropertyFilter = new HasPropertyFilter( idx.getPropertyKeyId(), value );
        Iterable<Long> addedNodesWithLabel = state.getNodesWithLabelAdded( idx.getLabelId() );
        diff.addAll( Iterables.filter( hasPropertyFilter, addedNodesWithLabel ) );

        // Remove de-labeled nodes that had the correct value before
        Iterable<Long> removedNodesWithLabel = state.getNodesWithLabelRemoved( idx.getLabelId() );
        diff.removeAll( Iterables.filter( hasPropertyFilter, removedNodesWithLabel ) );

        // Apply to actual index lookup
        return diff.apply( delegate.exactIndexLookup( indexId, value ) );
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
            return isLabelSetOnNode( labelId, nodeId );
        }
    }
}
