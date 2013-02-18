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

import static org.neo4j.helpers.collection.Iterables.concat;
import static org.neo4j.helpers.collection.Iterables.filter;

import java.util.Collection;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.StatementContext;

public class TransactionStateAwareStatementContext extends DelegatingStatementContext
{
    private final TxState state;

    public TransactionStateAwareStatementContext( StatementContext actual, TxState state )
    {
        super( actual );
        this.state = state;
    }

    @Override
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        boolean addedToState = state.addLabelToNode( labelId, nodeId );
        if ( !addedToState || delegate.isLabelSetOnNode( labelId, nodeId ) )
            return false;
        return true;
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
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        boolean removedFromState = state.removeLabelFromNode( labelId, nodeId );
        if ( !removedFromState || !delegate.isLabelSetOnNode( labelId, nodeId ) )
            return false;
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
        final Collection<Long> removed = state.getRemovedNodesWithLabel( labelId );
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
        
        Iterable<Long> added = state.getAddedNodesWithLabel( labelId );
        return concat( result, added );
    }

    @Override
    public void close( boolean successful )
    {
        if ( successful )
        {
            applyLabelAndIndexChangesToTransaction();
            applyNodeChangesToTransaction();
        }
        delegate.close( successful );
    }

    private void applyLabelAndIndexChangesToTransaction()
    {
        try
        {
            for ( TxState.LabelState labelState : state.getLabelStates() )
            {
                long labelId = labelState.getId();
                DiffSets<Long> indexRuleDiffSets = labelState.getIndexRuleDiffSets();
                for ( long propertyKey : indexRuleDiffSets.getAdded() )
                    delegate.addIndexRule( labelId, propertyKey );
                for ( long propertyKey : indexRuleDiffSets.getRemoved() )
                    delegate.dropIndexRule( labelId, propertyKey );
            }
        }
        catch ( ConstraintViolationKernelException e )
        {
            throw new TransactionFailureException( "Late unexpected schema exception", e );
        }
    }

    private void applyNodeChangesToTransaction()
    {
        for ( TxState.NodeState node : state.getNodeStates() )
        {
            DiffSets<Long> labelDiffSets = node.getLabelDiffSets();
            for ( Long labelId : labelDiffSets.getAdded() )
                delegate.addLabelToNode( labelId, node.getId() );
            for ( Long labelId : labelDiffSets.getRemoved() )
                delegate.removeLabelFromNode( labelId, node.getId() );
        }
    }
    
    @Override
    public void addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        ensureNoMatchingIndexRule( labelId, propertyKey );
        state.addIndexRule( labelId, propertyKey );
    }

    @Override
    public void dropIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        ensureSingleMatchingIndexRule( labelId, propertyKey );
        state.removeIndexRule( labelId, propertyKey );
    }

    private void ensureSingleMatchingIndexRule(long labelId, long propertyKey) throws ConstraintViolationKernelException
    {
        int i = countMatchingIndexRules( labelId, propertyKey );
        if (i == 0)
            throw new ConstraintViolationKernelException( "No matching index found (expected one)" );
        if (i > 1)
            throw new ConstraintViolationKernelException( "Too many matching indexes found (expected one)" );
    }

    private void ensureNoMatchingIndexRule(long labelId, long propertyKey) throws ConstraintViolationKernelException
    {
        int i = countMatchingIndexRules( labelId, propertyKey );
        if (i > 0)
            throw new ConstraintViolationKernelException( "Matching indexes found (expected none)" );
    }

    private int countMatchingIndexRules( long labelId, long propertyKey )
    {
        int i = 0;
        for ( long existingPropertyKey : getIndexRules( labelId ) )
        {
            if ( propertyKey == existingPropertyKey )
                i++;
        }
        return i;
    }

    @Override
    public Iterable<Long> getIndexRules( long labelId )
    {
        Iterable<Long> committedRules = delegate.getIndexRules( labelId );
        DiffSets<Long> diffSets = state.getIndexRuleDiffSets( labelId );
        return diffSets.apply( committedRules ); 
    }
}
