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
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Pair;
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
        Iterable<Long> committedLabels = delegate.getLabelsForNode( nodeId );
        if ( !state.hasChanges() )
            return committedLabels;

        Set<Long> result = addToCollection( committedLabels, new HashSet<Long>() );
        result.addAll( state.getAddedLabels( nodeId ) );
        result.removeAll( state.getRemovedLabels( nodeId ) );
        return result;
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
            applyLabelChangesToTransaction();
            applySchemaChangesToTransaction();
        }
        delegate.close( successful );
    }

    private void applyLabelChangesToTransaction()
    {
        for ( TxState.NodeState node : state.getNodes() )
        {
            for ( Long labelId : node.getAddedLabels() )
                delegate.addLabelToNode( labelId, node.getId() );
            for ( Long labelId : node.getRemovedLabels() )
                delegate.removeLabelFromNode( labelId, node.getId() );
        }
    }

    private void applySchemaChangesToTransaction()
    {
        try
        {
            for ( Map.Entry<Long, Collection<Pair<Long,Long>>> entry : state.getAddedIndexRules().entrySet() )
                for ( Pair<Long,Long> indexedProperty : entry.getValue() )
                    delegate.addIndexRule( entry.getKey(), indexedProperty.other() );
        }
        catch ( ConstraintViolationKernelException e )
        {
            throw new TransactionFailureException( "Late unexpected schema exception", e );
        }
    }

    @Override
    public void addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        for ( long existingPropertyKey : getIndexRules( labelId ) )
        {
            if ( propertyKey == existingPropertyKey )
                return;
        }
        
        state.addIndexRule( labelId, propertyKey );
    }
    
    @SuppressWarnings( "unchecked" )
    @Override
    public Iterable<Long> getIndexRules( long labelId )
    {
        Iterable<Long> committedRules = delegate.getIndexRules( labelId ), result = committedRules;
        Collection<Long> addedSchemaRules = state.getAddedIndexRules( labelId );
        if ( !addedSchemaRules.isEmpty() )
            result = concat( committedRules, addedSchemaRules );
        return result;
    }
}
