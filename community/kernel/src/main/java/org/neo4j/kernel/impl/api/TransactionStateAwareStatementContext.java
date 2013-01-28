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

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;

import java.util.HashSet;
import java.util.Set;

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
        Set<Long> addedLabels = state.getAddedLabels( nodeId, true );
        Set<Long> removedLabels = state.getRemovedLabels( nodeId, false );

        removedLabels.remove( labelId );
        return addedLabels.add( labelId );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        if ( state.hasChanges() )
        {
            Set<Long> addedLabels = state.getAddedLabels( nodeId, false );
            if ( addedLabels.contains( labelId ) )
                return true;
            Set<Long> removedLabels = state.getRemovedLabels( nodeId, false );
            if ( removedLabels.contains( labelId ) )
                return false;
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
        result.addAll( state.getAddedLabels( nodeId, false ) );
        result.removeAll( state.getRemovedLabels( nodeId, false ) );
        return result;
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        Set<Long> addedLabels = state.getAddedLabels( nodeId, false );
        Set<Long> removedLabels = state.getRemovedLabels( nodeId, true );

        addedLabels.remove( labelId );
        return removedLabels.add( labelId );
    }

    @Override
    public void close( boolean successful )
    {
        if ( successful )
        {
            for ( TxState.NodeState node : state.getNodes() )
            {
                for ( Long labelId : node.getAddedLabels() )
                    delegate.addLabelToNode( labelId, node.getId() );
                for ( Long labelId : node.getRemovedLabels() )
                    delegate.removeLabelFromNode( labelId, node.getId() );
            }
        }
        delegate.close( successful );
    }
}
