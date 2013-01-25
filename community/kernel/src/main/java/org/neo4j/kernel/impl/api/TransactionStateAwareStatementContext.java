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

import java.util.Set;

import org.neo4j.helpers.Predicate;
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
    public void addLabelToNode( long labelId, long nodeId )
    {
        Set<Long> addedLabels = state.getAddedLabels( nodeId, true );
        Set<Long> removedLabels = state.getRemovedLabels( nodeId, false );

        if ( removedLabels != null )
            removedLabels.remove( labelId );
        if ( !addedLabels.add( labelId ) )
            return; // Already added
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        if ( state.hasChanges() )
        {
            Set<Long> addedLabels = state.getAddedLabels( nodeId, false );
            if ( addedLabels != null && addedLabels.contains( labelId ) )
                return true;
            Set<Long> removedLabels = state.getRemovedLabels( nodeId, false );
            if ( removedLabels != null && removedLabels.contains( labelId ) )
                return false;
        }

        return super.isLabelSetOnNode( labelId, nodeId );
    }

    @Override
    public Iterable<Long> getLabelsForNode( long nodeId )
    {
        Iterable<Long> committedLabels = super.getLabelsForNode( nodeId );
        if ( !state.hasChanges() )
            return committedLabels;

        Iterable<Long> result = committedLabels;
        Set<Long> addedLabels = state.getAddedLabels( nodeId, false );
        if ( addedLabels != null )
            result = concat( result, addedLabels );
        final Set<Long> removedLabels = state.getRemovedLabels( nodeId, false );
        if ( removedLabels != null )
        {
            result = filter( new Predicate<Long>()
            {
                @Override
                public boolean accept( Long item )
                {
                    return ! removedLabels.contains( item );
                }
            }, result );
        }
        return result;
    }

    @Override
    public void removeLabelFromNode( long labelId, long nodeId )
    {
        Set<Long> addedLabels = state.getAddedLabels( nodeId, false );
        Set<Long> removedLabels = state.getRemovedLabels( nodeId, true );

        if ( addedLabels != null )
            addedLabels.remove( labelId );
        if ( !removedLabels.add( labelId ) )
            return; // Already removed
    }

    @Override
    public void close()
    {
        for ( TxState.NodeState node : state.getNodes() )
        {
            for ( Long labelId : node.getAddedLabels() )
                super.addLabelToNode( labelId, node.getId() );
            for ( Long labelId : node.getRemovedLabels() )
                super.removeLabelFromNode( labelId, node.getId() );
        }
        super.close();
    }
}
