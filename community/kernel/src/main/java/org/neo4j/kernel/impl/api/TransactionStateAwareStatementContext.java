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
    public void addLabelToNode( long labelId, long nodeId )
    {
        Set<Long> addedLabels = state.getAddedLabels( nodeId, true );
//        Set<Long> removedLabels = state.getRemovedLabels( nodeId );

        if ( !addedLabels.add( labelId ) )
            return; // Already added
//        if ( removedLabels != null )
//            removedLabels.remove( labelId );

        super.addLabelToNode( labelId, nodeId );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        if ( state.hasChanges() )
        {
            Set<Long> addedLabels = state.getAddedLabels( nodeId, false );
            if ( addedLabels != null && addedLabels.contains( labelId ) )
                return true;

//            Set<Long> removedLabels = state.getRemovedLabels( nodeId );
//            if ( removedLabels != null && removedLabels.contains( labelId ) )
//                return false;
        }

        return super.isLabelSetOnNode( labelId, nodeId );
    }
}
