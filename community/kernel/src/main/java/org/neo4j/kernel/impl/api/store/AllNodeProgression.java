/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public class AllNodeProgression implements NodeProgression
{
    private final NodeStore nodeStore;
    private final ReadableTransactionState state;

    private long start;
    private boolean done;

    AllNodeProgression( NodeStore nodeStore, ReadableTransactionState state )
    {
        this.nodeStore = nodeStore;
        this.state = state;
        this.start = nodeStore.getNumberOfReservedLowIds();
    }

    @Override
    public boolean nextBatch( Batch batch )
    {
        while ( true )
        {
            if ( done )
            {
                batch.nothing();
                return false;
            }

            long highId = nodeStore.getHighestPossibleIdInUse();
            if ( start <= highId )
            {
                batch.init( start, highId );
                start = highId + 1;
                return true;
            }

            done = true;
        }
    }

    @Override
    public Iterator<Long> addedNodes()
    {
        return state == null ? null : state.addedAndRemovedNodes().getAdded().iterator();
    }

    @Override
    public boolean fetchFromTxState( long id )
    {
        return false;
    }

    @Override
    public boolean fetchFromDisk( long id )
    {
        return state == null || !state.nodeIsDeletedInThisTx( id );
    }

    @Override
    public NodeState nodeState( long id )
    {
        return state == null ? NodeState.EMPTY : state.getNodeState( id );
    }
}
