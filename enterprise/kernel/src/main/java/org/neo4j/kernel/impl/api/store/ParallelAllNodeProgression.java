/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static java.lang.Math.max;

class ParallelAllNodeProgression implements NodeProgression
{
    private final NodeStore nodeStore;
    private final ReadableTransactionState state;
    private final int recordsPerPage;
    private final int numberOfReservedLowIds;
    private final long lastPageId;

    private final AtomicLong nextPageId = new AtomicLong();
    private final AtomicBoolean done = new AtomicBoolean();
    private final AtomicBoolean append = new AtomicBoolean( true );

    ParallelAllNodeProgression( NodeStore nodeStore, ReadableTransactionState state )
    {
        this.nodeStore = nodeStore;
        this.state = state;
        recordsPerPage = nodeStore.getRecordsPerPage();
        numberOfReservedLowIds = nodeStore.getNumberOfReservedLowIds();
        // last page to process is the one containing the highest id in use
        lastPageId = nodeStore.getHighestPossibleIdInUse() / recordsPerPage;
        // start from the page containing the first non reserved id
        nextPageId.set( numberOfReservedLowIds / recordsPerPage );
    }

    @Override
    public boolean nextBatch( Batch batch )
    {
        while ( true )
        {
            if ( done.get() )
            {
                batch.nothing();
                return false;
            }

            long pageId = nextPageId.getAndIncrement();
            if ( pageId < lastPageId )
            {
                long first = firstIdOnPage( pageId );
                long last = firstIdOnPage( pageId + 1 ) - 1;
                batch.init( first, last );
                return true;
            }
            else if ( !done.get() && done.compareAndSet( false, true ) )
            {
                long first = firstIdOnPage( lastPageId );
                long last = nodeStore.getHighestPossibleIdInUse();
                batch.init( first, last );
                return true;
            }
        }
    }

    private long firstIdOnPage( long pageId )
    {
        return max( numberOfReservedLowIds, pageId * recordsPerPage );
    }

    @Override
    public Iterator<Long> addedNodes()
    {
        if ( state != null && append.get() && append.compareAndSet( true, false  ) )
        {
            return state.addedAndRemovedNodes().getAdded().iterator();
        }
        return null;
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
