/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.txstate.RichLongSet;
import org.neo4j.util.Preconditions;

final class NodeCursorScan implements Scan<NodeCursor>
{
    private final AllNodeScan allNodeScan;
    private final Read read;
    private final boolean hasChanges;
    private volatile boolean addedNodesConsumed;
    private final RichLongSet addedNodesSet;
    private final int numberOfAddedNodes;
    private final AtomicInteger addedChunk = new AtomicInteger( 0 );

    NodeCursorScan( AllNodeScan internalScan, Read read )
    {
        this.allNodeScan = internalScan;
        this.read = read;
        this.hasChanges = read.hasTxStateWithChanges();
        this.addedNodesSet = read.txState().addedAndRemovedNodes().getAdded().freeze();
        this.numberOfAddedNodes = addedNodesSet.size();
        this.addedNodesConsumed = numberOfAddedNodes == 0;
    }

    @Override
    public boolean reserveBatch( NodeCursor cursor, int sizeHint )
    {
        Preconditions.requirePositive( sizeHint );

        LongIterator addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        if ( hasChanges && !addedNodesConsumed )
        {
            //the idea here is to give each batch an exclusive range of the underlying
            //memory so that each thread can read in parallel without contention.
            int addedStart = addedChunk.getAndAdd( sizeHint );
            if ( addedStart < numberOfAddedNodes )
            {
                int batchSize = Math.min( sizeHint, numberOfAddedNodes - addedStart  );
                sizeHint -= batchSize;
                addedNodes = addedNodesSet.rangeIterator( addedStart, batchSize );
            }
            else
            {
                addedNodesConsumed = true;
            }
        }
        return ((DefaultNodeCursor) cursor).scanBatch( read, allNodeScan, sizeHint, addedNodes, hasChanges );
    }
}
