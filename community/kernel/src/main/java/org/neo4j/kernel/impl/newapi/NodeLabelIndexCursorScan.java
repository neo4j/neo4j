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
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.RangeLongIterator;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.labelscan.LabelScan;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.util.Preconditions;

import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;

class NodeLabelIndexCursorScan implements Scan<NodeLabelIndexCursor>
{
    private final AtomicInteger nextTxState;
    private final long upperBound;
    private static final int CHUNK_SIZE = Long.SIZE;
    private final Read read;
    private final long[] addedNodesArray;
    private final LongSet removed;
    private volatile boolean addedNodesConsumed;
    private final LabelScan labelScan;

    NodeLabelIndexCursorScan( Read read, int label, long highestNodeId, LabelScan labelScan )
    {
        this.read = read;
        this.nextTxState = new AtomicInteger( 0 );
        this.upperBound = roundUp( highestNodeId );
        if ( read.hasTxStateWithChanges() )
        {
            final LongDiffSets changes = read.txState().nodesWithLabelChanged( label );
            this.addedNodesArray = changes.getAdded().toArray();
            this.removed = mergeToSet( read.txState().addedAndRemovedNodes().getRemoved(), changes.getRemoved() );
        }
        else
        {
            this.addedNodesArray = PrimitiveLongCollections.EMPTY_LONG_ARRAY;
            this.removed = LongSets.immutable.empty();
        }
        this.addedNodesConsumed = addedNodesArray.length == 0;
        this.labelScan = labelScan;
    }

    @Override
    public boolean reserveBatch( NodeLabelIndexCursor cursor, int sizeHint )
    {
        Preconditions.requirePositive( sizeHint );

        LongIterator addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        if ( read.hasTxStateWithChanges() && !addedNodesConsumed )
        {
            int start = nextTxState.getAndAdd( sizeHint );
            if ( start < addedNodesArray.length )
            {
                int batchSize = Math.min( sizeHint, addedNodesArray.length - start  );
                sizeHint -= batchSize;
                addedNodes = new RangeLongIterator( addedNodesArray, start, batchSize );
            }
            else
            {
                addedNodesConsumed = true;
            }
        }

        return reserveStoreBatch( cursor, sizeHint, addedNodes );
    }

    private boolean reserveStoreBatch( NodeLabelIndexCursor cursor, int sizeHint, LongIterator addedNodes )
    {
        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead( read );
        IndexProgressor indexProgressor = labelScan.initializeBatch( indexCursor, sizeHint, upperBound );

        if ( indexProgressor == IndexProgressor.EMPTY && !addedNodes.hasNext() )
        {
            return false;
        }
        else
        {
            indexCursor.scan( indexProgressor, addedNodes, removed );
            return true;
        }
    }

    private long roundUp( long sizeHint )
    {
        return (sizeHint / CHUNK_SIZE + 1) * CHUNK_SIZE;
    }
}
