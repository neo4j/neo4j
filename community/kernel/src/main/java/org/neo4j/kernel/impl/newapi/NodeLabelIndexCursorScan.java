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

import org.neo4j.collection.RangeLongIterator;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.labelscan.LabelScan;
import org.neo4j.storageengine.api.txstate.LongDiffSets;

import static java.lang.Math.min;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;
import static org.neo4j.util.Preconditions.requirePositive;

class NodeLabelIndexCursorScan implements Scan<NodeLabelIndexCursor>
{
    private final AtomicInteger nextTxState;
    private final Read read;
    private final long[] addedNodesArray;
    private final LongSet removed;
    private volatile boolean addedNodesConsumed;
    private final boolean hasChanges;
    private final LabelScan labelScan;

    NodeLabelIndexCursorScan( Read read, int label, LabelScan labelScan )
    {
        this.read = read;
        this.nextTxState = new AtomicInteger( 0 );
        this.hasChanges = read.hasTxStateWithChanges();
        if ( hasChanges )
        {
            final LongDiffSets changes = read.txState().nodesWithLabelChanged( label );
            this.addedNodesArray = changes.getAdded().toArray();
            this.removed = mergeToSet( read.txState().addedAndRemovedNodes().getRemoved(), changes.getRemoved() );
        }
        else
        {
            this.addedNodesArray = EMPTY_LONG_ARRAY;
            this.removed = LongSets.immutable.empty();
        }
        this.addedNodesConsumed = addedNodesArray.length == 0;
        this.labelScan = labelScan;
    }

    @Override
    public boolean reserveBatch( NodeLabelIndexCursor cursor, int sizeHint )
    {
        requirePositive( sizeHint );

        LongIterator addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        if ( hasChanges && !addedNodesConsumed )
        {
            int start = nextTxState.getAndAdd( sizeHint );
            if ( start < addedNodesArray.length )
            {
                int batchSize = min( sizeHint, addedNodesArray.length - start  );
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
        IndexProgressor indexProgressor = labelScan.initializeBatch( indexCursor, sizeHint );

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
}
