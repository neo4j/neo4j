/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static java.lang.Long.min;
import static java.lang.Math.toIntExact;
import static org.neo4j.kernel.impl.index.labelscan.LabelScanValue.RANGE_SIZE;

/**
 * {@link LabelScanWriter} for {@link NativeLabelScanStore}, or rather an {@link Writer} for its
 * internal {@link GBPTree}.
 * <p>
 * {@link #write(NodeLabelUpdate) updates} are queued up to a maximum batch size and, for performance,
 * applied in sorted order (by label and node id) when reaches batch size or on {@link #close()}.
 * <p>
 * Updates aren't visible to {@link LabelScanReader readers} immediately, rather when queue happens to be applied.
 * <p>
 * Incoming {@link NodeLabelUpdate updates} are actually modified from representing physical before/after
 * state to represent logical to-add/to-remove state. These changes are done directly inside the provided
 * {@link NodeLabelUpdate#getLabelsAfter()} and {@link NodeLabelUpdate#getLabelsBefore()} arrays,
 * relying on the fact that those arrays are returned in its essential form, instead of copies.
 * This conversion is done like so mostly to reduce garbage.
 *
 * @see PhysicalToLogicalLabelChanges
 */
class NativeLabelScanWriter implements LabelScanWriter
{
    /**
     * {@link Comparator} for sorting the node id ranges, used in batches to apply updates in sorted order.
     */
    private static final Comparator<NodeLabelUpdate> UPDATE_SORTER =
            Comparator.comparingLong( NodeLabelUpdate::getNodeId );

    /**
     * {@link ValueMerger} used for adding label->node mappings, see {@link LabelScanValue#add(LabelScanValue)}.
     */
    private final ValueMerger<LabelScanKey,LabelScanValue> addMerger;

    /**
     * {@link ValueMerger} used for removing label->node mappings, see {@link LabelScanValue#remove(LabelScanValue)}.
     */
    private final ValueMerger<LabelScanKey,LabelScanValue> removeMerger;

    private final WriteMonitor monitor;

    /**
     * {@link Writer} acquired when acquiring this {@link NativeLabelScanWriter},
     * acquired from {@link GBPTree#writer()}.
     */
    private Writer<LabelScanKey,LabelScanValue> writer;

    /**
     * Instance of {@link LabelScanKey} acting as place to read keys into and also to set for each applied update.
     */
    private final LabelScanKey key = new LabelScanKey();

    /**
     * Instance of {@link LabelScanValue} acting as place to read values into and also to update
     * for each applied update.
     */
    private final LabelScanValue value = new LabelScanValue();

    /**
     * Batch currently building up as {@link #write(NodeLabelUpdate) updates} come in. Cursor for where
     * to place new updates is {@link #pendingUpdatesCursor}. Length of this queue is decided in constructor
     * and defines the maximum batch size.
     */
    private final NodeLabelUpdate[] pendingUpdates;

    /**
     * Cursor into {@link #pendingUpdates}, where to place new {@link #write(NodeLabelUpdate) updates}.
     * When full the batch is applied and this cursor reset to {@code 0}.
     */
    private int pendingUpdatesCursor;

    /**
     * There are two levels of batching, one for {@link NodeLabelUpdate updates} and one when applying.
     * This variable helps keeping track of the second level where updates to the actual {@link GBPTree}
     * are batched per node id range, i.e. to add several labelId->nodeId mappings falling into the same
     * range, all of those updates are made into one {@link LabelScanValue} and then issues as one update
     * to the tree. There are additions and removals, this variable keeps track of which.
     */
    private boolean addition;

    /**
     * When applying {@link NodeLabelUpdate updates} (when batch full or in {@link #close()}), updates are
     * applied labelId by labelId. All updates are scanned through multiple times, with one label in mind at a time.
     * For each round the current round tries to figure out which is the closest higher labelId to apply
     * in the next round. This variable keeps track of that next labelId.
     */
    private long lowestLabelId;

    interface WriteMonitor
    {
        default void range( long range, int labelId )
        {
        }

        default void prepareAdd( long txId, int offset )
        {
        }

        default void prepareRemove( long txId, int offset )
        {
        }

        default void mergeAdd( LabelScanValue existingValue, LabelScanValue newValue )
        {
        }

        default void mergeRemove( LabelScanValue existingValue, LabelScanValue newValue )
        {
        }

        default void flushPendingUpdates()
        {
        }

        default void writeSessionEnded()
        {
        }

        default void force()
        {
        }

        default void close()
        {
        }
    }

    static WriteMonitor EMPTY = new WriteMonitor()
    {
    };

    NativeLabelScanWriter( int batchSize, WriteMonitor monitor )
    {
        this.pendingUpdates = new NodeLabelUpdate[batchSize];
        this.addMerger = ( existingKey, newKey, existingValue, newValue ) ->
        {
            monitor.mergeAdd( existingValue, newValue );
            return existingValue.add( newValue );
        };
        this.removeMerger = ( existingKey, newKey, existingValue, newValue ) ->
        {
            monitor.mergeRemove( existingValue, newValue );
            return existingValue.remove( newValue );
        };
        this.monitor = monitor;
    }

    NativeLabelScanWriter initialize( Writer<LabelScanKey,LabelScanValue> writer )
    {
        this.writer = writer;
        this.pendingUpdatesCursor = 0;
        this.addition = false;
        this.lowestLabelId = Long.MAX_VALUE;
        return this;
    }

    /**
     * Queues a {@link NodeLabelUpdate} to this writer for applying when batch gets full,
     * or when {@link #close() closing}.
     */
    @Override
    public void write( NodeLabelUpdate update ) throws IOException
    {
        if ( pendingUpdatesCursor == pendingUpdates.length )
        {
            flushPendingChanges();
        }

        pendingUpdates[pendingUpdatesCursor++] = update;
        PhysicalToLogicalLabelChanges.convertToAdditionsAndRemovals( update );
        checkNextLabelId( update.getLabelsBefore() );
        checkNextLabelId( update.getLabelsAfter() );
    }

    private void checkNextLabelId( long[] labels )
    {
        if ( labels.length > 0 && labels[0] != -1 )
        {
            lowestLabelId = min( lowestLabelId, labels[0] );
        }
    }

    private void flushPendingChanges() throws IOException
    {
        Arrays.sort( pendingUpdates, 0, pendingUpdatesCursor, UPDATE_SORTER );
        monitor.flushPendingUpdates();
        long currentLabelId = lowestLabelId;
        value.clear();
        key.clear();
        while ( currentLabelId != Long.MAX_VALUE )
        {
            long nextLabelId = Long.MAX_VALUE;
            for ( int i = 0; i < pendingUpdatesCursor; i++ )
            {
                NodeLabelUpdate update = pendingUpdates[i];
                long nodeId = update.getNodeId();
                nextLabelId = extractChange( update.getLabelsAfter(), currentLabelId, nodeId, nextLabelId, true, update.getTxId() );
                nextLabelId = extractChange( update.getLabelsBefore(), currentLabelId, nodeId, nextLabelId, false, update.getTxId() );
            }
            currentLabelId = nextLabelId;
        }
        flushPendingRange();
        pendingUpdatesCursor = 0;
    }

    private long extractChange( long[] labels, long currentLabelId, long nodeId, long nextLabelId, boolean addition, long txId )
            throws IOException
    {
        long foundNextLabelId = nextLabelId;
        for ( int li = 0; li < labels.length; li++ )
        {
            long labelId = labels[li];
            if ( labelId == -1 )
            {
                break;
            }

            // Have this check here so that we can pick up the next labelId in our change set
            if ( labelId == currentLabelId )
            {
                change( currentLabelId, nodeId, addition, txId );

                // We can do a little shorter check for next labelId here straight away,
                // we just check the next if it's less than what we currently think is next labelId
                // and then break right after
                if ( li + 1 < labels.length && labels[li + 1] != -1 )
                {
                    long nextLabelCandidate = labels[li + 1];
                    if ( nextLabelCandidate < currentLabelId )
                    {
                        throw new IllegalArgumentException(
                                "The node label update contained unsorted label ids " + Arrays.toString( labels ) );
                    }
                    if ( nextLabelCandidate > currentLabelId )
                    {
                        foundNextLabelId = min( foundNextLabelId, nextLabelCandidate );
                    }
                }
                break;
            }
            else if ( labelId > currentLabelId )
            {
                foundNextLabelId = min( foundNextLabelId, labelId );
            }
        }
        return foundNextLabelId;
    }

    private void change( long currentLabelId, long nodeId, boolean add, long txId ) throws IOException
    {
        int labelId = toIntExact( currentLabelId );
        long idRange = rangeOf( nodeId );
        if ( labelId != key.labelId || idRange != key.idRange || addition != add )
        {
            flushPendingRange();

            // Set key to current and reset value
            key.labelId = labelId;
            key.idRange = idRange;
            addition = add;
            monitor.range( idRange, labelId );
        }

        int offset = toIntExact( nodeId % RANGE_SIZE );
        value.set( offset );
        if ( addition )
        {
            monitor.prepareAdd( txId, offset );
        }
        else
        {
            monitor.prepareRemove( txId, offset );
        }
    }

    private void flushPendingRange() throws IOException
    {
        if ( value.bits != 0 )
        {
            // There are changes in the current range, flush them
            writer.merge( key, value, addition ? addMerger : removeMerger );
            // TODO: after a remove we could check if the tree value is empty and if so remove it from the index
            // hmm, or perhaps that could be a feature of ValueAmender?
            value.clear();
        }
    }

    private static long rangeOf( long nodeId )
    {
        return nodeId / RANGE_SIZE;
    }

    /**
     * Applies {@link #write(NodeLabelUpdate) queued updates} which has not not yet been applied.
     * After this call no more {@link #write(NodeLabelUpdate)} can be applied.
     */
    @Override
    public void close() throws IOException
    {
        try
        {
            flushPendingChanges();
            monitor.writeSessionEnded();
        }
        finally
        {
            writer.close();
        }
    }
}
