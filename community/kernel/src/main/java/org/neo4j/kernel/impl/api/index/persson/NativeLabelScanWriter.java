/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.persson;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.index.SCInserter;
import org.neo4j.index.ValueAmender;
import org.neo4j.index.btree.LabelScanKey;
import org.neo4j.index.btree.LabelScanValue;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;

import static java.lang.Long.min;
import static java.lang.Math.toIntExact;

class NativeLabelScanWriter implements LabelScanWriter
{
    static final Comparator<NodeLabelUpdate> UPDATE_SORTER = new Comparator<NodeLabelUpdate>()
    {
        @Override
        public int compare( NodeLabelUpdate o1, NodeLabelUpdate o2 )
        {
            return Long.compare( o1.getNodeId(), o2.getNodeId() );
        }
    };
    private static final ValueAmender<LabelScanValue> ADD_AMENDER = (value,withValue) -> value.add( withValue );
    private static final ValueAmender<LabelScanValue> REMOVE_AMENDER = (value,withValue) -> value.remove( withValue );

    private final SCInserter<LabelScanKey,LabelScanValue> inserter;
    private final LabelScanKey key = new LabelScanKey();
    private final LabelScanValue value = new LabelScanValue();
    private boolean addition;
    private final NodeLabelUpdate[] pendingUpdates;
    private int pendingUpdatesCursor;
    private long lowestLabelId = Long.MAX_VALUE;
    private final int rangeSize;

    NativeLabelScanWriter( SCInserter<LabelScanKey,LabelScanValue> inserter, int rangeSize, int batchSize )
    {
        this.inserter = inserter;
        this.rangeSize = rangeSize;
        this.pendingUpdates = new NodeLabelUpdate[batchSize];
    }

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

        long currentLabelId = lowestLabelId;
        value.reset();
        key.set( -1, -1 );
        while ( currentLabelId != Long.MAX_VALUE )
        {
            long nextLabelId = Long.MAX_VALUE;
            for ( int i = 0; i < pendingUpdatesCursor; i++ )
            {
                NodeLabelUpdate update = pendingUpdates[i];
                long nodeId = update.getNodeId();
                nextLabelId = extractChange( update.getLabelsAfter(), currentLabelId, nodeId, nextLabelId, true );
                nextLabelId = extractChange( update.getLabelsBefore(), currentLabelId, nodeId, nextLabelId, false );
            }
            currentLabelId = nextLabelId;
        }
        flushPendingRange();
        pendingUpdatesCursor = 0;
    }

    private long extractChange( long[] labels, long currentLabelId, long nodeId, long nextLabelId, boolean addition )
            throws IOException
    {
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
                change( currentLabelId, nodeId, addition );

                // We can do a little shorter check for next labelId here straight away,
                // we just check the next if it's less than what we currently think is next labelId
                // and then break right after
                if ( li+1 < labels.length && labels[li+1] != -1 )
                {
                    nextLabelId = min( nextLabelId, labels[li+1] );
                }
                break;
            }
            else if ( labelId > currentLabelId )
            {
                nextLabelId = min( nextLabelId, labelId );
            }
        }
        return nextLabelId;
    }

    private void change( long currentLabelId, long nodeId, boolean add ) throws IOException
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
        }

        value.set( toIntExact( nodeId % rangeSize ) );
    }

    private void flushPendingRange() throws IOException
    {
        if ( value.bits != 0 )
        {
            // There are changes in the current range, flush them
            inserter.insert( key, value, addition ? ADD_AMENDER : REMOVE_AMENDER );
            // TODO: after a remove we could check if the tree value is empty and if so remove it from the index
            // hmm, or perhaps that could be a feature of ValueAmender?
            value.reset();
        }
    }

    private long rangeOf( long nodeId )
    {
        return nodeId / rangeSize;
    }

    @Override
    public void close() throws IOException
    {
        flushPendingChanges();
        inserter.close();
    }
}
