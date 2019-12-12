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
package org.neo4j.internal.index.label;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.storageengine.api.NodeLabelUpdate;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.index.label.NativeLabelScanWriter.offsetOf;
import static org.neo4j.internal.index.label.NativeLabelScanWriter.rangeOf;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * Writer that takes a more batching approach to adding data to the label scan store. It is optimized for writing large amounts of node-id-sequential updates,
 * especially when there are lots of labels involved. It works by having an array of ranges, slot index is labelId. Updates that comes in
 * will find the slot by labelId and add the correct bit the the current range, or if the bit is in another range, merge the current one first.
 * It cannot handle updates to nodes that are already in ths label index, such operations will fail before trying to make those changes.
 */
class BulkAppendNativeLabelScanWriter implements LabelScanWriter
{
    private final Writer<LabelScanKey,LabelScanValue> writer;
    private final ValueMerger<LabelScanKey,LabelScanValue> merger;
    private MutableIntObjectMap<Pair<LabelScanKey,LabelScanValue>> ranges = IntObjectMaps.mutable.empty();

    BulkAppendNativeLabelScanWriter( Writer<LabelScanKey,LabelScanValue> writer )
    {
        this.writer = writer;
        this.merger = new AddMerger( NativeLabelScanWriter.EMPTY );
    }

    @Override
    public void write( NodeLabelUpdate update )
    {
        checkArgument( update.getLabelsBefore().length == 0, "Was expecting no labels before, was %s", Arrays.toString( update.getLabelsBefore() ) );
        long idRange = rangeOf( update.getNodeId() );
        int previousLabelId = -1;
        for ( long labelId : update.getLabelsAfter() )
        {
            int intLabelId = toIntExact( labelId );
            checkArgument( intLabelId > previousLabelId, "Detected unsorted labels in %s", update );
            previousLabelId = intLabelId;
            Pair<LabelScanKey,LabelScanValue> range =
                    ranges.getIfAbsentPutWithKey( intLabelId, id -> Pair.of( new LabelScanKey( id, idRange ), new LabelScanValue() ) );
            if ( range.getKey().idRange != idRange )
            {
                if ( range.getKey().idRange != -1 )
                {
                    writer.merge( range.getKey(), range.getValue(), merger );
                }
                range.getKey().idRange = idRange;
                range.getValue().clear();
            }
            range.getValue().set( offsetOf( update.getNodeId() ) );
        }
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            for ( Pair<LabelScanKey,LabelScanValue> range : ranges )
            {
                if ( range != null )
                {
                    writer.merge( range.getKey(), range.getValue(), merger );
                }
            }
        }
        finally
        {
            writer.close();
        }
    }
}
