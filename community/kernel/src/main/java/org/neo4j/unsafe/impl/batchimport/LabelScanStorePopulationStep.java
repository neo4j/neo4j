/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

/**
 * Populates a {@link LabelScanWriter} with all node labels from {@link Batch batches} passing by.
 */
public class LabelScanStorePopulationStep extends ProcessorStep<Batch<InputNode,NodeRecord>>
{
    private final LabelScanWriter writer;

    public LabelScanStorePopulationStep( StageControl control, Configuration config, LabelScanStore labelScanStore )
    {
        super( control, "LABEL SCAN", config, 1 );
        this.writer = labelScanStore.newWriter();
    }

    @Override
    protected void process( Batch<InputNode,NodeRecord> batch, BatchSender sender ) throws Throwable
    {
        int length = batch.labels.length;
        for ( int i = 0; i < length; i++ )
        {
            long[] labels = batch.labels[i];
            NodeRecord node = batch.records[i];
            if ( labels != null && node.inUse() )
            {
                writer.write( labelChanges( node.getId(), EMPTY_LONG_ARRAY, labels ) );
            }
        }
        sender.send( batch );
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        writer.close();
    }
}
