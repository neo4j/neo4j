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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.store.NodeLabelsField.get;

public class LabelIndexWriterStep extends ProcessorStep<NodeRecord[]>
{
    private final LabelScanWriter writer;
    private final NodeStore nodeStore;

    public LabelIndexWriterStep( StageControl control, Configuration config, LabelScanStore store,
            NodeStore nodeStore )
    {
        super( control, "LABEL INDEX", config, 1 );
        this.writer = store.newWriter();
        this.nodeStore = nodeStore;
    }

    @Override
    protected void process( NodeRecord[] batch, BatchSender sender ) throws Throwable
    {
        for ( NodeRecord node : batch )
        {
            if ( node.inUse() )
            {
                writer.write( labelChanges( node.getId(), EMPTY_LONG_ARRAY, get( node, nodeStore ) ) );
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
