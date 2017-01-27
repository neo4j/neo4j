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
package org.neo4j.unsafe.impl.batchimport;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.LonelyProcessingStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

public class DeleteDuplicateNodesStep extends LonelyProcessingStep
{
    private final NodeStore nodeStore;
    private final PrimitiveLongIterator nodeIds;

    public DeleteDuplicateNodesStep( StageControl control, Configuration config, PrimitiveLongIterator nodeIds,
            NodeStore nodeStore )
    {
        super( control, "DEDUP", config );
        this.nodeStore = nodeStore;
        this.nodeIds = nodeIds;
    }

    @Override
    protected void process() throws IOException
    {
        NodeRecord record = nodeStore.newRecord();
        while ( nodeIds.hasNext() )
        {
            long duplicateNodeId = nodeIds.next();
            record.setId( duplicateNodeId );
            nodeStore.updateRecord( record );
        }
    }
}
