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

import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Scans {@link RelationshipGroupRecord group records} and discovers which should affect the owners.
 */
public class NodeSetFirstGroupStep extends ProcessorStep<RelationshipGroupRecord[]>
{
    private final int batchSize;
    private final ByteArray cache;
    private final RecordCursor<NodeRecord> nodeRecordCursor;

    private NodeRecord[] current;
    private int cursor;

    public NodeSetFirstGroupStep( StageControl control, Configuration config,
            RecordStore<NodeRecord> nodeStore, ByteArray cache )
    {
        super( control, "FIRST", config, 1 );
        this.cache = cache;
        this.batchSize = config.batchSize();
        this.nodeRecordCursor = nodeStore.newRecordCursor( nodeStore.newRecord() );
        newBatch();
    }

    @Override
    public void start( int orderingGuarantees )
    {
        nodeRecordCursor.acquire( 0, NORMAL );
        super.start( orderingGuarantees );
    }

    private void newBatch()
    {
        current = new NodeRecord[batchSize];
        cursor = 0;
    }

    @Override
    protected void process( RelationshipGroupRecord[] batch, BatchSender sender )
    {
        for ( RelationshipGroupRecord group : batch )
        {
            if ( !group.inUse() )
            {
                continue;
            }

            long nodeId = group.getOwningNode();
            if ( cache.getByte( nodeId, 0 ) == 0 )
            {
                cache.setByte( nodeId, 0, (byte) 1 );
                nodeRecordCursor.next( nodeId );
                NodeRecord node = nodeRecordCursor.get().clone();
                node.setNextRel( group.getId() );
                node.setDense( true );

                current[cursor++] = node;
                if ( cursor == batchSize )
                {
                    sender.send( current );
                    newBatch();
                }
            }
        }
        control.recycle( batch );
    }

    @Override
    protected void lastCallForEmittingOutstandingBatches( BatchSender sender )
    {
        if ( cursor > 0 )
        {
            sender.send( current );
        }
    }

    @Override
    public void close() throws Exception
    {
        nodeRecordCursor.close();
        super.close();
    }
}
