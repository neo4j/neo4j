/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.staging.IoProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.Math.min;

/**
 * Reads from {@link NodeStore} and produces batches of {@link NodeRecord} for others to process.
 *
 * Future: Would be quite efficient just get a page cursor and read inUse+labelField and store
 * all labelField values of a batch in one long[] or similar, instead of passing on a NodeRecord[].
 */
public class ReadNodeCountsDataStep extends IoProducerStep<NodeRecord[]>
{
    private final NodeStore nodeStore;
    private final long highId;
    private long id;

    public ReadNodeCountsDataStep( StageControl control, int batchSize, int movingAverageSize, NodeStore nodeStore )
    {
        super( control, batchSize, movingAverageSize );
        this.nodeStore = nodeStore;
        this.highId = nodeStore.getHighId();
    }

    @Override
    protected Object nextBatchOrNull( int batchSize )
    {
        int size = (int) min( batchSize, highId-id );
        NodeRecord[] batch = new NodeRecord[size];
        for ( int i = 0; i < size; i++ )
        {
            // Passing in null creates a new record if it's in use. Returning null is fine since our processor,
            // i.e. consumer of these batches, will filter those out.
            batch[i] = nodeStore.loadRecord( id++, null );
        }
        return size > 0 ? batch : null;
    }

    @Override
    protected long position()
    {
        return id * NodeStore.RECORD_SIZE;
    }
}
