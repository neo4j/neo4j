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

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.staging.IoProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.Math.min;

/**
 * Reads from {@link RelationshipStore} backwards and produces batches of {@link RelationshipRecord} for others
 * to process.
 */
public class ReadRelationshipRecordsBackwardsStep extends IoProducerStep<NodeRecord[]>
{
    private final RelationshipStore store;
    private final long highId;
    private long id;

    public ReadRelationshipRecordsBackwardsStep( StageControl control, int batchSize, int movingAverageSize,
            RelationshipStore store )
    {
        super( control, batchSize, movingAverageSize );
        this.store = store;
        this.highId = this.id = store.getHighId();
    }

    @Override
    protected Object nextBatchOrNull( int batchSize )
    {
        int size = (int) min( batchSize, id );
        RelationshipRecord[] batch = new RelationshipRecord[size];
        RelationshipRecord next = new RelationshipRecord( -1 );
        for ( int i = 0; i < size; i++ )
        {
            // null record is fine since our processor, i.e. consumer of these batches, will filter those out.
            if ( store.fillRecord( --id, next, RecordLoad.CHECK ) )
            {
                batch[i] = next;
                next = new RelationshipRecord( -1 );
            }
        }
        return size > 0 ? batch : null;
    }

    @Override
    protected long position()
    {
        return (highId-id) * RelationshipStore.RECORD_SIZE;
    }
}
