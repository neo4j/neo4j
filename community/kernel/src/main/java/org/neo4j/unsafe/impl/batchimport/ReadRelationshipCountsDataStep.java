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

import java.util.Arrays;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.staging.IoProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Reads from {@link RelationshipStore} and produces batches of startNode,type,endNode values for
 * others to process. The result is one long[] with all values in.
 */
public class ReadRelationshipCountsDataStep extends IoProducerStep
{
    private final RelationshipStore store;
    private final RelationshipRecord record = new RelationshipRecord( -1 );
    private final long highestId;
    private long id = -1;

    public ReadRelationshipCountsDataStep( StageControl control, Configuration config,
            RelationshipStore store )
    {
        super( control, config );
        this.store = store;
        this.highestId = store.getHighestPossibleIdInUse();
    }

    @Override
    protected long[] nextBatchOrNull( long ticket, int batchSize )
    {
        if ( id >= highestId )
        {
            return null;
        }

        long[] batch = new long[batchSize*3]; // start node, type, end node = 3
        int i = 0;
        for ( ; i < batchSize && ++id <= highestId; )
        {
            if ( store.fillRecord( id, record, RecordLoad.CHECK ) )
            {
                int index = i++ * 3;
                batch[index++] = record.getFirstNode();
                batch[index++] = record.getType();
                batch[index++] = record.getSecondNode();
            }
        }
        return i == batchSize ? batch : Arrays.copyOf( batch, i*3 );
    }

    @Override
    protected long position()
    {
        return doneBatches.get()*batchSize*RelationshipStore.RECORD_SIZE;
    }
}
