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

import java.util.Collection;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.PrepareIdSequence;

/**
 * Updates a batch of records to a store.
 */
public class UpdateRecordsStep<RECORD extends AbstractBaseRecord>
        extends ProcessorStep<RECORD[]>
        implements StatsProvider
{
    protected final RecordStore<RECORD> store;
    private final int recordSize;
    private final PrepareIdSequence prepareIdSequence;
    private final LongAdder recordsUpdated = new LongAdder();

    public UpdateRecordsStep( StageControl control, Configuration config, RecordStore<RECORD> store,
            PrepareIdSequence prepareIdSequence )
    {
        super( control, "v", config, config.parallelRecordWrites() ? 0 : 1 );
        this.store = store;
        this.prepareIdSequence = prepareIdSequence;
        this.recordSize = store.getRecordSize();
    }

    @Override
    protected void process( RECORD[] batch, BatchSender sender )
    {
        LongFunction<IdSequence> idSequence = prepareIdSequence.apply( store );
        int recordsUpdatedInThisBatch = 0;
        for ( RECORD record : batch )
        {
            if ( record != null && record.inUse() && !IdValidator.isReservedId( record.getId() ) )
            {
                store.prepareForCommit( record, idSequence.apply( record.getId() ) );
                store.updateRecord( record );
                recordsUpdatedInThisBatch++;
            }
        }
        recordsUpdated.add( recordsUpdatedInThisBatch );
    }

    @Override
    protected void collectStatsProviders( Collection<StatsProvider> into )
    {
        super.collectStatsProviders( into );
        into.add( this );
    }

    @Override
    public Stat stat( Key key )
    {
        return key == Keys.io_throughput ? new IoThroughputStat( startTime, endTime, recordSize * recordsUpdated.sum() ) : null;
    }

    @Override
    public Key[] keys()
    {
        return new Keys[] {Keys.io_throughput};
    }
}
