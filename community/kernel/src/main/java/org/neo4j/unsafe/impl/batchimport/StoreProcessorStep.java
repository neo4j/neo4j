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

import java.util.Collection;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.staging.LonelyProcessingStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

/**
 * Convenient step for processing all in use in a {@link RecordStore}.
 */
public abstract class StoreProcessorStep<RECORD extends AbstractBaseRecord> extends LonelyProcessingStep
        implements StatsProvider
{
    private final RecordStore<RECORD> store;
    private final StoreProcessor<RECORD> processor;
    private final int recordSize;
    private final boolean reversed;
    private long id;
    private long highestId;

    public StoreProcessorStep( StageControl control, String name, int batchSize, int movingAverageSize,
            RecordStore<RECORD> store, StoreProcessor<RECORD> processor, boolean reversed )
    {
        super( control, name, batchSize, movingAverageSize );
        this.store = store;
        this.processor = processor;
        this.reversed = reversed;
        this.recordSize = store.getRecordSize();
    }

    @Override
    protected void process()
    {
        highestId = store.getHighestPossibleIdInUse();
        id = reversed ? highestId : 0;
        long tooFar = reversed ? -1 : highestId+1;
        int stride = reversed ? -1 : 1;
        RECORD heavilyReusedRecord = createReusableRecord();
        for ( ; id != tooFar; id += stride )
        {
            RECORD record = loadRecord( id, heavilyReusedRecord );
            if ( record != null && processor.process( record ) )
            {
                store.updateRecord( heavilyReusedRecord );
            }
            itemProcessed();
        }
        processor.done();
    }

    @Override
    protected void addStatsProviders( Collection<StatsProvider> providers )
    {
        super.addStatsProviders( providers );
        providers.add( this );
    }

    @Override
    public Stat stat( Key key )
    {
        if ( key == Keys.io_throughput )
        {
            long position = reversed ? (highestId-id) : id;
            return new IoThroughputStat( startTime, endTime, recordSize*position );
        }
        return null;
    }

    @Override
    public Key[] keys()
    {
        return new Key[] { Keys.io_throughput };
    }

    protected abstract RECORD loadRecord( long id, RECORD into );

    protected abstract RECORD createReusableRecord();
}
