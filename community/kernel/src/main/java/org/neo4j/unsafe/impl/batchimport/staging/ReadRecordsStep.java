/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Reads records from a {@link RecordStore} and sends batches of those records downstream.
 * A {@link RecordCursor} is used during the life cycle of this {@link Step}, e.g. between
 * {@link #start(int)} and {@link #close()}.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
public class ReadRecordsStep<RECORD extends AbstractBaseRecord> extends IoProducerStep
{
    protected final RecordStore<RECORD> store;
    protected final RECORD record;
    protected final RecordCursor<RECORD> cursor;
    protected final long highId;
    private final PrimitiveLongIterator ids;
    private final Class<RECORD> klass;
    private final int recordSize;
    // volatile since written by processing threads and read by execution monitor
    private volatile long count;

    @SuppressWarnings( "unchecked" )
    public ReadRecordsStep( StageControl control, Configuration config, RecordStore<RECORD> store,
            PrimitiveLongIterator ids )
    {
        super( control, config );
        this.store = store;
        this.ids = ids;
        this.klass = (Class<RECORD>) store.newRecord().getClass();
        this.recordSize = store.getRecordSize();
        this.cursor = store.newRecordCursor( record = store.newRecord() );
        this.highId = store.getHighId();
    }

    @Override
    public void start( int orderingGuarantees )
    {
        cursor.acquire( 0, CHECK );
        super.start( orderingGuarantees );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Object nextBatchOrNull( long ticket, int batchSize )
    {
        if ( !ids.hasNext() )
        {
            return null;
        }

        RECORD[] batch = (RECORD[]) Array.newInstance( klass, batchSize );
        int i = 0;
        while ( i < batchSize && ids.hasNext() )
        {
            if ( cursor.next( ids.next() ) && !IdValidator.isReservedId( record.getId() ) )
            {
                RECORD newRecord = (RECORD) record.clone();
                batch[i] = newRecord;
                i++;
            }
        }

        count += i;
        batch = i == batchSize ? batch : Arrays.copyOf( batch, i );
        return batch.length > 0 ? batch : null;
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        cursor.close();
    }

    @Override
    protected long position()
    {
        return count * recordSize;
    }
}
