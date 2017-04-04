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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

/**
 * Reads records from a {@link RecordStore} and sends batches of those records downstream.
 * A {@link RecordCursor} is used during the life cycle of this {@link Step}, e.g. between
 * {@link #start(int)} and {@link #close()}.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
public class ReadRecordsStep<RECORD extends AbstractBaseRecord> extends ProcessorStep<PrimitiveLongIterator>
{
    protected final RecordStore<RECORD> store;
    private final Class<RECORD> klass;
    protected final int batchSize;

    @SuppressWarnings( "unchecked" )
    public ReadRecordsStep( StageControl control, Configuration config, RecordStore<RECORD> store )
    {
        super( control, ">", config, 0 );
        this.store = store;
        this.klass = (Class<RECORD>) store.newRecord().getClass();
        this.batchSize = config.batchSize();
    }

    @Override
    public void start( int orderingGuarantees )
    {
        super.start( orderingGuarantees | ORDER_SEND_DOWNSTREAM );
    }

    @Override
    protected void process( PrimitiveLongIterator idRange, BatchSender sender ) throws Throwable
    {
        if ( !idRange.hasNext() )
        {
            return;
        }

        long id = idRange.next();
        RECORD record = store.newRecord();
        RecordCursor<RECORD> cursor = store.newRecordCursor( record ).acquire( id, RecordLoad.CHECK );
        RECORD[] batch = (RECORD[]) Array.newInstance( klass, batchSize );

        int i = 0;
        boolean hasNext = true;
        while ( hasNext )
        {
            if ( cursor.next( id ) && !IdValidator.isReservedId( id ) )
            {
                batch[i++] = (RECORD) record.clone();
            }
            if ( hasNext = idRange.hasNext() )
            {
                id = idRange.next();
            }
        }

        sender.send( i == batchSize ? batch : Arrays.copyOf( batch, i ) );
    }
}
