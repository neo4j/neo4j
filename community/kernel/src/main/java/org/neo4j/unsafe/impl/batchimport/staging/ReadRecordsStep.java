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
import java.util.stream.Stream;

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
public abstract class ReadRecordsStep<RECORD extends AbstractBaseRecord> extends IoProducerStep
{
    protected final RecordStore<RECORD> store;
    protected final RECORD record;
    protected final RecordCursor<RECORD> cursor;
    protected final long highId;

    public ReadRecordsStep( StageControl control, Configuration config, RecordStore<RECORD> store )
    {
        super( control, config );
        this.store = store;
        this.cursor = store.newRecordCursor( record = store.newRecord() );
        this.highId = store.getHighId();
    }

    @Override
    public void start( int orderingGuarantees )
    {
        cursor.acquire( 0, CHECK );
        super.start( orderingGuarantees );
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        cursor.close();
    }

    protected RECORD[] removeRecordWithReservedId( RECORD[] records, boolean seenReservedId )
    {
        if ( !seenReservedId )
        {
            return records;
        }
        return Stream.of( records )
                .filter( record -> !IdValidator.isReservedId( record.getId() ) )
                .toArray( length -> newArray( length, records.getClass().getComponentType() ) );
    }

    @SuppressWarnings( "unchecked" )
    private RECORD[] newArray( int length, Class<?> componentType )
    {
        return (RECORD[]) Array.newInstance( componentType, length );
    }
}
