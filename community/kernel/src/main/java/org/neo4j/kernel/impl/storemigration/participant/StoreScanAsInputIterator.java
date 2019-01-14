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
package org.neo4j.kernel.impl.storemigration.participant;

import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

import static java.lang.Long.min;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * An {@link InputIterator} backed by a {@link RecordStore}, iterating over all used records.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
abstract class StoreScanAsInputIterator<RECORD extends AbstractBaseRecord> implements InputIterator
{
    private final RecordStore<RECORD> store;
    private final int batchSize;
    private final long highId;
    private long id;

    StoreScanAsInputIterator( RecordStore<RECORD> store )
    {
        this.store = store;
        this.batchSize = store.getRecordsPerPage() * 10;
        this.highId = store.getHighId();
    }

    RecordCursor<RECORD> createCursor()
    {
        return store.newRecordCursor( store.newRecord() ).acquire( 0, CHECK );
    }

    @Override
    public void close()
    {
    }

    @Override
    public synchronized boolean next( InputChunk chunk )
    {
        if ( id >= highId )
        {
            return false;
        }
        long startId = id;
        id = min( highId, startId + batchSize );
        ((StoreScanChunk)chunk).initialize( startId, id );
        return true;
    }
}
