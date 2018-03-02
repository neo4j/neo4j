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
package org.neo4j.kernel.impl.storemigration.participant;

import java.io.IOException;

import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

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

    @Override
    public InputChunk newChunk()
    {
        RecordCursor<RECORD> cursor = store.newRecordCursor( store.newRecord() ).acquire( 0, CHECK );
        return new StoreScanChunk( cursor );
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

    private class StoreScanChunk implements InputChunk
    {
        private final RecordCursor<RECORD> cursor;
        private long id;
        private long endId;

        StoreScanChunk( RecordCursor<RECORD> cursor )
        {
            this.cursor = cursor;
        }

        @Override
        public boolean next( InputEntityVisitor visitor ) throws IOException
        {
            if ( id < endId )
            {
                if ( cursor.next( id ) )
                {
                    visitRecord( cursor.get(), visitor );
                    visitor.endOfEntity();
                }
                id++;
                return true;
            }
            return false;
        }

        public void initialize( long startId, long endId )
        {
            this.id = startId;
            this.endId = endId;
        }

        @Override
        public void close()
        {
            cursor.close();
        }
    }

    protected abstract boolean visitRecord( RECORD record, InputEntityVisitor visitor );
}
