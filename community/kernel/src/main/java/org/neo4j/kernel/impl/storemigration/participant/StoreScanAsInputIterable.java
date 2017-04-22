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
package org.neo4j.kernel.impl.storemigration.participant;

import java.io.IOException;

import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import static java.lang.Long.min;

import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * An {@link InputIterable} backed by a {@link RecordStore}, iterating over all used records.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
abstract class StoreScanAsInputIterable<RECORD extends AbstractBaseRecord> implements InputIterable
{
    private final RecordStore<RECORD> store;
    private final StoreSourceTraceability traceability;
    private final int batchSize;

    StoreScanAsInputIterable( RecordStore<RECORD> store )
    {
        this.store = store;
        this.traceability = new StoreSourceTraceability( store.toString(), store.getRecordSize() );
        this.batchSize = store.getRecordsPerPage() * 10;
    }

    @Override
    public InputIterator iterator()
    {
        return new InputIterator.Adapter()
        {
            private final long highId = store.getHighId();
            private long id;

            @Override
            public String sourceDescription()
            {
                return traceability.sourceDescription();
            }

            @Override
            public long lineNumber()
            {
                return traceability.lineNumber();
            }

            @Override
            public long position()
            {
                return traceability.position();
            }

            @Override
            public InputChunk newChunk()
            {
                RecordCursor<RECORD> cursor = store.newRecordCursor( store.newRecord() ).acquire( 0, CHECK );
                return new StoreScanChunk( cursor );
            }

            @Override
            public synchronized boolean next( InputChunk chunk ) throws IOException
            {
                if ( id >= highId )
                {
                    return false;
                }
                long startId = id;
                id = min( highId, startId + batchSize );
                ((StoreScanChunk)chunk).initialize( startId, id );
                traceability.atId( startId );
                return true;
            }
        };
    }

    private class StoreScanChunk implements InputChunk
    {
        private final RecordCursor<RECORD> cursor;
        private long id;
        private long endId;

        public StoreScanChunk( RecordCursor<RECORD> cursor )
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
        public void close() throws IOException
        {
            cursor.close();
        }
    }

    protected abstract boolean visitRecord( RECORD record, InputEntityVisitor visitor );

    @Override
    public boolean supportsMultiplePasses()
    {
        return true;
    }
}
