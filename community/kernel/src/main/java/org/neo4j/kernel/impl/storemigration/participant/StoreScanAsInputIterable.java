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

import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;

import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * An {@link InputIterable} backed by a {@link RecordStore}, iterating over all used records.
 *
 * @param <INPUT> type of {@link InputEntity}
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
abstract class StoreScanAsInputIterable<INPUT extends InputEntity,RECORD extends AbstractBaseRecord>
        implements InputIterable<INPUT>
{
    private final RecordStore<RECORD> store;
    private final RecordCursor<RECORD> cursor;
    private final StoreSourceTraceability traceability;

    StoreScanAsInputIterable( RecordStore<RECORD> store )
    {
        this.store = store;
        this.cursor = store.newRecordCursor( store.newRecord() );
        this.traceability = new StoreSourceTraceability( store.toString(), store.getRecordSize() );
    }

    @Override
    public InputIterator<INPUT> iterator()
    {
        cursor.acquire( 0, CHECK );
        return new InputIterator.Adapter<INPUT>()
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
            public void close()
            {
                cursor.close();
            }

            @Override
            protected INPUT fetchNextOrNull()
            {
                while ( id < highId )
                {
                    if ( cursor.next( id++ ) )
                    {
                        RECORD record = cursor.get();
                        traceability.atId( record.getId() );
                        return inputEntityOf( record );
                    }
                }
                return null;
            }
        };
    }

    protected abstract INPUT inputEntityOf( RECORD record );

    @Override
    public boolean supportsMultiplePasses()
    {
        return true;
    }
}
