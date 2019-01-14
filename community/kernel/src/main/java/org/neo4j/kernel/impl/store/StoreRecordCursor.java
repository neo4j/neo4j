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
package org.neo4j.kernel.impl.store;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

class StoreRecordCursor<RECORD extends AbstractBaseRecord> implements RecordCursor<RECORD>
{
    private final RECORD record;
    private CommonAbstractStore<RECORD,?> store;
    private long currentId;
    private RecordLoad mode;
    private PageCursor pageCursor;

    StoreRecordCursor( RECORD record, CommonAbstractStore<RECORD,?> store )
    {
        this.record = record;
        this.store = store;
    }

    @Override
    public boolean next()
    {
        try
        {
            return next( currentId, record, mode );
        }
        finally
        {
            // This will get the next reference:
            // inUse ==> actual next reference
            // !inUse && mode == CHECK ==> NULL
            // !inUse && mode == NORMAL ==> NULL (+InvalidRecordException thrown in try)
            // !inUse && mode == FORCE ==> actual next reference
            currentId = store.getNextRecordReference( record );
        }
    }

    @Override
    public boolean next( long id )
    {
        return next( id, record, mode );
    }

    @Override
    public boolean next( long id, RECORD record, RecordLoad mode )
    {
        assert pageCursor != null : "Not initialized";
        if ( NULL_REFERENCE.is( id ) )
        {
            record.clear();
            record.setId( NULL_REFERENCE.intValue() );
            return false;
        }

        try
        {
            store.readIntoRecord( id, record, mode, pageCursor );
            return record.inUse();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void placeAt( long id, RecordLoad mode )
    {
        this.currentId = id;
        this.mode = mode;
    }

    @Override
    public void close()
    {
        assert pageCursor != null;
        this.pageCursor.close();
        this.pageCursor = null;
    }

    @Override
    public RECORD get()
    {
        return record;
    }

    @Override
    public RecordCursor<RECORD> acquire( long id, RecordLoad mode )
    {
        assert this.pageCursor == null;
        this.currentId = id;
        this.mode = mode;
        try
        {
            this.pageCursor = store.storeFile.io( store.pageIdForRecord( id ), PF_SHARED_READ_LOCK );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        return this;
    }
}
