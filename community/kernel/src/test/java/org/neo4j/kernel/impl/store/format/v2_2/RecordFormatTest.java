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
package org.neo4j.kernel.impl.store.format.v2_2;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.impl.store.format.Store;
import org.neo4j.kernel.impl.store.standard.StoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreToolkit;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;

public abstract class RecordFormatTest<FORMAT extends StoreFormat<RECORD, CURSOR>,
                                       RECORD,
                                       CURSOR extends Store.RecordCursor<RECORD>>
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    protected StubPageCursor pageCursor;
    protected final FORMAT format;
    protected StoreFormat.RecordFormat<RECORD> recordFormat;
    protected StoreToolkit storeToolkit;
    protected MuninnPageCache pageCache;
    protected PagedFile pagedFile;

    public RecordFormatTest( FORMAT format )
    {
        this.format = format;
        recordFormat = format.recordFormat();
    }

    @Before
    public void setup() throws IOException
    {
        int pageSize = 1024;
        pageCursor = new StubPageCursor( 0l, pageSize );
        pageCache = new MuninnPageCache( fsRule.get(), 1024, 1024, PageCacheMonitor.NULL );
        pagedFile = pageCache.map( new File("store"), 1024 );
        storeToolkit = new StoreToolkit( format.recordSize( null ), pageSize, 0, null, null );
    }

    //
    // Utilities
    //

    public void assertSerializes( RECORD record )
    {
        recordFormat.serialize( pageCursor, 0, record );
        RECORD deserialized = recordFormat.newRecord( 0 );
        recordFormat.deserialize( pageCursor, 0, recordFormat.id( record ), deserialized );
        assertEquals( record.toString(), deserialized.toString() );
    }

    public void writeToPagedFile( RECORD record ) throws IOException
    {
        try(PageCursor io = pagedFile.io( storeToolkit.pageId( recordFormat.id( record ) ), PagedFile.PF_EXCLUSIVE_LOCK ))
        {
            io.next();
            recordFormat.serialize( io, storeToolkit.recordOffset( recordFormat.id( record ) ), record );
        }
    }
}
