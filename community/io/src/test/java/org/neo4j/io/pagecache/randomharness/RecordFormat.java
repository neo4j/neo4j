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
package org.neo4j.io.pagecache.randomharness;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;

public abstract class RecordFormat
{
    public abstract int getRecordSize();

    public abstract Record createRecord( File file, int recordId );

    public abstract Record readRecord( PageCursor cursor ) throws IOException;

    public abstract Record zeroRecord();

    public abstract void write( Record record, PageCursor cursor );

    public final void writeRecord( PageCursor cursor )
    {
        int recordsPerPage = cursor.getCurrentPageSize() / getRecordSize();
        writeRecordToPage( cursor, cursor.getCurrentPageId(), recordsPerPage );
    }

    public final void writeRecord( Record record, StoreChannel channel ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( getRecordSize() );
        StubPageCursor cursor = new StubPageCursor( 0, buffer );
        write( record, cursor );
        channel.writeAll( buffer );
    }

    public final void fillWithRecords( PageCursor cursor )
    {
        cursor.setOffset( 0 );
        int recordsPerPage = cursor.getCurrentPageSize() / getRecordSize();
        for ( int i = 0; i < recordsPerPage; i++ )
        {
            writeRecordToPage( cursor, cursor.getCurrentPageId(), recordsPerPage );
        }
    }

    private void writeRecordToPage( PageCursor cursor, long pageId, int recordsPerPage )
    {
        int pageRecordId = cursor.getOffset() / getRecordSize();
        int recordId = (int) (pageId * recordsPerPage + pageRecordId);
        Record record = createRecord( cursor.getCurrentFile(), recordId );
        write( record, cursor );
    }

    public final void assertRecordsWrittenCorrectly( PageCursor cursor ) throws IOException
    {
        int currentPageSize = cursor.getCurrentPageSize();
        int recordSize = getRecordSize();
        int recordsPerPage = currentPageSize / recordSize;
        for ( int pageRecordId = 0; pageRecordId < recordsPerPage; pageRecordId++ )
        {
            long currentPageId = cursor.getCurrentPageId();
            int recordId = (int) (currentPageId * recordsPerPage + pageRecordId);
            Record expectedRecord = createRecord( cursor.getCurrentFile(), recordId );
            Record actualRecord;
            actualRecord = readRecord( cursor );
            assertThat( actualRecord, isOneOf( expectedRecord, zeroRecord() ) );
        }
    }

    public final void assertRecordsWrittenCorrectly( File file, StoreChannel channel ) throws IOException
    {
        int recordSize = getRecordSize();
        long recordsInFile = channel.size() / recordSize;
        ByteBuffer buffer = ByteBuffer.allocateDirect( recordSize );
        StubPageCursor cursor = new StubPageCursor( 0, buffer );
        for ( int i = 0; i < recordsInFile; i++ )
        {
            assertThat( "reading record id " + i, channel.read( buffer ), is( recordSize ) );
            buffer.flip();
            Record expectedRecord = createRecord( file, i );
            cursor.setOffset( 0 );
            Record actualRecord = readRecord( cursor );
            buffer.clear();
            assertThat( actualRecord, isOneOf( expectedRecord, zeroRecord() ) );
        }
    }
}
