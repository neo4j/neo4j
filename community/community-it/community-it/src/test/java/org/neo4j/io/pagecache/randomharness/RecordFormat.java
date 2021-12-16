/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.pagecache.randomharness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.reserved_page_header_bytes;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public abstract class RecordFormat
{
    public abstract int getRecordSize();

    public abstract Record createRecord( Path file, int recordId, int page, int offset );

    public abstract Record readRecord( PageCursor cursor ) throws IOException;

    public abstract Record zeroRecord();

    public abstract void write( Record record, PageCursor cursor );

    public final void writeRecord( Record record, StoreChannel channel ) throws IOException
    {
        ByteBuffer buffer = ByteBuffers.allocate( getRecordSize(), INSTANCE );
        StubPageCursor cursor = new StubPageCursor( 0, buffer );
        write( record, cursor );
        channel.writeAll( buffer );
    }

    public final void fillWithRecords( PageCursor cursor )
    {
        cursor.setOffset( reserved_page_header_bytes.defaultValue() );
        int recordsPerPage = cursor.getCurrentPayloadSize() / getRecordSize();
        for ( int i = 0; i < recordsPerPage; i++ )
        {
            writeRecordToPage( cursor, cursor.getCurrentPageId(), recordsPerPage );
        }
    }

    private void writeRecordToPage( PageCursor cursor, long pageId, int recordsPerPage )
    {
        int pageRecordId = cursor.getOffset() / getRecordSize();
        int recordId = (int) (pageId * recordsPerPage + pageRecordId);
        Record record = createRecord( cursor.getCurrentFile(), recordId, (int) pageId, cursor.getOffset() );
        write( record, cursor );
    }

    public final void assertRecordsWrittenCorrectly( PageCursor cursor ) throws IOException
    {
        int currentPayloadSize = cursor.getCurrentPayloadSize();
        int recordSize = getRecordSize();
        int recordsPerPage = currentPayloadSize / recordSize;
        for ( int pageRecordId = 0; pageRecordId < recordsPerPage; pageRecordId++ )
        {
            long currentPageId = cursor.getCurrentPageId();
            int recordId = (int) (currentPageId * recordsPerPage + pageRecordId);
            Record expectedRecord = createRecord( cursor.getCurrentFile(), recordId, (int) currentPageId, cursor.getOffset() );
            Record actualRecord;
            actualRecord = readRecord( cursor );
            try
            {
                assertThat( actualRecord ).isIn( expectedRecord, zeroRecord() );
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( dumpPageContent( cursor ), t );
            }
        }
    }

    protected static String dumpPageContent( PageCursor cursor )
    {
        int initialOffset = cursor.getOffset();
        byte[] bytes = new byte[cursor.getCurrentPayloadSize()];
        cursor.setOffset( 0 );
        cursor.getBytes( bytes );
        cursor.setOffset( initialOffset );
        return "Current page: " + cursor.getCurrentPageId() + ", payloadSize: " + cursor.getCurrentPayloadSize() + " Offset: " + initialOffset + ", data: " +
                Arrays.toString( bytes );
    }

    public final void assertRecordsWrittenCorrectly( Path file, StoreChannel channel ) throws IOException
    {
        int recordSize = getRecordSize();
        int reservedBytes = reserved_page_header_bytes.defaultValue();
        long pagesInFile = channel.size() / PAGE_SIZE;
        int recordsInPage = (PAGE_SIZE - reservedBytes) / recordSize;
        long recordsInFile = pagesInFile * recordsInPage;
        ByteBuffer buffer = ByteBuffers.allocate( recordSize, INSTANCE );
        StubPageCursor cursor = new StubPageCursor( 0, buffer, 0 );
        int page = 0;
        for ( int i = 0; i < recordsInFile; i++ )
        {
            if ( i % recordsInPage == 0 )
            {
                channel.position( page * PAGE_SIZE + reservedBytes );
                page++;
            }
            assertThat( channel.read( buffer ) ).as( "reading record id " + i ).isEqualTo( recordSize );
            buffer.flip();
            Record expectedRecord = createRecord( file, i, i / recordsInPage, 0 );
            cursor.setOffset( 0 );
            Record actualRecord = readRecord( cursor );
            buffer.clear();
            try
            {
                assertThat( actualRecord ).isIn( expectedRecord, zeroRecord() );
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }
    }
}
