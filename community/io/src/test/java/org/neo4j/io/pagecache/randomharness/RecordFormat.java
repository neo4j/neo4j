/*
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
package org.neo4j.io.pagecache.randomharness;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

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
        int recordsPerPage = cursor.getCurrentPageSize() / getRecordSize();
        for ( int pageRecordId = 0; pageRecordId < recordsPerPage; pageRecordId++ )
        {
            int recordId = (int) (cursor.getCurrentPageId() * recordsPerPage + pageRecordId);
            Record expectedRecord = createRecord( cursor.getCurrentFile(), recordId );
            Record actualRecord;
            int offset = cursor.getOffset();
            do
            {
                cursor.setOffset( offset );
                actualRecord = readRecord( cursor );
            }
            while ( cursor.shouldRetry() );
            assertThat( actualRecord, isOneOf( expectedRecord, zeroRecord() ) );
        }
    }
}
