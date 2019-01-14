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
package org.neo4j.io.pagecache.stress;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RecordFormat
{
    private final int numberOfThreads;
    private final int cachePageSize;
    private final int fieldSize;
    private final int checksumFieldOffset;
    private final int recordSize;

    public RecordFormat( int numberOfThreads, int cachePageSize )
    {
        this.numberOfThreads = numberOfThreads;
        this.cachePageSize = cachePageSize;
        this.fieldSize = Long.BYTES;
        this.checksumFieldOffset = numberOfThreads * fieldSize;
        this.recordSize = checksumFieldOffset + fieldSize; // extra field for keeping the checksum.
    }

    public int getRecordSize()
    {
        return recordSize;
    }

    public int getRecordsPerPage()
    {
        return cachePageSize / getRecordSize();
    }

    public int getFilePageSize()
    {
        return getRecordsPerPage() * getRecordSize();
    }

    /**
     * Assume the given cursor is writable and has already been positioned at the record offset.
     */
    public long incrementCounter( PageCursor cursor, int threadId )
    {
        int recordOffset = cursor.getOffset();
        int fieldOffset = recordOffset + (fieldSize * threadId);
        int checksumOffset = recordOffset + checksumFieldOffset;

        long newValue = 1 + cursor.getLong( fieldOffset );
        cursor.putLong( fieldOffset, newValue );
        cursor.putLong( checksumOffset, 1 + cursor.getLong( checksumOffset ) );
        return newValue;
    }

    /**
     * Sum up the fields for the given thread for all records on the given page.
     */
    public long sumCountsForThread( PageCursor cursor, int threadId ) throws IOException
    {
        int recordsPerPage = getRecordsPerPage();
        int fieldOffset = fieldSize * threadId;
        long sum;
        do
        {
            sum = 0;
            for ( int i = 0; i < recordsPerPage; i++ )
            {
                sum += cursor.getLong( (i * recordSize) + fieldOffset );
            }
        }
        while ( cursor.shouldRetry() );
        return sum;
    }

    /**
     * Verify the checksums on all the records on the given page
     */
    public void verifyCheckSums( PageCursor cursor ) throws IOException
    {
        int recordsPerPage = getRecordsPerPage();
        for ( int i = 0; i < recordsPerPage; i++ )
        {
            int recordOffset = i * recordSize;
            long expectedChecksum;
            long actualChecksum;
            do
            {
                actualChecksum = 0;
                for ( int j = 0; j < numberOfThreads; j++ )
                {
                    actualChecksum += cursor.getLong( recordOffset + (j * fieldSize) );
                }
                expectedChecksum = cursor.getLong( recordOffset + checksumFieldOffset );
            }
            while ( cursor.shouldRetry() );
            String msg = "Checksum for record " + i + " on page " + cursor.getCurrentPageId();
            assertThat( msg, actualChecksum, is( expectedChecksum ) );
        }
    }
}
