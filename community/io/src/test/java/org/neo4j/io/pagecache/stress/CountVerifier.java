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
package org.neo4j.io.pagecache.stress;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.io.pagecache.stress.StressTestRecord.SizeOfCounter;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class CountVerifier
{
    private final PagedFile pagedFile;
    private final int recordsPerPage;
    private final int countersPerRecord;

    public CountVerifier( PagedFile pagedFile, int recordsPerPage, int countersPerRecord )
    {
        this.pagedFile = pagedFile;
        this.recordsPerPage = recordsPerPage;
        this.countersPerRecord = countersPerRecord;
    }

    public void verifyCounts( PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for ( int pageNumber = 0; cursor.next(); pageNumber++ )
            {
                verifyPage( cursor, pageNumber );
            }
        }
    }

    private void verifyPage( PageCursor cursor, int pageNumber ) throws IOException
    {
        long[][] countsForPage;

        do
        {
            countsForPage = new long[recordsPerPage][countersPerRecord];

            for ( int recordNumber = 0; recordNumber < recordsPerPage; recordNumber++ )
            {
                cursor.setOffset( recordNumber * (countersPerRecord + 1) * SizeOfCounter );

                for ( int counterNumber = 0; counterNumber < countersPerRecord; counterNumber++ )
                {
                    countsForPage[recordNumber][counterNumber] = cursor.getLong();
                }
            }
        } while ( cursor.shouldRetry() );

        for ( int recordNumber = 0; recordNumber < recordsPerPage; recordNumber++ )
        {
            for ( int counterNumber = 0; counterNumber < countersPerRecord; counterNumber++ )
            {
                assertThat( countsForPage[recordNumber][counterNumber],
                        is( getExpectedCount( pageNumber, recordNumber, counterNumber ) ) );
            }
        }
    }

    private long getExpectedCount( int pageNumber, int recordNumber, int counterNumber ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( pageNumber, PF_SHARED_LOCK ) )
        {
            assertThat( "i must be able to access pages", cursor.next(), is( true ) );

            long count;

            do
            {
                cursor.setOffset( (recordNumber * countersPerRecord + counterNumber) * SizeOfCounter );
                count = cursor.getLong();
            } while ( cursor.shouldRetry() );

            return count;
        }
    }
}
