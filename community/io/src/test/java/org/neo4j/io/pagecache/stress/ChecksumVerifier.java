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

public class ChecksumVerifier
{
    private final int recordsPerPage;
    private final int countersPerRecord;

    public ChecksumVerifier( int recordsPerPage, int countersPerRecord )
    {
        this.recordsPerPage = recordsPerPage;
        this.countersPerRecord = countersPerRecord;
    }

    public void verifyChecksums( PagedFile pagedFile ) throws Exception
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                for ( int recordNumber = 0; recordNumber < recordsPerPage; recordNumber++ )
                {
                    verifyChecksum( cursor, recordNumber );
                }
            }
        }
    }

    public void verifyChecksum( PageCursor cursor, int recordNumber ) throws IOException
    {
        long actualChecksum = 0;
        long checksum;

        do
        {
            cursor.setOffset( recordNumber * (countersPerRecord + 1) * SizeOfCounter );

            for ( int i = 0; i < countersPerRecord; i++ )
            {
                long count = cursor.getLong();

                actualChecksum += count;
            }

            checksum = cursor.getLong();
        } while ( cursor.shouldRetry() );

        assertThat( actualChecksum, is( checksum ) );
    }

}
