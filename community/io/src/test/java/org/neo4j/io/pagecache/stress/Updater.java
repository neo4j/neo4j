/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

import java.util.Random;
import java.util.concurrent.Callable;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Updater implements Callable<Updater>
{
    private static final Random Random = new Random();

    private final PagedFile pagedFile;
    private final Condition condition;
    private final int maxPages;
    private final int recordsPerPage;
    private final RecordVerifierUpdater recordVerifierUpdater;
    private final int counterNumber;
    private long[] writesPerRecord;

    public Updater( PagedFile pagedFile, Condition condition, int maxPages, int recordsPerPage, RecordVerifierUpdater
            recordVerifierUpdater, int counterNumber )
    {
        this.pagedFile = pagedFile;
        this.condition = condition;
        this.maxPages = maxPages;
        this.recordsPerPage = recordsPerPage;
        this.recordVerifierUpdater = recordVerifierUpdater;
        this.counterNumber = counterNumber;
    }

    @Override
    public Updater call() throws Exception
    {
        writesPerRecord = new long[maxPages * recordsPerPage];

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            while ( !condition.fulfilled() )
            {
                int pageNumber = Random.nextInt( maxPages );
                assertTrue( "I must be able to access pages", cursor.next( pageNumber ) );

                int recordIndex = Random.nextInt( recordsPerPage );
                recordVerifierUpdater.verifyChecksumAndUpdateCount( cursor, recordIndex, counterNumber );
                writesPerRecord[pageNumber * recordsPerPage + recordIndex]++;

                assertFalse( "Exclusive lock, so never a need to retry", cursor.shouldRetry() );
            }
        }

        return this;
    }

    public void verifyCounts() throws Exception
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for ( int pageNumber = 0; pageNumber < maxPages; pageNumber++ )
            {
                assertTrue( format( "Unable to access page [%d]", pageNumber ), cursor.next( pageNumber ) );

                for ( int recordIndex = 0; recordIndex < recordsPerPage; recordIndex++ )
                {
                    long expectedCount = writesPerRecord[pageNumber * recordsPerPage + recordIndex];

                    recordVerifierUpdater.verifyCount( cursor, recordIndex, counterNumber, expectedCount );
                }
            }
        }
    }
}
