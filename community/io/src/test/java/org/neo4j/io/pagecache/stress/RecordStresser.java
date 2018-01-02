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

import java.util.Random;
import java.util.concurrent.Callable;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;

public class RecordStresser implements Callable<Void>
{
    private static final Random Random = new Random();

    private final PagedFile pagedFile;
    private final Condition condition;
    private final ChecksumVerifier checksumVerifier;
    private final CountUpdater countUpdater;
    private final CountKeeper countKeeper;
    private final int maxPages;
    private final int recordsPerPage;
    private final int counterNumber;

    public RecordStresser( PagedFile pagedFile,
                           Condition condition,
                           ChecksumVerifier checksumVerifier,
                           CountUpdater countUpdater,
                           CountKeeper countKeeper,
                           int maxPages,
                           int recordsPerPage,
                           int counterNumber )
    {
        this.pagedFile = pagedFile;
        this.condition = condition;
        this.checksumVerifier = checksumVerifier;
        this.countUpdater = countUpdater;
        this.countKeeper = countKeeper;
        this.maxPages = maxPages;
        this.recordsPerPage = recordsPerPage;
        this.counterNumber = counterNumber;
    }

    @Override
    public Void call() throws Exception
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            while ( !condition.fulfilled() )
            {
                int pageNumber = Random.nextInt( maxPages );
                assertTrue( "I must be able to access pages", cursor.next( pageNumber ) );

                int recordNumber = Random.nextInt( recordsPerPage );

                checksumVerifier.verifyChecksum( cursor, recordNumber );

                countUpdater.updateCount( cursor, recordNumber, counterNumber );

                countKeeper.onCounterUpdated( pageNumber, recordNumber, counterNumber );

                assertFalse( "Exclusive lock, so never a need to retry", cursor.shouldRetry() );
            }
        }

        return null;
    }
}
