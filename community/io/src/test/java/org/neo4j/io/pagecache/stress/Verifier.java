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

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

import java.util.concurrent.Callable;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Verifier implements Callable<Void>
{
    private final PagedFile pagedFile;
    private final int counterNumber;
    private final int recordsPerPage;
    private final long[] writesPerRecord;
    private final RecordVerifierUpdater recordVerifierUpdater;

    public Verifier( PagedFile pagedFile, int counterNumber, int recordsPerPage, long[] writesPerRecord, RecordVerifierUpdater recordVerifierUpdater )
    {
        this.pagedFile = pagedFile;
        this.counterNumber = counterNumber;
        this.recordsPerPage = recordsPerPage;
        this.writesPerRecord = writesPerRecord;
        this.recordVerifierUpdater = recordVerifierUpdater;
    }

    @Override
    public Void call() throws Exception
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for ( int pageNumber = 0; cursor.next(); pageNumber++ )
            {
                for ( int recordIndex = 0; recordIndex < recordsPerPage; recordIndex++ )
                {
                    long expectedCount = writesPerRecord[pageNumber * recordsPerPage + recordIndex];

                    recordVerifierUpdater.verifyCount( cursor, recordIndex, counterNumber, expectedCount );
                }
            }
        }

        return null;
    }
}
