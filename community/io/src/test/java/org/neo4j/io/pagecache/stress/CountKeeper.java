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
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.stress.StressTestRecord.SizeOfCounter;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class CountKeeper
{
    private final PagedFile pagedFile;
    private final int countersPerRecord;

    public CountKeeper( PagedFile pagedFile, int countersPerRecord )
    {
        this.pagedFile = pagedFile;
        this.countersPerRecord = countersPerRecord;
    }

    public void onCounterUpdated( int pageNumber, int recordNumber, int counterNumber ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( pageNumber, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( "i must be able to access pages", cursor.next(), is( true ) );

            setOffset( cursor, recordNumber, counterNumber );
            long count = cursor.getLong();
            setOffset( cursor, recordNumber, counterNumber );
            cursor.putLong( count + 1 );
        }
    }

    private void setOffset( PageCursor cursor, int recordNumber, int counterNumber )
    {
        cursor.setOffset( (recordNumber * countersPerRecord + counterNumber) * SizeOfCounter );
    }
}
