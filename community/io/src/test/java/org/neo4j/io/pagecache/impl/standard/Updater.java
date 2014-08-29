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
package org.neo4j.io.pagecache.impl.standard;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

import java.util.Random;
import java.util.concurrent.Callable;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Updater implements Callable<Void>
{
    private static final Random Random = new Random();

    static final int RecordSize = 8;

    private final PagedFile pagedFile;
    private final Condition condition;
    private final int prime;
    private final int numberOfPages;
    private final int recordsPerPage;
    private short[] writes;

    public Updater( PagedFile pagedFile, Condition condition, int prime, int numberOfPages, int recordsPerPage )
    {
        this.pagedFile = pagedFile;
        this.condition = condition;
        this.prime = prime;
        this.numberOfPages = numberOfPages;
        this.recordsPerPage = recordsPerPage;
    }

    @Override
    public Void call() throws Exception
    {
        writes = new short[numberOfPages * recordsPerPage];

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            while ( !condition.fulfilled() )
            {
                int pageId = Random.nextInt( numberOfPages );

                assertTrue( cursor.next( pageId ) );

                do
                {
                    int recordIndex = Random.nextInt( recordsPerPage );
                    int pageOffset = recordIndex * RecordSize;
                    boolean success = mutatePage( cursor, pageOffset );
                    if ( success )
                    {
                        writes[pageId * recordsPerPage + recordIndex]++;
                    }
                }
                while ( cursor.shouldRetry() );
            }
        }

        return null;
    }

    public void verify() throws Exception
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for ( int pageNumber = 0; pageNumber < numberOfPages; pageNumber++ )
            {
                assertTrue( format( "Unable to access page [%d]", pageNumber ), cursor.next(pageNumber) );

                for ( int recordIndexInPage = 0; recordIndexInPage < recordsPerPage; recordIndexInPage++ )
                {
                    long recordChecksum = cursor.getLong();
                    short count = writes[(pageNumber * recordsPerPage + recordIndexInPage)];

                    long threadSignature = (long) Math.pow( prime, count );

                    assertTrue( format( "update lost [%d, %d]", threadSignature, recordChecksum ),
                            recordChecksum % threadSignature == 0 );

                    long largerSignature = threadSignature * prime;

                    assertTrue( format( "duplicate update [%d, %d]", largerSignature, recordChecksum ),
                            recordChecksum % largerSignature != 0 );
                }
            }
        }
    }

    private boolean mutatePage( PageCursor cursor, int offset )
    {
        cursor.setOffset( offset );
        long product = cursor.getLong();

        if ( product > Long.MAX_VALUE / prime )
        {
            return false;
        }

        if ( product == 0 )
        {
            product = prime;
        }
        else
        {
            product *= prime;
        }

        cursor.setOffset( offset );
        cursor.putLong( product );

        return true;
    }
}
