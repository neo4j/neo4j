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
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.impl.standard.Updater.RecordSize;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/*
 * Stressing the page cache:
 *  - we need to cause evictions etc
 *    that means the cache should cover only a fraction of a file, and updates should be spread to entire file
 *
 *  - we need to ensure serializable isolation
 *   we can't have multiple writers stepping on each others' toes
 *
 * So, each writer must overwrite previous writes so that we get potential conflicts, but in a way that allows us to
 * assert that previous writes were not lost or duplicated. Also, writers must keep a local manifest so that we can
 * assert at the end that all writes were made durable
 *
 * A scheme then, use prime factors:
 * - a writer A reads a previously written number x
 * - the writer multiplies x with his own prime number pA; pA is now a prime factor of x
 * - induction then tells us that starting with x=1, multiplying primes creates a number where the prime factors can
 *   be counted - or in other words, we can count the number of writes per writer per record, and compare to the
 *   writers' local manifest
 *
 * E.g. if a record R contains the checksum 147, it is made up of prime factors 147=3*49=3*7*7. That means 1x writes
 * from a thread B using the prime 3 + 2x writes from a writer C using the prime 7. Looking in the manifests of B and C
 * we should find counts 1 and 2 for record R.
 */
public class PageCacheStresser
{
    private static final int[] primes = new int[]{2, 3, 5, 7, 11, 13, 17, 19};

    private final int maxPages;
    private final int recordsPerPage;
    private final int numberOfThreads;

    private PageCacheStresser( int maxPages, int recordsPerPage, int numberOfThreads )
    {
        assertThat( format( "Only [%d] threads supported", primes.length ),
                numberOfThreads, is( lessThanOrEqualTo( primes.length ) ) );

        this.maxPages = maxPages;
        this.recordsPerPage = recordsPerPage;
        this.numberOfThreads = numberOfThreads;
    }

    public void stress( PageCache pageCache, Condition condition ) throws Exception
    {
        File file = Files.createTempFile( "pagecachestress", ".bin" ).toFile();

        PagedFile pagedFile = pageCache.map( file, recordsPerPage * RecordSize );

        prepare( pagedFile );

        Collection<Updater> updaters = executeWrites( condition, pagedFile );

        for ( Updater updater : updaters )
        {
            updater.verify();
        }

        pageCache.unmap( file );
    }

    private Collection<Updater> executeWrites( Condition condition, PagedFile pagedFile ) throws
            InterruptedException, java.util.concurrent.ExecutionException
    {
        Collection<Updater> updaters = prepareUpdaters( pagedFile, condition );
        ExecutorService executorService = Executors.newFixedThreadPool( updaters.size() );
        for ( Future<Void> future : executorService.invokeAll( updaters, 5, MINUTES ) )
        {
            future.get();
        }
        return updaters;
    }

    private void prepare( PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < maxPages; i++ )
            {
                assertTrue( "I must be able to access pages", cursor.next() );

                for ( int recordIndexInPage = 0; recordIndexInPage < recordsPerPage; recordIndexInPage++ )
                {
                    cursor.putLong( 1 );
                }

                assertFalse( "Exclusive lock, so never a need to retry", cursor.shouldRetry() );
            }
        }
    }

    private List<Updater> prepareUpdaters( PagedFile pagedFile, Condition condition )
    {
        List<Updater> updaters = new LinkedList<>();

        for ( int i = 0; i < numberOfThreads; i++ )
        {
            updaters.add( new Updater( pagedFile, condition, primes[i], maxPages, recordsPerPage ) );
        }

        return updaters;
    }

    public static PageCacheStresser create( int maxPages, int pageSize )
    {
        return new PageCacheStresser( maxPages, pageSize, 8 );
    }
}
