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

import java.io.File;
import java.nio.file.Files;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static java.lang.System.getProperty;
import static java.nio.file.Paths.get;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.neo4j.io.pagecache.stress.StressTestRecord.SizeOfCounter;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

/**
 * A stress test for page cache(s).
 *
 * The test will stress a page cache by mutating records and keeping an invariant for each record. Thus, before writing
 * to a record, the record is be tested to see if the invariant still holds. Also, at the end of the test all records
 * are verified in that same manner.
 *
 * The test runs using multiple threads. It relies on page cache's exclusive locks to maintain the invariant.
 *
 * The page cache covers a fraction of a file, and the access pattern is uniformly random, so that pages are loaded
 * and evicted frequently.
 *
 * Records: a record is 1x counter for each thread, indexed by the threads' number, with 1x checksum = sum of counters.
 *
 * Invariant: the sum of counters is always equal to the checksum. For a blank file, this is trivially true:
 * sum(0, 0, 0, ...) = 0. Any record mutation is a counter increment and checksum increment.
 */
public class PageCacheStressTest
{
    private final int numberOfPages;
    private final int recordsPerPage;
    private final int numberOfThreads;

    private final int numberOfCachePages;
    private final int cachePageSize;

    private final PageCacheTracer tracer;
    private final Condition condition;

    private final String workingDirectory;

    private PageCacheStressTest( Builder builder )
    {
        this.numberOfPages = builder.numberOfPages;
        this.recordsPerPage = builder.recordsPerPage;
        this.numberOfThreads = builder.numberOfThreads;

        this.numberOfCachePages = builder.numberOfCachePages;
        this.cachePageSize = builder.cachePageSize;

        this.tracer = builder.tracer;
        this.condition = builder.condition;

        this.workingDirectory = builder.workingDirectory;
    }

    public void run() throws Exception
    {
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( fs );
        PageCache pageCacheUnderTest = new MuninnPageCache(
                swapperFactory, numberOfCachePages, cachePageSize, tracer );
        PageCache pageCacheKeepingCount = new MuninnPageCache(
                swapperFactory, numberOfCachePages, cachePageSize, tracer );

        try
        {
            File file = Files.createTempFile( get( workingDirectory ), "pagecachekeepingcounts", ".bin" ).toFile();
            file.deleteOnExit();
            PagedFile pagedFile = pageCacheKeepingCount.map( file, recordsPerPage * numberOfThreads * SizeOfCounter );

            CountKeeperFactory countKeeperFactory = new CountKeeperFactory( pagedFile, recordsPerPage, numberOfThreads );
            PageCacheStresser pageCacheStresser = new PageCacheStresser( numberOfPages, recordsPerPage, numberOfThreads, workingDirectory );

            pageCacheStresser.stress( pageCacheUnderTest, condition, countKeeperFactory );

            pagedFile.close();
        }
        finally
        {
            pageCacheUnderTest.close();
            pageCacheKeepingCount.close();
        }
    }

    /**
     * Default stress test config:
     *
     * Target page size is 8192 which is what the product uses by default
     *
     * 8 threads => 8*8 bytes for counters + 8 bytes for checksum = 72 bytes per record
     * <p>
     * 8192 bytes per page / 72 bytes per record = 113 records per page
     * <p>
     * 8192 bytes per page - 72 bytes per record * 113 records per page =
     * 8192 bytes per page - 8136 bytes for the records in the page =
     * 56 bytes padding
     * <p>
     * 8136 bytes per page * 100,000 pages = 776 MB for the whole file
     * <p>
     * 8192 bytes per page * 10,000 pages = 78 MB cache in memory
     *
     * 8 threads * 1 counter per thread per record * 100,000 pages * 113 records per page * 8 bytes per counter =
     * 8 counter per record * 11,300,000 records * 8 bytes per counter =
     * 90,400,000 counters * 8 bytes per counter =
     * 723,200,000 bytes = 690 MB memory for counters
     */
    public static class Builder
    {
        int numberOfPages = 10000;
        int recordsPerPage = 113;
        int numberOfThreads = 8;
        int cachePagePadding = 56;

        int numberOfCachePages = 1000;
        int cachePageSize;

        PageCacheTracer tracer = NULL;
        Condition condition;

        String workingDirectory = getProperty( "java.io.tmpdir" );

        public PageCacheStressTest build()
        {
            assertThat( "the cache should cover only a fraction of the mapped file",
                    numberOfPages, is( greaterThanOrEqualTo( 10 * numberOfCachePages ) ) );

            int pageSize = recordsPerPage * (numberOfThreads + 1) * SizeOfCounter;

            assertThat( "padding should not allow another page to fit", cachePagePadding, is( lessThan( pageSize ) ) );

            cachePageSize = pageSize + cachePagePadding;

            return new PageCacheStressTest( this );
        }

        public Builder with( PageCacheTracer tracer )
        {
            this.tracer = tracer;
            return this;
        }

        public Builder with( Condition condition )
        {
            this.condition = condition;
            return this;
        }

        public Builder withNumberOfPages( int value )
        {
            this.numberOfPages = value;
            return this;
        }

        public Builder withRecordsPerPage( int value )
        {
            this.recordsPerPage = value;
            return this;
        }

        public Builder withNumberOfThreads( int value )
        {
            this.numberOfThreads = value;
            return this;
        }

        public Builder withCachePagePadding( int value )
        {
            this.cachePagePadding = value;
            return this;
        }

        public Builder withNumberOfCachePages( int value )
        {
            this.numberOfCachePages = value;
            return this;
        }

        public Builder withWorkingDirectory( String workingDirectory )
        {
            this.workingDirectory = workingDirectory;
            return this;
        }
    }
}
