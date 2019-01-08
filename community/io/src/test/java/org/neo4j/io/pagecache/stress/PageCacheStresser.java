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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.TinyLockManager;

import static org.junit.Assert.assertTrue;

/**
 * It works like this: We have N threads, and a number of records with N long fields plus a sum field. So each record
 * can verify their consistency by summing up their N fields and comparing the result to their sum field. Further, each
 * thread can also verify their consistency, by taking the sum of their respective N field across all records, and
 * comparing it to the number of increments they've done. The records are protected by entity locks, since page write
 * locks are not exclusive, so in the end we should see no lost updates. That is, both consistency checks should pass.
 * We will also have many more file pages and cache pages, so we'll have lots of concurrent eviction and page faulting
 * as well.
 */
public class PageCacheStresser
{
    private final int maxPages;
    private final int numberOfThreads;

    private final File workingDirectory;

    public PageCacheStresser( int maxPages, int numberOfThreads, File workingDirectory )
    {
        this.maxPages = maxPages;
        this.numberOfThreads = numberOfThreads;
        this.workingDirectory = workingDirectory;
    }

    public void stress( PageCache pageCache, Condition condition ) throws Exception
    {
        File file = Files.createTempFile( workingDirectory.toPath(), "pagecacheundertest", ".bin" ).toFile();
        file.deleteOnExit();

        int cachePageSize = pageCache.pageSize();
        RecordFormat format = new RecordFormat( numberOfThreads, cachePageSize );
        int filePageSize = format.getFilePageSize();

        try ( PagedFile pagedFile = pageCache.map( file, filePageSize ) )
        {
            List<RecordStresser> recordStressers = prepare( condition, pagedFile, format );
            verifyResults( format, pagedFile, recordStressers );
            execute( recordStressers );
            verifyResults( format, pagedFile, recordStressers );
        }
    }

    private List<RecordStresser> prepare( Condition condition, PagedFile pagedFile, RecordFormat format )
    {
        int maxRecords = Math.multiplyExact( maxPages, format.getRecordsPerPage() );
        TinyLockManager locks = new TinyLockManager();

        List<RecordStresser> recordStressers = new LinkedList<>();
        for ( int threadId = 0; threadId < numberOfThreads; threadId++ )
        {
            recordStressers.add( new RecordStresser( pagedFile, condition, maxRecords, format, threadId, locks ) );
        }
        return recordStressers;
    }

    private void execute( List<RecordStresser> recordStressers ) throws InterruptedException, ExecutionException
    {
        ExecutorService executorService = Executors.newFixedThreadPool( numberOfThreads, r ->
        {
            Thread thread = Executors.defaultThreadFactory().newThread( r );
            thread.setDaemon( true );
            return thread;
        } );
        List<Future<Void>> futures = executorService.invokeAll( recordStressers );
        for ( Future<Void> future : futures )
        {
            future.get();
        }
        executorService.shutdown();
        assertTrue( executorService.awaitTermination( 10, TimeUnit.SECONDS ) );
    }

    private void verifyResults( RecordFormat format, PagedFile pagedFile, List<RecordStresser> recordStressers )
            throws IOException
    {
        for ( RecordStresser stresser : recordStressers )
        {
            stresser.verifyCounts();
        }
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            while ( cursor.next() )
            {
                format.verifyCheckSums( cursor );
            }
        }
    }
}
