/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.pagecache;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.scheduler.JobScheduler;

/**
 * The page cache warmer, in this rudimentary implementation, just blindly loads in all pages of all mapped files at
 * startup.
 */
public class PageCacheWarmer
{
    private static final int IO_PARALLELISM = Runtime.getRuntime().availableProcessors();

    private final PageCache pageCache;
    private final JobScheduler scheduler;
    private volatile boolean stopped;
    private ExecutorService executor;

    PageCacheWarmer( PageCache pageCache, JobScheduler scheduler )
    {
        this.pageCache = pageCache;
        this.scheduler = scheduler;
    }

    public synchronized void start()
    {
        stopped = false;
        executor = buildExecutorService( scheduler );
    }

    public void stop()
    {
        stopped = true;
        stopWarmer();
    }

    /**
     * Stopping warmer process.
     */
    private synchronized void stopWarmer()
    {
        if ( executor != null )
        {
            executor.shutdown();
            executor = null;
        }
    }

    /**
     * Reheat the page cache by accessing all pages of all mapped files.
     *
     * @return An {@link OptionalLong} of the number of pages loaded in, or {@link OptionalLong#empty()} if the
     * reheating was stopped early via {@link #stop()}.
     * @throws IOException if anything goes wrong while reading the data back in.
     */
    synchronized OptionalLong reheat() throws IOException
    {
        if ( stopped )
        {
            return OptionalLong.empty();
        }
        long pagesLoaded = 0;
        List<PagedFile> files = pageCache.listExistingMappings();
        for ( PagedFile file : files )
        {
            try
            {
                pagesLoaded += reheat( file );
            }
            catch ( Exception ignore )
            {
                // The database is allowed to map and unmap files while we are trying to heat it up.
            }
        }
        return OptionalLong.of( pagesLoaded );
    }

    private long reheat( PagedFile file ) throws IOException
    {
        long lastPageId = file.getLastPageId();
        try ( ParallelPageLoader loader = new ParallelPageLoader( file, executor, pageCache ) )
        {
            long pageId = 0;
            while ( pageId <= lastPageId )
            {
                if ( stopped )
                {
                    pageCache.reportEvents();
                    return pageId;
                }
                loader.load( pageId );
                pageId++;
            }
        }
        pageCache.reportEvents();
        return lastPageId;
    }

    private ExecutorService buildExecutorService( JobScheduler scheduler )
    {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>( IO_PARALLELISM * 4 );
        RejectedExecutionHandler rejectionPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
        ThreadFactory threadFactory = scheduler.threadFactory( JobScheduler.Groups.storageMaintenance );
        return new ThreadPoolExecutor(
                0, IO_PARALLELISM, 10, TimeUnit.SECONDS, workQueue,
                threadFactory, rejectionPolicy );
    }
}
