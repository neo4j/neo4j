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

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Resource;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

/**
 * The page cache warmer profiles the page cache to figure out what data is in memory and what is not, and uses those
 * profiles to load probably-desirable data into the page cache during startup.
 * <p>
 * The profiling data is stored in sibling files to the mapped files. These siblings have the same name as the mapped
 * file, except they start with a "." (to hide them on POSIX-like systems) and end with ".cacheprof".
 * <p>
 * These cacheprof files are compressed bitmaps where each raised bit indicates that the page identified by the
 * bit-index was in memory.
 */
public class PageCacheWarmer implements NeoStoreFileListing.StoreFileProvider
{
    public static final String SUFFIX_CACHEPROF = ".cacheprof";
    public static final String SUFFIX_CACHEPROF_TMP = ".cacheprof.tmp";

    // We use the deflate algorithm since it has been experimentally shown to be both the fastest,
    // and the algorithm that produces the smallest output.
    // For instance, a 5.7 GiB file where 7 out of 8 pages are in memory, produces a 57 KiB profile file,
    // where the uncompressed profile is 87.5 KiB. A 35% reduction.
    private static final String COMPRESSION_FORMAT = CompressorStreamFactory.getDeflate();
    private static final int IO_PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static final CompressorStreamFactory COMPRESSOR_FACTORY = new CompressorStreamFactory( true, 1024 );

    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final ExecutorService executor;
    private final PageLoaderFactory pageLoaderFactory;
    private volatile boolean stopped;

    PageCacheWarmer( FileSystemAbstraction fs, PageCache pageCache, JobScheduler scheduler )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.executor = buildExecutorService( scheduler );
        this.pageLoaderFactory = new PageLoaderFactory( executor, pageCache );
    }

    private ExecutorService buildExecutorService( JobScheduler scheduler )
    {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>( IO_PARALLELISM * 4 );
        RejectedExecutionHandler rejectionPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
        ThreadFactory threadFactory = scheduler.threadFactory( JobScheduler.Groups.pageCacheIOHelper );
        return new ThreadPoolExecutor(
                0, IO_PARALLELISM, 10, TimeUnit.SECONDS, workQueue,
                threadFactory, rejectionPolicy );
    }

    @Override
    public synchronized Resource addFilesTo( Collection<StoreFileMetadata> coll ) throws IOException
    {
        if ( stopped )
        {
            return Resource.EMPTY;
        }
        List<PagedFile> files = pageCache.listExistingMappings();
        for ( PagedFile file : files )
        {
            File profileFile = profileOutputFileFinal( file );
            if ( fs.fileExists( profileFile ) )
            {
                coll.add( new StoreFileMetadata( profileFile, 1, false ) );
            }
        }
        return Resource.EMPTY;
    }

    public void stop()
    {
        stopped = true;
        synchronized ( this )
        {
            // This synchronised block means we'll wait for any reheat() or profile() to notice our raised stopped flag,
            // before we return to the caller. This helps avoid races, e.g. if the page cache is closed while this page
            // cache warmer is still holding on to some mapped pages.
        }
        executor.shutdown();
    }

    /**
     * Reheat the page cache based on existing profiling data, or do nothing if no profiling data is available.
     *
     * @return An {@link OptionalLong} of the number of pages loaded in, or {@link OptionalLong#empty()} if the
     * reheating was stopped early via {@link #stop()}.
     * @throws IOException if anything goes wrong while reading the profiled data back in.
     */
    public synchronized OptionalLong reheat() throws IOException
    {
        long pagesLoaded = 0;
        List<PagedFile> files = pageCache.listExistingMappings();
        for ( PagedFile file : files )
        {
            try
            {
                pagesLoaded += reheat( file );
            }
            catch ( FileIsNotMappedException ignore )
            {
                // The database is allowed to map and unmap files while we are trying to heat it up.
            }
        }
        return stopped ? OptionalLong.empty() : OptionalLong.of( pagesLoaded );
    }

    private long reheat( PagedFile file ) throws IOException
    {
        long pagesLoaded = 0;
        File savedProfile = profileOutputFileFinal( file );

        if ( !fs.fileExists( savedProfile ) )
        {
            return pagesLoaded;
        }

        try ( InputStream inputStream = compressedInputStream( savedProfile );
              PageLoader loader = pageLoaderFactory.getLoader( file ) )
        {
            long pageId = 0;
            int b;
            while ( (b = inputStream.read()) != -1 )
            {
                for ( int i = 0; i < 8; i++ )
                {
                    if ( stopped )
                    {
                        return pagesLoaded;
                    }
                    if ( (b & 1) == 1 )
                    {
                        loader.load( pageId );
                        pagesLoaded++;
                    }
                    b >>= 1;
                    pageId++;
                }
            }
        }
        pageCache.reportEvents();
        return pagesLoaded;
    }

    /**
     * Profile the in-memory data in the page cache, and write it to "cacheprof" file siblings of the mapped files.
     *
     * @return An {@link OptionalLong} of the number of pages that were found to be in memory, or
     * {@link OptionalLong#empty()} if the profiling was stopped early via {@link #stop()}.
     * @throws IOException If anything goes wrong while accessing the page cache or writing out the profile data.
     */
    public synchronized OptionalLong profile() throws IOException
    {
        // Note that we could in principle profile the files in parallel. However, producing a profile is usually so
        // fast, that it rivals the overhead of starting and stopping threads. Because of this, the complexity of
        // profiling in parallel is just not worth it.
        long pagesInMemory = 0;
        List<PagedFile> files = pageCache.listExistingMappings();
        for ( PagedFile file : files )
        {
            try
            {
                pagesInMemory += profile( file );
            }
            catch ( FileIsNotMappedException ignore )
            {
                // The database is allowed to map and unmap files while we are profiling the page cache.
            }
        }
        pageCache.reportEvents();
        return stopped ? OptionalLong.empty() : OptionalLong.of( pagesInMemory );
    }

    private long profile( PagedFile file ) throws IOException
    {
        long pagesInMemory = 0;
        File outputNext = profileOutputFileNext( file );

        try ( OutputStream outputStream = compressedOutputStream( outputNext );
              PageCursor cursor = file.io( 0, PF_SHARED_READ_LOCK | PF_NO_FAULT ) )
        {
            int stepper = 0;
            int b = 0;
            for ( ; ; )
            {
                if ( stopped )
                {
                    return pagesInMemory;
                }
                if ( !cursor.next() )
                {
                    break; // Exit the loop if there are no more pages.
                }
                if ( cursor.getCurrentPageId() != PageCursor.UNBOUND_PAGE_ID )
                {
                    pagesInMemory++;
                    b |= 1 << stepper;
                }
                stepper++;
                if ( stepper == 8 )
                {
                    outputStream.write( b );
                    b = 0;
                    stepper = 0;
                }
            }
            outputStream.write( b );
            outputStream.flush();
        }

        File outputFinal = profileOutputFileFinal( file );
        fs.renameFile( outputNext, outputFinal, StandardCopyOption.REPLACE_EXISTING );
        return pagesInMemory;
    }

    private InputStream compressedInputStream( File input ) throws IOException
    {
        InputStream source = fs.openAsInputStream( input );
        try
        {
            return COMPRESSOR_FACTORY.createCompressorInputStream( COMPRESSION_FORMAT, source );
        }
        catch ( CompressorException e )
        {
            IOUtils.closeAllSilently( source );
            throw new IOException( "Exception when building decompressor.", e );
        }
    }

    private OutputStream compressedOutputStream( File output ) throws IOException
    {
        OutputStream sink = fs.openAsOutputStream( output, false );
        try
        {
            return COMPRESSOR_FACTORY.createCompressorOutputStream( COMPRESSION_FORMAT, sink );
        }
        catch ( CompressorException e )
        {
            IOUtils.closeAllSilently( sink );
            throw new IOException( "Exception when building compressor.", e );
        }
    }

    private File profileOutputFileFinal( PagedFile file )
    {
        File mappedFile = file.file();
        String profileOutputName = "." + mappedFile.getName() + SUFFIX_CACHEPROF;
        File parent = mappedFile.getParentFile();
        return new File( parent, profileOutputName );
    }

    private File profileOutputFileNext( PagedFile file )
    {
        File mappedFile = file.file();
        String profileOutputName = "." + mappedFile.getName() + SUFFIX_CACHEPROF_TMP;
        File parent = mappedFile.getParentFile();
        return new File( parent, profileOutputName );
    }
}
