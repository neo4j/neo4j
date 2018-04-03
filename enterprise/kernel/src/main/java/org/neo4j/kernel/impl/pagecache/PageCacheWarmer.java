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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

import org.neo4j.graphdb.Resource;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
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
public class PageCacheWarmer extends LifecycleAdapter implements NeoStoreFileListing.StoreFileProvider
{
    public static final String SUFFIX_CACHEPROF = ".cacheprof";
    private static final String PROFILE_FOLDER = "profile";

    private static final int IO_PARALLELISM = Runtime.getRuntime().availableProcessors();

    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final JobScheduler scheduler;
    private volatile boolean stopOngoingWarming;
    private ExecutorService executor;
    private PageLoaderFactory pageLoaderFactory;

    PageCacheWarmer( FileSystemAbstraction fs, PageCache pageCache, JobScheduler scheduler )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.scheduler = scheduler;
    }

    @Override
    public synchronized Resource addFilesTo( Collection<StoreFileMetadata> coll ) throws IOException
    {
        if ( stopOngoingWarming )
        {
            return Resource.EMPTY;
        }
        List<PagedFile> files = pageCache.listExistingMappings();
        for ( PagedFile file : files )
        {
            File profileFile = profileOutputFileName( file );
            if ( fs.fileExists( profileFile ) )
            {
                coll.add( new StoreFileMetadata( profileFile, 1, false ) );
            }
        }
        return Resource.EMPTY;
    }

    @Override
    public synchronized void start()
    {
        stopOngoingWarming = false;
        executor = buildExecutorService( scheduler );
        pageLoaderFactory = new PageLoaderFactory( executor, pageCache );
    }

    @Override
    public void stop()
    {
        stopOngoingWarming = true;
        stopWarmer();
    }

    /**
     * Stopping warmer process, needs to be synchronised to prevent racing with profiling and heating
     */
    private synchronized void stopWarmer()
    {
        if ( executor != null )
        {
            executor.shutdown();
        }
    }

    /**
     * Reheat the page cache based on existing profiling data, or do nothing if no profiling data is available.
     *
     * @return An {@link OptionalLong} of the number of pages loaded in, or {@link OptionalLong#empty()} if the
     * reheating was stopped early via {@link #stop()}.
     * @throws IOException if anything goes wrong while reading the profiled data back in.
     */
    synchronized OptionalLong reheat() throws IOException
    {
        if ( stopOngoingWarming )
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
            catch ( FileIsNotMappedException ignore )
            {
                // The database is allowed to map and unmap files while we are trying to heat it up.
            }
        }
        return OptionalLong.of( pagesLoaded );
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
        if ( stopOngoingWarming )
        {
            return OptionalLong.empty();
        }
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
        return OptionalLong.of( pagesInMemory );
    }

    private long reheat( PagedFile file ) throws IOException
    {
        long pagesLoaded = 0;
        File savedProfile = profileOutputFileName( file );

        if ( !fs.fileExists( savedProfile ) )
        {
            return pagesLoaded;
        }

        // First read through the profile to verify its checksum.
        try ( InputStream inputStream = compressedInputStream( savedProfile ) )
        {
            int b;
            do
            {
                b = inputStream.read();
            }
            while ( b != -1 );
        }
        catch ( ZipException ignore )
        {
            // ZipException is used to indicate checksum failures.
            // Let's ignore this file since it's corrupt.
            return pagesLoaded;
        }

        // The file contents checks out. Let's load it in.
        try ( InputStream inputStream = compressedInputStream( savedProfile );
                PageLoader loader = pageLoaderFactory.getLoader( file ) )
        {
            long pageId = 0;
            int b;
            while ( (b = inputStream.read()) != -1 )
            {
                for ( int i = 0; i < 8; i++ )
                {
                    if ( stopOngoingWarming )
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

    private long profile( PagedFile file ) throws IOException
    {
        long pagesInMemory = 0;
        File outputFile = profileOutputFileName( file );

        try ( OutputStream outputStream = compressedOutputStream( outputFile );
              PageCursor cursor = file.io( 0, PF_SHARED_READ_LOCK | PF_NO_FAULT ) )
        {
            int stepper = 0;
            int b = 0;
            for ( ; ; )
            {
                if ( stopOngoingWarming )
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

        return pagesInMemory;
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

    private InputStream compressedInputStream( File input ) throws IOException
    {
        InputStream source = fs.openAsInputStream( input );
        try
        {
            return new GZIPInputStream( source );
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( source );
            throw new IOException( "Exception when building decompressor.", e );
        }
    }

    private OutputStream compressedOutputStream( File output ) throws IOException
    {
        fs.mkdirs( output.getParentFile() );
        StoreChannel channel = fs.open( output, OpenMode.READ_WRITE );
        ByteBuffer buf = ByteBuffer.allocate( 1 );
        OutputStream sink = new OutputStream()
        {
            @Override
            public void write( int b ) throws IOException
            {
                buf.put( (byte) b );
                buf.flip();
                channel.write( buf );
                buf.flip();
            }

            @Override
            public void close() throws IOException
            {
                channel.truncate( channel.position() );
                channel.close();
            }
        };
        try
        {
            return new GZIPOutputStream( sink );
        }
        catch ( IOException e )
        {
            // We close the channel instead of the sink here, because we don't want to truncate the file if we fail
            // to open the gzip output stream.
            IOUtils.closeAllSilently( channel );
            throw new IOException( "Exception when building compressor.", e );
        }
    }

    private File profileOutputFileName( PagedFile file )
    {
        File mappedFile = file.file();
        String profileOutputName = mappedFile.getName() + SUFFIX_CACHEPROF;
        File profileFolder = new File( mappedFile.getParentFile(), PROFILE_FOLDER );
        return new File( profileFolder, profileOutputName );
    }
}
