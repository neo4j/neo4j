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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.common.SingleFilePageSwapperFactory;

/**
 * The Muninn {@link org.neo4j.io.pagecache.PageCache page cache} implementation.
 * <pre>
 *                                                                      ....
 *                                                                .;okKNWMUWN0ko,
 *        O'er Mithgarth Hugin and Munin both                   ;0WMUNINNMUNINNMUNOdc:.
 *        Each day set forth to fly;                          .OWMUNINNMUNI  00WMUNINNXko;.
 *        For Hugin I fear lest he come not home,            .KMUNINNMUNINNMWKKWMUNINNMUNIN0l.
 *        But for Munin my care is more.                    .KMUNINNMUNINNMUNINNWKkdlc:::::::'
 *                                                        .lXMUNINNMUNINNMUNINXo'
 *                                                    .,lONMUNINNMUNINNMUNINNk'
 *                                              .,cox0NMUNINNMUNINNMUNINNMUNI:
 *                                         .;dONMUNINNMUNINNMUNINNMUNINNMUNIN'
 *                                   .';okKWMUNINNMUNINNMUNINNMUNINNMUNINNMUx
 *                              .:dkKNWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNN'
 *                        .';lONMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNWl
 *                       .:okXWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNM0'
 *                   .,oONMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNo
 *             .';lx0NMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUN0'
 *          ;kKWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMWx'
 *        .,kWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMXd'
 *   .;lkKNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMNx;'
 *   .oNMUNINNMWNKOxoc;'';:cdkKNWMUNINNMUNINNMUNINNMUNINNMUNINWKx;'
 *    lkOkkdl:'´                `':lkWMUNINNMUNINNMUNINN0kdoc;'
 *                                  c0WMUNINNMUNINNMUWx'
 *                                   .;ccllllxNMUNIXo'
 *                                           lWMUWkK;   .
 *                                           OMUNK.dNdc,....
 *                                           cWMUNlkWWWO:cl;.
 *                                            ;kWO,....',,,.
 *                                              cNd
 *                                               :Nk.
 *                                                cWK,
 *                                             .,ccxXWd.
 *                                                   dWNkxkOdc::;.
 *                                                    cNNo:ldo:.
 *                                                     'xo.   ..
 * </pre>
 * <p>
 *     In Norse mythology, Huginn (from Old Norse "thought") and Muninn (Old Norse
 *     "memory" or "mind") are a pair of ravens that fly all over the world, Midgard,
 *     and bring information to the god Odin.
 * </p>
 * <p>
 *     This implementation of {@link org.neo4j.io.pagecache.PageCache} is optimised for
 *     configurations with large memory capacities and large stores, and uses sequence
 *     locks to make uncontended reads and writes fast.
 * </p>
 */
public class MuninnPageCache implements PageCache, Runnable
{
    // Keep this many pages free and ready for use in faulting.
    // This will be truncated to be no more than half of the number of pages
    // in the cache.
    private static final int pagesToKeepFree = Integer.getInteger(
            "org.neo4j.io.pagecache.impl.muninn.pagesToKeepFree", 30 );

    private final PageSwapperFactory swapperFactory;
    private final int cachePageSize;
    private final PageCacheMonitor monitor;
    final MuninnPage[] pages;

    // Linked list of free pages
    private final AtomicReference<MuninnPage> freelist;

    // Linked list of mappings - guarded by synchronized(this)
    private volatile FileMapping mappedFiles;

    // The thread that runs the eviction algorithm. We unpark this when we've run out of
    // free pages to grab.
    private volatile Thread evictorThread;
    private volatile IOException evictorException;

    // Flag for when page cache is closed - guarded by synchronized(this)
    private boolean closed;

    public MuninnPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor )
    {
        this( new SingleFilePageSwapperFactory( fs ), maxPages, pageSize, monitor );
    }

    public MuninnPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            int cachePageSize,
            PageCacheMonitor monitor )
    {
        verifyHacks();

        this.swapperFactory = swapperFactory;
        this.cachePageSize = cachePageSize;
        this.pages = new MuninnPage[maxPages];
        this.monitor = monitor;

        MuninnPage pageList = null;
        int cachePageId = maxPages;
        while ( cachePageId --> 0 )
        {
            MuninnPage page = new MuninnPage( cachePageId, cachePageSize );
            pages[cachePageId] = page;
            page.nextFree = pageList;
            pageList = page;
        }
        freelist = new AtomicReference<>( pageList );
    }

    static void verifyHacks()
    {
        // Make sure that we can do unaligned get* and put*
        // See java.nio.Bits.unaligned().
        String arch = System.getProperty( "os.arch", "?" );
        if ( !arch.equals( "x86_64" ) && !arch.equals( "i386" )
                && !arch.equals( "x86" ) && !arch.equals( "amd64" ) )
        {
            throw new AssertionError(
                    "MuninnPageCache cannot be guaranteed to work on CPU architecture '" + arch + "' " +
                            "where support for unaligned word access is unknown." );
        }

        // Make sure that we have access to theUnsafe.
        if ( !UnsafeUtil.hasUnsafe() )
        {
            throw new AssertionError( "MuninnPageCache requires access to sun.misc.Unsafe" );
        }

        // Make sure that we are running Java 7, because our backported
        // StampedLock relies on undocumented memory fence side-effects of
        // Unsafe.getXVolatile, and these might go away in Java 8 and beyond.
        // When we migrate to Java 8, remove this check, along with the
        // org.neo4j.io.pagecache.impl.muninn.jsr166e.StampedLock and its test.
        // Also remember to remove it from the licensing-requirements-base.xml.
        if ( !System.getProperty( "java.specification.version" ).equals( "1.7" ) )
        {
            throw new AssertionError( "MuninnPageCache only supports Java 7." );
        }
    }

    @Override
    public synchronized PagedFile map( File file, int filePageSize ) throws IOException
    {
        assertHealthy();
        if ( filePageSize > cachePageSize )
        {
            throw new IllegalArgumentException( "Cannot map files with a filePageSize (" +
                    filePageSize + ") that is greater than the cachePageSize (" +
                    cachePageSize + ")" );
        }

        FileMapping current = mappedFiles;

        // find an existing mapping
        while ( current != null )
        {
            if ( current.file.equals( file ) )
            {
                MuninnPagedFile pagedFile = current.pagedFile;
                if ( pagedFile.pageSize() != filePageSize )
                {
                    String msg = "Cannot map file " + file + " with " +
                            "filePageSize " + filePageSize + " bytes, " +
                            "because it has already been mapped with a " +
                            "filePageSize of " + pagedFile.pageSize() +
                            " bytes.";
                    throw new IllegalArgumentException( msg );
                }
                pagedFile.incrementReferences();
                return pagedFile;
            }
            current = current.next;
        }

        // there was no existing mapping
        MuninnPagedFile pagedFile = new MuninnPagedFile(
                file,
                this,
                filePageSize,
                swapperFactory,
                freelist,
                monitor );
        current = new FileMapping( file, pagedFile );
        current.next = mappedFiles;
        mappedFiles = current;
        return pagedFile;
    }

    @Override
    public synchronized void unmap( File file ) throws IOException
    {
        assertHealthy();
        FileMapping prev = null;
        FileMapping current = mappedFiles;

        // find an existing mapping
        while ( current != null )
        {
            if ( current.file.equals( file ) )
            {
                MuninnPagedFile pagedFile = current.pagedFile;
                if ( pagedFile.decrementReferences() )
                {
                    // this was the last reference; boot it from the list
                    if ( prev == null )
                    {
                        mappedFiles = current.next;
                    }
                    else
                    {
                        prev.next = current.next;
                    }
                    pagedFile.close();
                }
                break;
            }
            prev = current;
            current = current.next;
        }
    }

    @Override
    public synchronized void flush() throws IOException
    {
        assertHealthy();
        flushAllPages();
    }

    private void flushAllPages() throws IOException
    {
        for ( int i = 0; i < pages.length; i++ )
        {
            MuninnPage page = pages[i];
            long stamp = page.writeLock();
            try
            {
                page.flush();
            }
            finally
            {
                page.unlockWrite( stamp );
            }
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        if ( closed )
        {
            return;
        }

        closed = true;
        FileMapping file = mappedFiles;
        while ( file != null )
        {
            file.pagedFile.close();
            file = file.next;
        }

        for ( int i = 0; i < pages.length; i++ )
        {
            pages[i].freeBuffer();
        }
    }

    private void assertHealthy() throws IOException
    {
        if ( closed )
        {
            throw new IllegalStateException( "The PageCache has been shut down" );
        }
        IOException exception = evictorException;
        if ( exception != null )
        {
            throw new IOException( "The page eviction thread is dead.", exception );
        }
    }

    @Override
    public int pageSize()
    {
        return cachePageSize;
    }

    @Override
    public int maxCachedPages()
    {
        return pages.length;
    }

    void unparkEvictor()
    {
        Thread thread = evictorThread;
        if ( thread != null )
        {
            LockSupport.unpark( thread );
        }
    }

    /**
     * Runs the eviction algorithm. Must be run in a dedicated thread.
     */
    @Override
    public void run()
    {
        // We scan through all the pages, one by one, and decrement their usage stamps.
        // If a usage reaches zero, we try-write-locking it, and if we get that lock,
        // we evict the page. If we don't, we move on to the next page.
        // Once we have enough free pages, we park our thread. Page-faulting will
        // unpark our thread as needed.
        evictorThread = Thread.currentThread();
        try
        {
            continuouslySweepPages();
        }
        catch ( IOException e )
        {
            evictorException = e;
        }
    }

    private void continuouslySweepPages() throws IOException
    {
        int keepFree = Math.min( pagesToKeepFree, pages.length / 2 );
        int clockArm = 0;

        while ( !Thread.interrupted() )
        {
            int pageCountToEvict = parkUntilEvictionRequired( keepFree );
            clockArm = evictPages( pageCountToEvict, clockArm );
        }
    }

    private int parkUntilEvictionRequired( int keepFree )
    {
        // Park until we're either interrupted, or the number of free pages drops
        // bellow keepFree.
        long parkNanos = TimeUnit.MILLISECONDS.toNanos( 10 );
        int availablePages;
        do
        {
            LockSupport.parkNanos( parkNanos );
            if ( Thread.currentThread().isInterrupted() )
            {
                return 0;
            }
            availablePages = 0;
            MuninnPage page = freelist.get();
            while ( page != null && availablePages < keepFree )
            {
                availablePages++;
                page = page.nextFree;
            }
        } while ( availablePages == keepFree );
        return keepFree - availablePages;
    }

    int evictPages( int pageCountToEvict, int clockArm ) throws IOException
    {
        while ( pageCountToEvict > 0 ) {
            if ( clockArm == pages.length )
            {
                clockArm = 0;
            }
            MuninnPage page = pages[clockArm];

            if ( page.isLoaded() && page.decrementUsage() )
            {
                long stamp = page.tryWriteLock();
                if ( stamp != 0 )
                {
                    // We got the lock.
                    // We have to grab the swapper and the filePageId, because
                    // we cannot do the onEviction notification while holding
                    // the lock on the page. The reason is that the
                    // notification will take a lock on the translation table,
                    // and that is the wrong lock order. We must always first
                    // lock on the translation table, and then on the page.
                    // Never the other way around. Otherwise we risk
                    // dead-locking
                    PageSwapper swapper = page.getSwapper();
                    long filePageId = page.getFilePageId();
                    try
                    {
                        page.evict();
                        pageCountToEvict--;
                    }
                    finally
                    {
                        page.unlockWrite( stamp );
                    }
                    swapper.evicted( filePageId );
                    monitor.evict( filePageId, swapper );

                    MuninnPage next;
                    do
                    {
                        next = freelist.get();
                        page.nextFree = next;
                    }
                    while ( !freelist.compareAndSet( next, page ) );
                }
            }

            clockArm++;
        }
        return clockArm;
    }
}
