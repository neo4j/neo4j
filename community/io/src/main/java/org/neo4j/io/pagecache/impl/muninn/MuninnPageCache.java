/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

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
public class MuninnPageCache implements PageCache
{
    // The number of times we will spin, during page faulting, on checking the
    // freelist and LockSupport.unpark'ing the eviction thread, before blocking
    // and waiting for the eviction thread to do something.
    private static final int pageFaultSpinCount = Integer.getInteger(
            "org.neo4j.io.pagecache.impl.muninn.pageFaultSpinCount",
            FileUtils.OS_IS_WINDOWS? 10 : 1000 );

    // Keep this many pages free and ready for use in faulting.
    // This will be truncated to be no more than half of the number of pages
    // in the cache.
    private static final int pagesToKeepFree = Integer.getInteger(
            "org.neo4j.io.pagecache.impl.muninn.pagesToKeepFree", 30 );

    // The background flush task will only spend a certain amount of time doing IO, to avoid saturating the IO
    // subsystem during times when there is more important work to be done. It will do this by measuring how much
    // time it spends on each flush, and then accumulate a sleep debt. Once the sleep debt grows beyond this
    // threshold, the flush task will pay it back.
    private static final long backgroundFlushSleepDebtThreshold = Long.getLong(
            "org.neo4j.io.pagecache.impl.muninn.backgroundFlushSleepDebtThreshold", 10 );

    // This ratio determines how the background flush task will spend its time. Specifically, it is a ratio of how
    // much of its time will be spent doing IO. For instance, setting the ratio to 0.3 will make the flusher task
    // spend 30% of its time doing IO, and 70% of its time sleeping.
    private static final double backgroundFlushIoRatio = getDouble(
            "org.neo4j.io.pagecache.impl.muninn.backgroundFlushIoRatio", 0.1 );

    private static double getDouble( String property, double def )
    {
        try
        {
            return Double.parseDouble( System.getProperty( property ) );
        }
        catch ( Exception e )
        {
            return def;
        }
    }

    private static final long backgroundFlushBusyBreak = Long.getLong(
            "org.neo4j.io.pagecache.impl.muninn.backgroundFlushBusyBreak", 100 );
    private static final long backgroundFlushMediumBreak = Long.getLong(
            "org.neo4j.io.pagecache.impl.muninn.backgroundFlushMediumBreak", 200 );
    private static final long backgroundFlushLongBreak = Long.getLong(
            "org.neo4j.io.pagecache.impl.muninn.backgroundFlushLongBreak", 1000 );

    // This is a pre-allocated constant, so we can throw it without allocating any objects:
    private static final IOException oomException = new IOException(
            "OutOfMemoryError encountered in the page cache background eviction thread" );

    // The field offset to unsafely access the freelist field.
    private static final long freelistOffset =
            UnsafeUtil.getFieldOffset( MuninnPageCache.class, "freelist" );

    // This is used as a poison-pill signal in the freelist, to inform any
    // page faulting thread that it is now no longer possible to queue up and
    // wait for more pages to be evicted, because the page cache has been shut
    // down.
    private static final FreePageWaiter shutdownSignal = new FreePageWaiter();

    // A counter used to identify which background threads belong to which page cache.
    private static final AtomicInteger pageCacheIdCounter = new AtomicInteger();

    // This Executor runs all the background threads for all page cache instances. It allows us to reuse threads
    // between multiple page cache instances, which is of no consequence in normal usage, but is quite useful for the
    // many, many tests that create and close page caches all the time. We DO NOT want to take an Executor in through
    // the constructor of the PageCache, because the Executors have too many configuration options, many of which are
    // highly troublesome for our use case; caller-runs, bounded submission queues, bounded thread count, non-daemon
    // thread factories, etc.
    private static final Executor backgroundThreadExecutor = BackgroundThreadExecutor.INSTANCE;

    private final int pageCacheId;
    private final PageSwapperFactory swapperFactory;
    private final int cachePageSize;
    private final int keepFree;
    private final CursorPool cursorPool;
    private final PageCacheTracer tracer;
    private final MuninnPage[] pages;
    private final AtomicInteger backgroundFlushPauseRequests;

    // The freelist takes a bit of explanation. It is a thread-safe linked-list
    // of 3 types of objects. A link can either be a MuninnPage, a FreePage or
    // a FreePageWaiter.
    // Initially, most of the links are MuninnPages that are ready for the
    // taking. Then towards the end, we have the last bunch of pages linked
    // through FreePage objects. We make this transition because, once a
    // MuninnPage has been removed from the list, it cannot be added back. The
    // reason is that the MuninnPages are reused, and adding them back into the
    // freelist would expose us to the ABA-problem, which can cause cycles to
    // form. The FreePage and FreePageWaiter objects, however, are single-use
    // such that they don't exhibit the ABA-problem. In other words, eviction
    // will never add MuninnPages to the freelist; it will only add free pages
    // through a new FreePage object, or with direct transfer through a
    // FreePageWaiter. FreePageWaiters are added to the list by threads that
    // want a free page, but have discovered that the freelist is either empty,
    // or has another FreePageWaiter at the head.
    // This contraption basically gives us a "transfer stack" with some space
    // optimisations for the initial bulk of contents.
    @SuppressWarnings( "unused" ) // This field is accessed via Unsafe.
    private volatile Object freelist;

    // Linked list of mappings - guarded by synchronized(this)
    private volatile FileMapping mappedFiles;

    // The thread that runs the eviction algorithm. We unpark this when we've run out of
    // free pages to grab.
    private volatile Thread evictionThread;
    private volatile IOException evictorException;
    // The thread that does background flushing.
    private volatile Thread flushThread;

    // Flag for when page cache is closed - writes guarded by synchronized(this), reads can be unsynchronized
    private volatile boolean closed;

    // Only used by ensureThreadsInitialised while holding the monitor lock on this MuninnPageCache instance.
    private boolean threadsInitialised;

    // The accumulator for the flush task sleep debt. This is only accessed from the flush task.
    private long sleepDebtNanos;

    // 'true' (the default) if we should print any exceptions we get when unmapping a file.
    private boolean printExceptionsOnClose;

    public MuninnPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            int cachePageSize,
            PageCacheTracer tracer )
    {
        verifyHacks();
        verifyCachePageSizeIsPowerOfTwo( cachePageSize );
        verifyMinimumPageCount( maxPages, cachePageSize );

        this.pageCacheId = pageCacheIdCounter.incrementAndGet();
        this.swapperFactory = swapperFactory;
        this.cachePageSize = cachePageSize;
        this.keepFree = Math.min( pagesToKeepFree, maxPages / 2 );
        this.cursorPool = new CursorPool();
        this.tracer = tracer;
        this.pages = new MuninnPage[maxPages];
        this.backgroundFlushPauseRequests = new AtomicInteger();
        this.printExceptionsOnClose = true;

        MemoryReleaser memoryReleaser = new MemoryReleaser( maxPages );
        Object pageList = null;
        int pageIndex = maxPages;
        while ( pageIndex --> 0 )
        {
            MuninnPage page = new MuninnPage( cachePageSize, memoryReleaser );
            pages[pageIndex] = page;

            if ( pageList == null )
            {
                FreePage freePage = new FreePage( page );
                freePage.setNext( null );
                pageList = freePage;
            }
            else if ( pageList instanceof FreePage
                    && ((FreePage) pageList).count < keepFree )
            {
                FreePage freePage = new FreePage( page );
                freePage.setNext( (FreePage) pageList );
                pageList = freePage;
            }
            else
            {
                page.nextFree = pageList;
                pageList = page;
            }
        }
        UnsafeUtil.putObjectVolatile( this, freelistOffset, pageList );
    }

    private static void verifyHacks()
    {
        // Make sure that we have access to theUnsafe.
        UnsafeUtil.assertHasUnsafe();
    }

    private static void verifyCachePageSizeIsPowerOfTwo( int cachePageSize )
    {
        int exponent = 31 - Integer.numberOfLeadingZeros( cachePageSize );
        if ( 1 << exponent != cachePageSize )
        {
            throw new IllegalArgumentException(
                    "Cache page size must be a power of two, but was " + cachePageSize );
        }
    }

    private static void verifyMinimumPageCount( int maxPages, int cachePageSize )
    {
        int minimumPageCount = 2;
        if ( maxPages < minimumPageCount )
        {
            throw new IllegalArgumentException( String.format(
                    "Page cache must have at least %s pages (%s bytes of memory), but was given %s pages.",
                    minimumPageCount, minimumPageCount * cachePageSize, maxPages ) );
        }
    }

    @Override
    public synchronized PagedFile map( File file, int filePageSize ) throws IOException
    {
        assertHealthy();
        ensureThreadsInitialised();
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
                pagedFile.incrementRefCount();
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
                cursorPool,
                tracer );
        pagedFile.incrementRefCount();
        current = new FileMapping( file, pagedFile );
        current.next = mappedFiles;
        mappedFiles = current;
        tracer.mappedFile( file );
        return pagedFile;
    }

    /**
     * Note: Must be called while synchronizing on the MuninnPageCache instance.
     */
    private void ensureThreadsInitialised() throws IOException
    {
        if ( threadsInitialised )
        {
            return;
        }
        threadsInitialised = true;

        try
        {
            backgroundThreadExecutor.execute( new EvictionTask( this ) );
            backgroundThreadExecutor.execute( new FlushTask( this ) );
        }
        catch ( Exception e )
        {
            IOException exception = new IOException( e );
            try
            {
                close();
            }
            catch ( IOException closeException )
            {
                exception.addSuppressed( closeException );
            }
            throw exception;
        }
    }

    synchronized void unmap( MuninnPagedFile file )
    {
        if ( file.decrementRefCount() )
        {
            // This was the last reference!
            // Find and remove the existing mapping:
            FileMapping prev = null;
            FileMapping current = mappedFiles;

            while ( current != null )
            {
                if ( current.pagedFile == file )
                {
                    if ( prev == null )
                    {
                        mappedFiles = current.next;
                    }
                    else
                    {
                        prev.next = current.next;
                    }
                    tracer.unmappedFile( current.file );
                    flushAndCloseWithoutFail( file );
                    break;
                }
                prev = current;
                current = current.next;
            }
        }
    }

    private void flushAndCloseWithoutFail( MuninnPagedFile file )
    {
        boolean flushedAndClosed = false;
        boolean printedFirstException = false;
        do
        {
            try
            {
                file.flushAndForce();
                file.closeSwapper();
                flushedAndClosed = true;
            }
            catch ( IOException e )
            {
                if ( printExceptionsOnClose && !printedFirstException )
                {
                    printedFirstException = true;
                    try
                    {
                        e.printStackTrace();
                    }
                    catch ( Exception ignore )
                    {
                    }
                }
            }
        }
        while ( !flushedAndClosed );
    }

    public void setPrintExceptionsOnClose( boolean enabled )
    {
        this.printExceptionsOnClose = enabled;
    }

    @Override
    public synchronized void flushAndForce() throws IOException
    {
        assertNotClosed();
        flushAllPages();
        clearEvictorException();
    }

    private void flushAllPages() throws IOException
    {
        try ( MajorFlushEvent cacheFlush = tracer.beginCacheFlush() )
        {
            FlushEventOpportunity flushOpportunity = cacheFlush.flushEventOpportunity();
            FileMapping fileMapping = mappedFiles;
            while ( fileMapping != null )
            {
                fileMapping.pagedFile.flushAndForceInternal( flushOpportunity );
                fileMapping = fileMapping.next;
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

        FileMapping files = mappedFiles;
        if ( files != null )
        {
            StringBuilder msg = new StringBuilder(
                    "Cannot close the PageCache while files are still mapped:" );
            while ( files != null )
            {
                int refCount = files.pagedFile.getRefCount();
                msg.append( "\n\t" );
                msg.append( files.file.getName() );
                msg.append( " (" ).append( refCount );
                msg.append( refCount == 1? " mapping)" : " mappings)" );
                files = files.next;
            }
            throw new IllegalStateException( msg.toString() );
        }

        closed = true;

        for ( int i = 0; i < pages.length; i++ )
        {
            pages[i] = null;
        }

        interrupt( evictionThread );
        evictionThread = null;
        interrupt( flushThread );
        flushThread = null;
    }

    private void interrupt( Thread thread )
    {
        if ( thread != null )
        {
            thread.interrupt();
        }
    }

    @Override
    protected void finalize() throws Throwable
    {
        close();
        super.finalize();
    }

    private void assertHealthy() throws IOException
    {
        assertNotClosed();
        IOException exception = evictorException;
        if ( exception != null )
        {
            throw new IOException( "Exception in the page eviction thread", exception );
        }
    }

    private void assertNotClosed()
    {
        if ( closed )
        {
            throw new IllegalStateException( "The PageCache has been shut down" );
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

    int getPageCacheId()
    {
        return pageCacheId;
    }

    MuninnPage grabFreePage( PageFaultEvent faultEvent ) throws IOException
    {
        // Review the comment on the freelist field before making changes to
        // this part of the code.
        // Whatever the case, we're going to the head-pointer of the freelist,
        // and in doing so, we can discover a number of things.
        // We can discover a MuninnPage object, in which case we can try to
        // CAS the freelist pointer to the value of the MuninnPage.nextFree
        // pointer, and if this succeeds then we've grabbed that page.
        // We can discover a FreePage object, in which case we'll do a similar
        // dance by attempting to CAS the freelist to the FreePage objects next
        // pointer, and again, if we succeed then we've grabbed the MuninnPage
        // given by the FreePage object.
        // We can discover a FreePageWaiter object, which means that other
        // threads have already given up spinning on the freelist waiting for
        // free pages, so we might as well just get in line right away.
        // We can discover a null-pointer, in which case the freelist has just
        // been emptied for whatever it contained before. Either new FreePage
        // objects are going to be added to it, or another thread is going to
        // get tired of waiting and add a FreePageWaiter to it, or we are going
        // to get tired of waiting and add our own FreePageWaiter to it.
        // Importantly, this FreePageWaiter could be the shutdownSignal
        // instance, so we need to check for that and throw the appropriate
        // exception if that turns out to be the case.

        Object current;
        FreePageWaiter waiter = null;
        int iterationCount = 0;
        boolean shouldUnparkInSpin = true;
        for (;;)
        {
            assertHealthy();
            iterationCount++;
            current = getFreelistHead();
            if ( current == null && iterationCount > pageFaultSpinCount )
            {
                waiter = waiter == null? new FreePageWaiter() : waiter;
                // Make sure to null out the next pointer, in case the waiter
                // was created at a time where the current object was another
                // waiter.
                waiter.next = null;
                if ( compareAndSetFreelistHead( null, waiter ) )
                {
                    unparkEvictor();
                    faultEvent.setParked( true );
                    return waiter.park( this );
                }
            }
            else if ( current == null )
            {
                // Short-circuit the checks bellow for performance, because we
                // know that they will always fail when 'current' is null.
                if ( shouldUnparkInSpin )
                {
                    unparkEvictor();
                    shouldUnparkInSpin = false;
                }
                continue;
            }
            else if ( current instanceof MuninnPage )
            {
                MuninnPage page = (MuninnPage) current;
                if ( compareAndSetFreelistHead( page, page.nextFree ) )
                {
                    return page;
                }
            }
            else if ( current instanceof FreePage )
            {
                FreePage freePage = (FreePage) current;
                if ( compareAndSetFreelistHead( freePage, freePage.next ) )
                {
                    return freePage.page;
                }
            }
            else if ( current instanceof FreePageWaiter )
            {
                if ( current == shutdownSignal )
                {
                    throw new IllegalStateException(
                            "The PageCache has been shut down" );
                }

                waiter = waiter == null? new FreePageWaiter() : waiter;
                waiter.next = (FreePageWaiter) current;
                if ( compareAndSetFreelistHead( current, waiter ) )
                {
                    unparkEvictor();
                    faultEvent.setParked( true );
                    return waiter.park( this );
                }
            }
            unparkEvictor();
        }
    }

    private void unparkEvictor()
    {
        LockSupport.unpark( evictionThread );
    }

    private Object getFreelistHead()
    {
        return UnsafeUtil.getObjectVolatile( this, freelistOffset );
    }

    private boolean compareAndSetFreelistHead( Object expected, Object update )
    {
        return UnsafeUtil.compareAndSwapObject(
                this, freelistOffset, expected, update );
    }

    private Object getAndSetFreelistHead( Object newFreelistHead )
    {
        return UnsafeUtil.getAndSetObject(
                this, freelistOffset, newFreelistHead );
    }

    /**
     * Scan through all the pages, one by one, and decrement their usage stamps.
     * If a usage reaches zero, we try-write-locking it, and if we get that lock,
     * we evict the page. If we don't, we move on to the next page.
     * Once we have enough free pages, we park our thread. Page-faulting will
     * unpark our thread as needed.
     */
    void continuouslySweepPages()
    {
        evictionThread = Thread.currentThread();
        int clockArm = 0;

        while ( !Thread.interrupted() )
        {
            int pageCountToEvict = parkUntilEvictionRequired( keepFree );
            try ( EvictionRunEvent evictionRunEvent = tracer.beginPageEvictions( pageCountToEvict ) )
            {
                clockArm = evictPages( pageCountToEvict, clockArm, evictionRunEvent );
            }
        }

        // The last thing we do, is unparking any thread that might be waiting
        // for free pages in a page fault.
        // This can happen because files can be unmapped while their cursors
        // are in use.
        Object freelistHead = getAndSetFreelistHead( shutdownSignal );
        if ( freelistHead instanceof FreePageWaiter )
        {
            FreePageWaiter waiters = (FreePageWaiter) freelistHead;
            interruptAllWaiters( waiters );
        }
    }

    private void interruptAllWaiters( FreePageWaiter waiters )
    {
        while ( waiters != null )
        {
            waiters.unparkInterrupt();
            waiters = waiters.next;
        }
    }

    private int parkUntilEvictionRequired( int keepFree )
    {
        // Park until we're either interrupted, or the number of free pages drops
        // bellow keepFree.
        long parkNanos = TimeUnit.MILLISECONDS.toNanos( 10 );
        for (;;)
        {
            LockSupport.parkNanos( parkNanos );
            if ( Thread.currentThread().isInterrupted() || closed )
            {
                return 0;
            }

            Object freelistHead = getFreelistHead();

            if ( freelistHead instanceof FreePage )
            {
                int availablePages = ((FreePage) freelistHead).count;
                if ( availablePages < keepFree )
                {
                    return keepFree - availablePages;
                }
            }
            else if ( freelistHead instanceof FreePageWaiter )
            {
                return keepFree;
            }
        }
    }

    int evictPages( int pageCountToEvict, int clockArm, EvictionRunEvent evictionRunEvent )
    {
        FreePageWaiter waiters = grabFreePageWaitersIfAny();

        Thread currentThread = Thread.currentThread();
        while ( (pageCountToEvict > 0 || waiters != null) && !currentThread.isInterrupted() ) {
            if ( clockArm == pages.length )
            {
                clockArm = 0;
            }
            MuninnPage page = pages[clockArm];

            if ( page == null )
            {
                // The page cache has been shut down.
                currentThread.interrupt();
                interruptAllWaiters( waiters );
                return 0;
            }

            if ( page.isLoaded() && page.decrementUsage() )
            {
                long stamp = page.tryWriteLock();
                if ( stamp != 0 )
                {
                    // We got the lock.
                    // Assume that the eviction is going to succeed, so that we
                    // always make some kind of progress. This means that, if
                    // we have a temporary outage of the storage system, for
                    // instance if the drive is full, then we won't spin in
                    // this forever. Instead, we'll eventually make our way
                    // back out to the main loop, where we have a chance to
                    // sleep for a little while in `parkUntilEvictionRequired`.
                    // This reduces the CPU load and power usage in such a
                    // scenario.
                    pageCountToEvict--;
                    boolean pageEvicted;

                    try ( EvictionEvent evictionEvent = evictionRunEvent.beginEviction() )
                    {
                        pageEvicted = evictPage( page, evictionEvent );
                    }
                    finally
                    {
                        page.unlockWrite( stamp );
                    }

                    if ( pageEvicted )
                    {
                        if ( waiters != null )
                        {
                            waiters.unpark( page );
                            waiters = waiters.next;
                        }
                        else
                        {
                            Object current;
                            Object nextListHead;
                            FreePage freePage = null;
                            FreePageWaiter waiter;
                            do
                            {
                                waiter = null;
                                current = getFreelistHead();
                                if ( current == null || current instanceof FreePage )
                                {
                                    freePage = freePage == null?
                                            new FreePage( page ) : freePage;
                                    freePage.setNext( (FreePage) current );
                                    nextListHead = freePage;
                                }
                                else
                                {
                                    assert current instanceof FreePageWaiter :
                                            "Unexpected link type: " + current;
                                    waiter = (FreePageWaiter) current;
                                    nextListHead = waiter.next;
                                }
                            }
                            while ( !compareAndSetFreelistHead(
                                    current, nextListHead ) );
                            if ( waiter != null )
                            {
                                waiter.unpark( page );
                            }
                        }
                    }
                    else if ( waiters != null && evictorException != null )
                    {
                        waiters.unparkException( evictorException );
                        waiters = waiters.next;
                    }
                }
            }

            clockArm++;
        }

        // If we still have waiters left, then it means our eviction loop was
        // interrupted and that we are about to shut down.
        // We first have to unblock our waiters and let them know what's going
        // on, though.
        // New waiters can queue up while we are doing this, however, so we
        // must also take care of those waiters as the last thing we do before
        // the eviction thread finally terminates. And we must prevent new
        // waiters from queueing up.
        interruptAllWaiters( waiters );

        return clockArm;
    }

    private boolean evictPage( MuninnPage page, EvictionEvent evictionEvent )
    {
        try
        {
            page.evict( evictionEvent );
            clearEvictorException();
            return true;
        }
        catch ( IOException ioException )
        {
            evictorException = ioException;
            evictionEvent.threwException( ioException );
        }
        catch ( OutOfMemoryError ignore )
        {
            evictorException = oomException;
            evictionEvent.threwException( oomException );
        }
        catch ( Throwable throwable )
        {
            evictorException = new IOException(
                    "Eviction thread encountered a problem", throwable );
            evictionEvent.threwException( evictorException );
        }
        return false;
    }

    private FreePageWaiter grabFreePageWaitersIfAny()
    {
        Object freelistHead = getFreelistHead();
        if ( freelistHead instanceof FreePageWaiter )
        {
            FreePageWaiter waiters =
                    (FreePageWaiter) getAndSetFreelistHead( null );
            return reverse( waiters );
        }
        return null;
    }

    private FreePageWaiter reverse( FreePageWaiter waiters )
    {
        FreePageWaiter result = null;
        while ( waiters != null )
        {
            FreePageWaiter tail = waiters.next;
            waiters.next = result;
            result = waiters;
            waiters = tail;
        }
        return result;
    }

    private void clearEvictorException()
    {
        evictorException = null;
    }

    void pauseBackgroundFlushTask()
    {
        backgroundFlushPauseRequests.getAndIncrement();
    }

    void unpauseBackgroundFlushTask()
    {
        backgroundFlushPauseRequests.getAndDecrement();
        LockSupport.unpark( flushThread );
    }

    private void checkBackgroundFlushPause()
    {
        while ( backgroundFlushPauseRequests.get() > 0 )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
    }

    /**
     * Scan through all the pages, flushing the dirty ones. Aim to only spend at most 50% of its time doing IO, in an
     * effort to avoid saturating the IO subsystem or steal precious IO resources from more important work.
     */
    void continuouslyFlushPages()
    {
        Thread thread = Thread.currentThread();
        flushThread = thread;

        while ( !thread.isInterrupted() )
        {
            long iterationSleepMillis = flushAtIORatio( backgroundFlushIoRatio );
            if ( iterationSleepMillis > 0 )
            {
                LockSupport.parkNanos( this, TimeUnit.MILLISECONDS.toNanos( iterationSleepMillis ) );
                sleepDebtNanos = 0;
            }
        }
    }

    private long flushAtIORatio( double ratio )
    {
        Thread thread = Thread.currentThread();
        long sleepPaymentThreshold = TimeUnit.MILLISECONDS.toNanos( backgroundFlushSleepDebtThreshold );
        boolean seenDirtyPages = false;
        boolean flushedPages = false;
        double sleepFactor = (1 - ratio) / ratio;

        try ( MajorFlushEvent event = tracer.beginCacheFlush() )
        {
            for ( MuninnPage page : pages )
            {
                if ( page == null || thread.isInterrupted() )
                {
                    // Null pages means the page cache has been closed.
                    thread.interrupt();
                    return 0;
                }

                // The rate is the percentage of time that we want to spend doing IO. If the rate is 0.3, then we
                // want to spend 30% of our time doing IO. We would then spend the other 70% of the time just
                // sleeping. This means that for every IO we do, we measure how long it takes. We can then compute
                // the amount of time we need to sleep. Basically, if we spend 30 microseconds doing IO, then we need
                // to sleep for 70 microseconds, with the 0.3 ratio. To get the sleep time, we can divide the IO time
                // T by the ratio R, and then multiply the result by 1 - R. This is equivalent to (T/R) - T = S.
                // Then, because we don't want to sleep too frequently in too small intervals, we sum up our S's and
                // only sleep when we have collected a sleep debt of at least 10 milliseconds.
                // IO is not the only point of contention, however. Doing a flush also means that we have to take a
                // pessimistic read-lock on the page, and if we do this on a page that is very popular for writing,
                // then it can noticeably impact the performance of the database. Therefore, we check the dirtiness of
                // a given page under and *optimistic* read lock, and we also decrement the usage counter to avoid
                // aggressively flushing very popular pages. We need to carefully balance this, though, since we are
                // at risk of the mutator threads performing so many writes that we can't decrement the usage
                // counters fast enough to reach zero.

                // Skip the page if it is already write locked, or not dirty, or too popular.
                boolean thisPageIsDirty = false;
                if ( page.isWriteLocked() || !(thisPageIsDirty = page.isDirty()) || !page.decrementUsage() )
                {
                    seenDirtyPages |= thisPageIsDirty;
                    continue; // Continue looping to the next page.
                }

                long stamp = page.tryReadLock();
                if ( stamp != 0 )
                {
                    try
                    {
                        // Double-check that the page is still dirty. We could be racing with other flushing threads.
                        if ( !page.isDirty() )
                        {
                            continue; // Continue looping to the next page.
                        }

                        long startNanos = System.nanoTime();
                        page.flush( event.flushEventOpportunity() );
                        long elapsedNanos = System.nanoTime() - startNanos;

                        sleepDebtNanos += elapsedNanos * sleepFactor;
                        flushedPages = true;
                    }
                    catch ( Throwable ignore )
                    {
                        // The MuninnPage.flush method will keep the page dirty if flushing fails, and the eviction
                        // thread will eventually report the problem if its serious. Ergo, we can just ignore any and
                        // all exceptions, and move on to the next page. If we end up not getting anything done this
                        // iteration of flushAtIORatio, then that's fine too.
                    }
                    finally
                    {
                        page.unlockRead( stamp );
                    }
                }

                // Check if we've collected enough sleep debt, and if so, pay it back.
                if ( sleepDebtNanos > sleepPaymentThreshold )
                {
                    LockSupport.parkNanos( sleepDebtNanos );
                    sleepDebtNanos = 0;
                }

                // Check if we've been asked to pause, because another thread wants to focus on flushing.
                checkBackgroundFlushPause();
            }
        }

        // We return an amount of time, in milliseconds, that we want to wait before we do the next iteration. If we
        // have seen no dirty pages, then we can take a long break because the database is presumably not very busy
        // with writing. If we have seen dirty pages and flushed some, then we can take a medium break since we've
        // made some progress but we also need to keep up. If we have seen dirty pages and flushed none of them, then
        // we shouldn't take any break, since we are falling behind the mutator threads.
        return seenDirtyPages?
               flushedPages? backgroundFlushMediumBreak : backgroundFlushBusyBreak
                             : backgroundFlushLongBreak;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "MuninnPageCache[ \n" );
        for ( MuninnPage page : pages )
        {
            sb.append( ' ' ).append( page ).append( '\n' );
        }
        sb.append( ']' ).append( '\n' );
        return sb.toString();
    }
}
