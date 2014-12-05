/**
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.monitoring.EvictionEvent;
import org.neo4j.io.pagecache.monitoring.EvictionRunEvent;
import org.neo4j.io.pagecache.monitoring.MajorFlushEvent;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.RunnablePageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.monitoring.PageFaultEvent;

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
public class MuninnPageCache implements RunnablePageCache
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

    private final PageSwapperFactory swapperFactory;
    private final int cachePageSize;
    private final int keepFree;
    private final MuninnCursorPool cursorPool;
    private final PageCacheMonitor monitor;
    private final MuninnPage[] pages;

    // This holds a reference to the next FreePage object that will be used to
    // release an evicted page back into the freelist.
    // This exists to make sure that at least one page release can always
    // succeed, even when we have completely exhausted the heap space of the
    // JVM, such that any allocation would throw an OutOfMemoryError.
    // After the initialisation in the constructor, this field will only ever
    // be accessed by the eviction thread.
    private FreePage nextReleasableFreePage;

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
    // This field is accessed via Unsafe.
    private volatile Object freelist;

    // Linked list of mappings - guarded by synchronized(this)
    private volatile FileMapping mappedFiles;

    // The thread that runs the eviction algorithm. We unpark this when we've run out of
    // free pages to grab.
    private volatile Thread evictorThread;
    private volatile IOException evictorException;

    // Flag for when page cache is closed - writes guarded by synchronized(this), reads can be unsynchronized
    private volatile boolean closed;

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
        this.keepFree = Math.min( pagesToKeepFree, maxPages / 2 );
        this.cursorPool = new MuninnCursorPool();
        this.monitor = monitor;
        this.pages = new MuninnPage[maxPages];
        this.nextReleasableFreePage = new FreePage( null );

        MemoryReleaser memoryReleaser = new MemoryReleaser( maxPages );
        Object pageList = null;
        int cachePageId = maxPages;
        while ( cachePageId --> 0 )
        {
            MuninnPage page = new MuninnPage( cachePageSize, cachePageId, memoryReleaser );
            pages[cachePageId] = page;

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

        // Note that we are relying on the fact that submitting a Runnable to
        // an Executor, and starting a thread, are both synchronising actions,
        // and thus ensure safe publication of this object instance.
    }

    static void verifyHacks()
    {
        // Make sure that we have access to theUnsafe.
        if ( !UnsafeUtil.hasUnsafe() )
        {
            throw new AssertionError( "MuninnPageCache requires access to sun.misc.Unsafe" );
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
                monitor );
        pagedFile.incrementRefCount();
        current = new FileMapping( file, pagedFile );
        current.next = mappedFiles;
        mappedFiles = current;
        monitor.mappedFile( file );
        return pagedFile;
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
                    monitor.unmappedFile( current.file );
                    // We have already committed to unmapping the file, so we
                    // have to make sure all changes are flushed to storage.
                    // This is important because once all files are unmapped,
                    // the page cache can be closed. And when that happens,
                    // there will no longer be any way for dirty pages to have
                    // their changes made durable.
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
                file.flush();
                file.closeSwapper();
                flushedAndClosed = true;
            }
            catch ( Throwable e )
            {
                try
                {
                    if ( !printedFirstException )
                    {
                        // We don't have access to the logging subsystem in
                        // neo4j-io, so we just print the stack trace here and
                        // hope for the best.
                        e.printStackTrace();
                        printedFirstException = true;
                    }
                    else
                    {
                        // It could be the case that the storage device is
                        // full. Such a problem can take a long time to
                        // resolve, so we might as well take it easy.
                        sleepBeforeRetryingFailedFlush();
                    }
                }
                catch ( Throwable ignore )
                {
                }
            }
        }
        while ( !flushedAndClosed );
    }

    private void sleepBeforeRetryingFailedFlush() throws InterruptedException
    {
        // This is in a nicely named method of its own, in case it shows up in
        // a profiler.
        Thread.sleep( 250 );
    }

    @Override
    public synchronized void flush() throws IOException
    {
        assertNotClosed();
        flushAllPages();
        clearEvictorException();
    }

    private void flushAllPages() throws IOException
    {
        try ( MajorFlushEvent cacheFlush = monitor.beginCacheFlush() )
        {
            for ( MuninnPage page : pages )
            {
                long stamp = page.writeLock();
                try
                {
                    page.flush( cacheFlush.flushEventOpportunity() );
                }
                finally
                {
                    page.unlockWrite( stamp );
                }
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

    MuninnPage grabFreePage( PageFaultEvent faultEvent ) throws IOException
    {
        // This method is executed by threads that are in the process of page-
        // faulting, and want a fresh new page to fault into.
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
        LockSupport.unpark( evictorThread );
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
        continuouslySweepPages();
    }

    private void continuouslySweepPages()
    {
        int clockArm = 0;

        try
        {
            while ( !Thread.interrupted() )
            {
                int pageCountToEvict = parkUntilEvictionRequired( keepFree );
                try ( EvictionRunEvent evictionRunEvent = monitor.beginPageEvictions( pageCountToEvict ) )
                {
                    clockArm = evictPages( pageCountToEvict, clockArm, evictionRunEvent );
                }
            }
        }
        finally
        {
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

            // Make sure to null out this reference, as we might otherwise have
            // a transitive reference to the MemoryReleaser, which would
            // prevent our native pointers from being freed.
            nextReleasableFreePage = null;
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
        FreePageWaiter waiters = grabFreePageWaitersIfAny(); // allocation free

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
                try
                {
                    // This might not be allocation free if we have a
                    // SecurityManager configured, but that's okay because
                    // we're shutting down anyway.
                    currentThread.interrupt();
                }
                finally
                {
                    interruptAllWaiters( waiters );
                }
                return 0;
            }

            if ( page.isLoaded() && page.decrementUsage() )
            {
                long stamp = page.tryWriteLock(); // tryWriteLock is allocation free
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
                        page.unlockWrite( stamp ); // Allocation free
                    }

                    if ( pageEvicted )
                    {
                        // releaseEvictedPage() is not allocation free, but
                        // any OOMEs thrown are taken care of for us.
                        waiters = releaseEvictedPage( page, waiters );
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
        // Note: this method is expected to handle all the failures that can
        // occur during eviction, including OutOfMemoryErrors, and is therefore
        // itself not allowed to throw anything.
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
            try
            {
                evictorException = new IOException(
                        "Eviction thread encountered a problem", throwable );
            }
            catch ( OutOfMemoryError ignore )
            {
                evictorException = oomException;
            }
            evictionEvent.threwException( evictorException );
        }
        return false;
    }

    private FreePageWaiter releaseEvictedPage(
            MuninnPage page, FreePageWaiter waiters )
    {
        // Note: this method is called at a sensitive time; we are outside of
        // the catch clauses that protect the eviction itself, we are at a
        // point where we have already evicted a page but not yet made it
        // available to anyone, we'll leak the page if we fail to make it
        // available, we risk crashing the eviction thread if we throw an
        // exception.
        // In other words, we have to be pretty careful about our failure
        // handling. We are not allowed to *not* succeed in releasing the given
        // page, and we are not allowed to throw any exception. Not even
        // OutOfMemoryError.
        if ( waiters != null )
        {
            waiters.unpark( page );
            return waiters.next;
        }
        Object current;
        Object nextListHead;
        FreePageWaiter waiter;
        do
        {
            waiter = null;
            current = getFreelistHead();
            if ( current == null || current instanceof FreePage )
            {
                nextReleasableFreePage.page = page;
                nextReleasableFreePage.setNext( (FreePage) current );
                nextListHead = nextReleasableFreePage;
            }
            else
            {
                waiter = (FreePageWaiter) current;
                nextListHead = waiter.next;
            }
        }
        while ( !compareAndSetFreelistHead( current, nextListHead ) );

        if ( waiter != null )
        {
            waiter.unpark( page );
        }
        else
        {
            // If there's no waiter to unpark, then that means we've spent the
            // nextReleasableFreePage instance, and we must therefor prepare a
            // new one for the next iteration.
            // We absolutely cannot progress until this succeeds.
            prepareNextReleasableFreePage();
        }
        return waiters;
    }

    private void prepareNextReleasableFreePage()
    {
        boolean allocated = false;
        do
        {
            // 'Tis a lesson you should heed:
            // Try, try, try again.
            // If at first you don't succeed,
            // Try, try, try again.
            try
            {
                nextReleasableFreePage = new FreePage( null );
                allocated = true;
            }
            catch ( OutOfMemoryError ignore )
            {
                handleOutOfMemoryInNextReleasableFreePagePreparation();
            }
        }
        while ( !allocated );
    }

    private void handleOutOfMemoryInNextReleasableFreePagePreparation()
    {
        // This is in a separate method because it is so rare, and we'd like it
        // to not count against the inlining budget for the
        // prepareNextReleasableFreePage method.
        evictorException = oomException;
        // Also try clearing out some scraps of memory, if possible
        FreePageWaiter waiter = grabFreePageWaitersIfAny();
        while ( waiter != null )
        {
            waiter.unparkException( oomException );
            // Sadly, we have to kill all of them, because once we grab
            // the stack, we can't put it back. We'd have ABA issues on
            // our hands if we tried.
            waiter = waiter.next;
        }
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
