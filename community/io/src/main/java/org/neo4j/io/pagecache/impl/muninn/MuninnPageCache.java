/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.MemoryManager;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;
import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.getInteger;

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
    public static final byte ZERO_BYTE =
            (byte) (flag( MuninnPageCache.class, "brandedZeroByte", false ) ? 0x0f : 0);

    // Keep this many pages free and ready for use in faulting.
    // This will be truncated to be no more than half of the number of pages
    // in the cache.
    private static final int pagesToKeepFree = getInteger(
            MuninnPageCache.class, "pagesToKeepFree", 30 );

    // This is how many times that, during cooperative eviction, we'll iterate through the entire set of pages looking
    // for a page to evict, before we give up and throw CacheLiveLockException. This MUST be greater than 1.
    private static final int cooperativeEvictionLiveLockThreshold = getInteger(
            MuninnPageCache.class, "cooperativeEvictionLiveLockThreshold", 100 );

    // This is a pre-allocated constant, so we can throw it without allocating any objects:
    @SuppressWarnings( "ThrowableInstanceNeverThrown" )
    private static final IOException oomException = new IOException(
            "OutOfMemoryError encountered in the page cache background eviction thread" );

    // The field offset to unsafely access the freelist field.
    private static final long freelistOffset =
            UnsafeUtil.getFieldOffset( MuninnPageCache.class, "freelist" );

    // This is used as a poison-pill signal in the freelist, to inform any
    // page faulting thread that it is now no longer possible to queue up and
    // wait for more pages to be evicted, because the page cache has been shut
    // down.
    private static final FreePage shutdownSignal = new FreePage( null );

    // A counter used to identify which background threads belong to which page cache.
    private static final AtomicInteger pageCacheIdCounter = new AtomicInteger();

    // This Executor runs all the background threads for all page cache instances. It allows us to reuse threads
    // between multiple page cache instances, which is of no consequence in normal usage, but is quite useful for the
    // many, many tests that create and close page caches all the time. We DO NOT want to take an Executor in through
    // the constructor of the PageCache, because the Executors have too many configuration options, many of which are
    // highly troublesome for our use case; caller-runs, bounded submission queues, bounded thread count, non-daemon
    // thread factories, etc.
    private static final Executor backgroundThreadExecutor = BackgroundThreadExecutor.INSTANCE;

    private static final List<OpenOption> ignoredOpenOptions = Arrays.asList( (OpenOption) StandardOpenOption.APPEND,
            StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE );

    private final int pageCacheId;
    private final PageSwapperFactory swapperFactory;
    private final int cachePageSize;
    private final int keepFree;
    private final PageCacheTracer tracer;
    private final MuninnPage[] pages;
    // All PageCursors are initialised with their pointers pointing to the victim page. This way, we can do branch-free
    // bounds checking of page accesses without fear of segfaulting newly allocated cursors.
    final long victimPage;

    // The freelist is a thread-safe linked-list of 2 types of objects. A link
    // can either be a MuninnPage or a FreePage.
    // Initially, most of the links are MuninnPages that are ready for the
    // taking. Then towards the end, we have the last bunch of pages linked
    // through FreePage objects. We make this transition because, once a
    // MuninnPage has been removed from the list, it cannot be added back. The
    // reason is that the MuninnPages are reused, and adding them back into the
    // freelist would expose us to the ABA-problem, which can cause cycles to
    // form. The FreePage objects, however, are single-use such that they don't
    // exhibit the ABA-problem. In other words, eviction will never add
    // MuninnPages to the freelist; it will only add free pages through a new
    // FreePage object.
    @SuppressWarnings( "unused" ) // This field is accessed via Unsafe.
    private volatile Object freelist;

    // Linked list of mappings - guarded by synchronized(this)
    private volatile FileMapping mappedFiles;

    // The thread that runs the eviction algorithm. We unpark this when we've run out of
    // free pages to grab.
    private volatile Thread evictionThread;
    // True if the eviction thread is currently parked, without someone having
    // signalled it to wake up. This is used as a weak guard for unparking the
    // eviction thread, because calling unpark too much (from many page
    // faulting threads) can cause contention on the locks protecting that
    // threads scheduling meta-data in the OS kernel.
    private volatile boolean evictorParked;
    private volatile IOException evictorException;

    // Flag for when page cache is closed - writes guarded by synchronized(this), reads can be unsynchronized
    private volatile boolean closed;

    // Only used by ensureThreadsInitialised while holding the monitor lock on this MuninnPageCache instance.
    private boolean threadsInitialised;

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
        this.tracer = tracer;
        this.pages = new MuninnPage[maxPages];
        this.printExceptionsOnClose = true;

        long alignment = swapperFactory.getRequiredBufferAlignment();
        long expectedMaxMemory = ((long) maxPages) * cachePageSize; // cast to long prevents overflow
        MemoryManager memoryManager = new MemoryManager( expectedMaxMemory, alignment );
        this.victimPage = VictimPageReference.getVictimPage( cachePageSize );
        Object pageList = null;
        int pageIndex = maxPages;
        while ( pageIndex --> 0 )
        {
            MuninnPage page = new MuninnPage( cachePageSize, memoryManager );
            page.tryExclusiveLock(); // All pages in the free-list are exclusively locked, and unlocked by page fault.
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
    public synchronized PagedFile map( File file, int filePageSize, OpenOption... openOptions ) throws IOException
    {
        assertHealthy();
        ensureThreadsInitialised();
        if ( filePageSize > cachePageSize )
        {
            throw new IllegalArgumentException(
                    "Cannot map files with a filePageSize (" + filePageSize + ") that is greater than the " +
                    "cachePageSize (" + cachePageSize + ")" );
        }
        boolean createIfNotExists = false;
        boolean truncateExisting = false;
        boolean deleteOnClose = false;
        boolean exclusiveMapping = false;
        boolean anyPageSize = false;
        for ( OpenOption option : openOptions )
        {
            if ( option.equals( StandardOpenOption.CREATE ) )
            {
                createIfNotExists = true;
            }
            else if ( option.equals( StandardOpenOption.TRUNCATE_EXISTING ) )
            {
                truncateExisting = true;
            }
            else if ( option.equals( StandardOpenOption.DELETE_ON_CLOSE ) )
            {
                deleteOnClose = true;
            }
            else if ( option.equals( PageCacheOpenOptions.EXCLUSIVE ) )
            {
                exclusiveMapping = true;
            }
            else if ( option.equals( PageCacheOpenOptions.ANY_PAGE_SIZE ) )
            {
                anyPageSize = true;
            }
            else if ( !ignoredOpenOptions.contains( option ) )
            {
                throw new UnsupportedOperationException( "Unsupported OpenOption: " + option );
            }
        }

        FileMapping current = mappedFiles;

        // find an existing mapping
        while ( current != null )
        {
            if ( current.file.equals( file ) )
            {
                MuninnPagedFile pagedFile = current.pagedFile;
                if ( pagedFile.pageSize() != filePageSize && !anyPageSize )
                {
                    String msg = "Cannot map file " + file + " with " +
                            "filePageSize " + filePageSize + " bytes, " +
                            "because it has already been mapped with a " +
                            "filePageSize of " + pagedFile.pageSize() +
                            " bytes.";
                    throw new IllegalArgumentException( msg );
                }
                if ( truncateExisting )
                {
                    throw new UnsupportedOperationException( "Cannot truncate a file that is already mapped" );
                }
                if ( exclusiveMapping || pagedFile.isExclusiveMapping() )
                {
                    String msg;
                    if ( exclusiveMapping )
                    {
                        msg = "Cannot exclusively map file because it is already mapped: " + file;
                    }
                    else
                    {
                        msg = "Cannot map file because it is already exclusively mapped: " + file;
                    }
                    throw new IOException( msg );
                }
                pagedFile.incrementRefCount();
                pagedFile.markDeleteOnClose( deleteOnClose );
                return pagedFile;
            }
            current = current.next;
        }

        if ( filePageSize < Long.BYTES )
        {
            throw new IllegalArgumentException(
                    "Cannot map files with a filePageSize (" + filePageSize + ") that is less than " +
                    Long.BYTES + " bytes" );
        }

        // there was no existing mapping
        MuninnPagedFile pagedFile = new MuninnPagedFile(
                file,
                this,
                filePageSize,
                swapperFactory,
                tracer,
                createIfNotExists,
                truncateExisting,
                exclusiveMapping );
        pagedFile.incrementRefCount();
        pagedFile.markDeleteOnClose( deleteOnClose );
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
                file.flushAndForceForClose();
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
    public void flushAndForce() throws IOException
    {
        flushAndForce( IOLimiter.unlimited() );
    }

    @Override
    public synchronized void flushAndForce( IOLimiter limiter ) throws IOException
    {
        if ( limiter == null )
        {
            throw new IllegalArgumentException( "IOPSLimiter cannot be null" );
        }
        assertNotClosed();
        flushAllPages( limiter );
        clearEvictorException();
    }

    private void flushAllPages( IOLimiter limiter ) throws IOException
    {
        try ( MajorFlushEvent cacheFlush = tracer.beginCacheFlush() )
        {
            FlushEventOpportunity flushOpportunity = cacheFlush.flushEventOpportunity();
            FileMapping fileMapping = mappedFiles;
            while ( fileMapping != null )
            {
                fileMapping.pagedFile.flushAndForceInternal( flushOpportunity, false, limiter );
                fileMapping = fileMapping.next;
            }
            syncDevice();
        }
    }

    void syncDevice() throws IOException
    {
        swapperFactory.syncDevice();
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

    MuninnPage grabFreeAndExclusivelyLockedPage( PageFaultEvent faultEvent ) throws IOException
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
        // We can discover a null-pointer, in which case the freelist has just
        // been emptied for whatever it contained before. New FreePage objects
        // are eventually going to be added to the freelist, but we are not
        // going to wait around for that to happen. If the freelist is empty,
        // then we do our own eviction to get a free page.
        // If we find a FreePage object on the freelist, then it is important
        // to check and see if it is the shutdownSignal instance. If that's the
        // case, then the page cache has been shut down, and we should throw an
        // exception from our page fault routine.
        Object current;
        for (;;)
        {
            assertHealthy();
            current = getFreelistHead();
            if ( current == null )
            {
                unparkEvictor();
                return cooperativelyEvict( faultEvent );
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
                if ( freePage == shutdownSignal )
                {
                    throw new IllegalStateException( "The PageCache has been shut down." );
                }

                if ( compareAndSetFreelistHead( freePage, freePage.next ) )
                {
                    return freePage.page;
                }
            }
        }
    }

    private MuninnPage cooperativelyEvict( PageFaultEvent faultEvent ) throws IOException
    {
        int iterations = 0;
        int clockArm = ThreadLocalRandom.current().nextInt( pages.length );
        MuninnPage page;
        boolean evicted = false;
        do
        {
            assertHealthy();

            if ( clockArm == pages.length )
            {
                if ( iterations == cooperativeEvictionLiveLockThreshold )
                {
                    throw cooperativeEvictionLiveLock();
                }
                iterations++;
                clockArm = 0;
            }

            page = pages[clockArm];
            if ( page == null )
            {
                throw new IllegalStateException(
                        "The PageCache has been shut down" );
            }

            if ( page.isLoaded() && page.decrementUsage() )
            {
                if ( page.tryExclusiveLock() )
                {
                    // We got the write lock. Time to evict the page!
                    try ( EvictionEvent evictionEvent = faultEvent.beginEviction() )
                    {
                        evicted = page.isLoaded() && evictPage( page, evictionEvent );
                    }
                    finally
                    {
                        if ( !evicted )
                        {
                            page.unlockExclusive();
                        }
                    }
                }
            }
            clockArm++;
        }
        while ( !evicted );
        return page;
    }

    private CacheLiveLockException cooperativeEvictionLiveLock()
    {
        return new CacheLiveLockException(
                "Live-lock encountered when trying to cooperatively evict a page during page fault. " +
                "This happens when we want to access a page that is not in memory, so it has to be faulted in, but " +
                "there are no free memory pages available to accept the page fault, so we have to evict an existing " +
                "page, but all the in-memory pages are currently locked by other accesses. If those other access are " +
                "waiting for our page fault to make progress, then we have a live-lock, and the only way we can get " +
                "out of it is by throwing this exception. This should be extremely rare, but can happen if the page " +
                "cache size is tiny and the number of concurrently running transactions is very high. You should be " +
                "able to get around this problem by increasing the amount of memory allocated to the page cache " +
                "with the `dbms.memory.pagecache.size` setting. Please contact Neo4j support if you need help tuning " +
                "your database." );
    }

    private void unparkEvictor()
    {
        if ( evictorParked )
        {
            evictorParked = false;
            LockSupport.unpark( evictionThread );
        }
    }

    private void parkEvictor( long parkNanos )
    {
        // Only called from the background eviction thread!
        evictorParked = true;
        LockSupport.parkNanos( this, parkNanos );
        evictorParked = false;
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

        while ( !closed )
        {
            int pageCountToEvict = parkUntilEvictionRequired( keepFree );
            try ( EvictionRunEvent evictionRunEvent = tracer.beginPageEvictions( pageCountToEvict ) )
            {
                clockArm = evictPages( pageCountToEvict, clockArm, evictionRunEvent );
            }
        }

        // The last thing we do, is signalling the shutdown of the cache via
        // the freelist. This signal is looked out for in grabFreePage.
        getAndSetFreelistHead( shutdownSignal );
    }

    private int parkUntilEvictionRequired( int keepFree )
    {
        // Park until we're either interrupted, or the number of free pages drops
        // bellow keepFree.
        long parkNanos = TimeUnit.MILLISECONDS.toNanos( 10 );
        for (;;)
        {
            parkEvictor( parkNanos );
            if ( Thread.interrupted() || closed )
            {
                return 0;
            }

            Object freelistHead = getFreelistHead();

            if ( freelistHead == null )
            {
                return keepFree;
            }
            else if ( freelistHead.getClass() == FreePage.class )
            {
                int availablePages = ((FreePage) freelistHead).count;
                if ( availablePages < keepFree )
                {
                    return keepFree - availablePages;
                }
            }
        }
    }

    int evictPages( int pageCountToEvict, int clockArm, EvictionRunEvent evictionRunEvent )
    {
        while ( pageCountToEvict > 0 && !closed )
        {
            if ( clockArm == pages.length )
            {
                clockArm = 0;
            }
            MuninnPage page = pages[clockArm];

            if ( page == null )
            {
                // The page cache has been shut down.
                return 0;
            }

            if ( page.isLoaded() && page.decrementUsage() )
            {
                if ( page.tryExclusiveLock() )
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
                    boolean pageEvicted = false;

                    try ( EvictionEvent evictionEvent = evictionRunEvent.beginEviction() )
                    {
                        pageEvicted = page.isLoaded() && evictPage( page, evictionEvent );
                        if ( pageEvicted )
                        {
                            Object current;
                            Object nextListHead;
                            FreePage freePage = null;
                            do
                            {
                                current = getFreelistHead();
                                freePage = freePage == null?
                                           new FreePage( page ) : freePage;
                                freePage.setNext( (FreePage) current );
                                nextListHead = freePage;
                            }
                            while ( !compareAndSetFreelistHead( current, nextListHead ) );
                        }
                    }
                    finally
                    {
                        if ( !pageEvicted )
                        {
                            // Pages we put into the free-list remain exclusively locked until a page fault unlocks
                            // them. If we somehow failed to evict the page, then we need to make sure that we release
                            // the exclusive lock.
                            page.unlockExclusive();
                        }
                    }
                }
            }

            clockArm++;
        }

        return clockArm;
    }

    /**
     * Evict the given page, or return {@code false} if the eviction failed for any reason.
     * This method will never throw an exception!
     */
    private boolean evictPage( MuninnPage page, EvictionEvent evictionEvent )
    {
        //noinspection TryWithIdenticalCatches - this warning is a false positive; bug in Intellij inspection
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

    private void clearEvictorException()
    {
        if ( evictorException != null )
        {
            evictorException = null;
        }
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
