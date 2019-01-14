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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.memory.GlobalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.util.FeatureToggles.getInteger;

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

    // The amount of memory we need for every page, both its buffer and its meta-data.
    private static final int MEMORY_USE_PER_PAGE = PAGE_SIZE + PageList.META_DATA_BYTES_PER_PAGE;

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
    private static final FreePage shutdownSignal = new FreePage( 0 );

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
    private final PageCacheTracer pageCacheTracer;
    private final PageCursorTracerSupplier pageCursorTracerSupplier;
    private final VersionContextSupplier versionContextSupplier;
    final PageList pages;
    // All PageCursors are initialised with their pointers pointing to the victim page. This way, we don't have to throw
    // exceptions on bounds checking failures; we can instead return the victim page pointer, and permit the page
    // accesses to take place without fear of segfaulting newly allocated cursors.
    final long victimPage;

    // The freelist is a thread-safe linked-list of FreePage objects, or an AtomicInteger, or null.
    // Initially, the field is an AtomicInteger that counts from zero to the max page count, at which point all of the
    // pages have been put in use. Once this happens, the field is set to null to allow the background eviction thread
    // to start its work. From that point on, the field will operate as a concurrent stack of FreePage objects. The
    // eviction thread pushes newly freed FreePage objects onto the stack, and page faulting threads pops FreePage
    // objects from the stack. The FreePage objects are single-use, to avoid running into the ABA-problem.
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
    /**
     * Compute the amount of memory needed for a page cache with the given number of 8 KiB pages.
     * @param pageCount The number of pages
     * @return The memory required for the buffers and meta-data of the given number of pages
     */
    public static long memoryRequiredForPages( long pageCount )
    {
        return pageCount * MEMORY_USE_PER_PAGE;
    }

    /**
     * Create page cache.
     * @param swapperFactory page cache swapper factory
     * @param maxPages maximum number of pages
     * @param pageCacheTracer global page cache tracer
     * @param pageCursorTracerSupplier supplier of thread local (transaction local) page cursor tracer that will provide
     * thread local page cache statistics
     * @param versionContextSupplier supplier of thread local (transaction local) version context that will provide
     * access to thread local version context
     */
    public MuninnPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            PageCacheTracer pageCacheTracer,
            PageCursorTracerSupplier pageCursorTracerSupplier,
            VersionContextSupplier versionContextSupplier )
    {
        this( swapperFactory,
                // Cast to long prevents overflow:
                MemoryAllocator.createAllocator( "" + memoryRequiredForPages( maxPages ), GlobalMemoryTracker.INSTANCE ),
                PAGE_SIZE,
                pageCacheTracer,
                pageCursorTracerSupplier,
                versionContextSupplier );
    }

    /**
     * Create page cache.
     * @param swapperFactory page cache swapper factory
     * @param memoryAllocator the source of native memory the page cache should use
     * @param pageCacheTracer global page cache tracer
     * @param pageCursorTracerSupplier supplier of thread local (transaction local) page cursor tracer that will provide
     * thread local page cache statistics
     * @param versionContextSupplier supplier of thread local (transaction local) version context that will provide
     *        access to thread local version context
     */
    public MuninnPageCache(
            PageSwapperFactory swapperFactory,
            MemoryAllocator memoryAllocator,
            PageCacheTracer pageCacheTracer,
            PageCursorTracerSupplier pageCursorTracerSupplier,
            VersionContextSupplier versionContextSupplier )
    {
        this( swapperFactory, memoryAllocator, PAGE_SIZE, pageCacheTracer, pageCursorTracerSupplier, versionContextSupplier );
    }

    /**
     * Constructor variant that allows setting a non-standard cache page size.
     * Only ever use this for testing.
     */
    @SuppressWarnings( "DeprecatedIsStillUsed" )
    @Deprecated
    public MuninnPageCache(
            PageSwapperFactory swapperFactory,
            MemoryAllocator memoryAllocator,
            int cachePageSize,
            PageCacheTracer pageCacheTracer,
            PageCursorTracerSupplier pageCursorTracerSupplier,
            VersionContextSupplier versionContextSupplier )
    {
        verifyHacks();
        verifyCachePageSizeIsPowerOfTwo( cachePageSize );
        int maxPages = calculatePageCount( memoryAllocator, cachePageSize );

        // Expose the total number of pages
        pageCacheTracer.maxPages( maxPages );
        MemoryAllocationTracker memoryTracker = GlobalMemoryTracker.INSTANCE;

        this.pageCacheId = pageCacheIdCounter.incrementAndGet();
        this.swapperFactory = swapperFactory;
        this.cachePageSize = cachePageSize;
        this.keepFree = Math.min( pagesToKeepFree, maxPages / 2 );
        this.pageCacheTracer = pageCacheTracer;
        this.pageCursorTracerSupplier = pageCursorTracerSupplier;
        this.versionContextSupplier = versionContextSupplier;
        this.printExceptionsOnClose = true;
        long alignment = swapperFactory.getRequiredBufferAlignment();
        this.victimPage = VictimPageReference.getVictimPage( cachePageSize, memoryTracker );
        this.pages = new PageList( maxPages, cachePageSize, memoryAllocator, new SwapperSet(), victimPage, alignment );

        setFreelistHead( new AtomicInteger() );
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

    private static int calculatePageCount( MemoryAllocator memoryAllocator, int cachePageSize )
    {
        long memoryPerPage = cachePageSize + PageList.META_DATA_BYTES_PER_PAGE;
        long maxPages = memoryAllocator.availableMemory() / memoryPerPage;
        int minimumPageCount = 2;
        if ( maxPages < minimumPageCount )
        {
            throw new IllegalArgumentException( String.format(
                    "Page cache must have at least %s pages (%s bytes of memory), but was given %s pages.",
                    minimumPageCount, minimumPageCount * memoryPerPage, maxPages ) );
        }
        maxPages = Math.min( maxPages, PageList.MAX_PAGES );
        return Math.toIntExact( maxPages );
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
        file = file.getCanonicalFile();
        boolean createIfNotExists = false;
        boolean truncateExisting = false;
        boolean deleteOnClose = false;
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
                pageCacheTracer,
                pageCursorTracerSupplier,
                versionContextSupplier,
                createIfNotExists,
                truncateExisting );
        pagedFile.incrementRefCount();
        pagedFile.markDeleteOnClose( deleteOnClose );
        current = new FileMapping( file, pagedFile );
        current.next = mappedFiles;
        mappedFiles = current;
        pageCacheTracer.mappedFile( file );
        return pagedFile;
    }

    @Override
    public synchronized Optional<PagedFile> getExistingMapping( File file ) throws IOException
    {
        assertHealthy();
        ensureThreadsInitialised();

        file = file.getCanonicalFile();
        MuninnPagedFile pagedFile = tryGetMappingOrNull( file );
        if ( pagedFile != null )
        {
            pagedFile.incrementRefCount();
            return Optional.of( pagedFile );
        }
        return Optional.empty();
    }

    private MuninnPagedFile tryGetMappingOrNull( File file )
    {
        FileMapping current = mappedFiles;

        // find an existing mapping
        while ( current != null )
        {
            if ( current.file.equals( file ) )
            {
                return current.pagedFile;
            }
            current = current.next;
        }

        // no mapping exists
        return null;
    }

    @Override
    public synchronized List<PagedFile> listExistingMappings() throws IOException
    {
        assertNotClosed();
        ensureThreadsInitialised();

        List<PagedFile> list = new ArrayList<>();
        FileMapping current = mappedFiles;

        while ( current != null )
        {
            // Note that we are NOT incrementing the reference count here.
            // Calling code is expected to be able to deal with asynchronously closed PagedFiles.
            MuninnPagedFile pagedFile = current.pagedFile;
            list.add( pagedFile );
            current = current.next;
        }
        return list;
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
            catch ( Exception closeException )
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
                    pageCacheTracer.unmappedFile( current.file );
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
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        if ( limiter == null )
        {
            throw new IllegalArgumentException( "IOLimiter cannot be null" );
        }
        assertNotClosed();
        List<PagedFile> files = listExistingMappings();
        flushAllPages( files, limiter );
        clearEvictorException();
    }

    private void flushAllPages( List<PagedFile> files, IOLimiter limiter ) throws IOException
    {
        try ( MajorFlushEvent cacheFlush = pageCacheTracer.beginCacheFlush() )
        {
            for ( PagedFile file : files )
            {
                MuninnPagedFile muninnPagedFile = (MuninnPagedFile) file;
                try ( MajorFlushEvent fileFlush = pageCacheTracer.beginFileFlush( muninnPagedFile.swapper ) )
                {
                    FlushEventOpportunity flushOpportunity = fileFlush.flushEventOpportunity();
                    muninnPagedFile.flushAndForceInternal( flushOpportunity, false, limiter );
                }
                catch ( ClosedChannelException e )
                {
                    if ( muninnPagedFile.getRefCount() > 0 )
                    {
                        // The file is not supposed to be closed, since we have a positive ref-count, yet we got a
                        // ClosedChannelException anyway? It's an odd situation, so let's tell the outside world about
                        // this failure.
                        throw e;
                    }
                    // Otherwise: The file was closed while we were trying to flush it. Since unmapping implies a flush
                    // anyway, we can safely assume that this is not a problem. The file was flushed, and it doesn't
                    // really matter how that happened. We'll ignore this exception.
                }
            }
            syncDevice();
        }
    }

    void syncDevice()
    {
        swapperFactory.syncDevice();
    }

    @Override
    public synchronized void close()
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
                msg.append( files.file );
                msg.append( " (" ).append( refCount );
                msg.append( refCount == 1 ? " mapping)" : " mappings)" );
                files = files.next;
            }
            throw new IllegalStateException( msg.toString() );
        }

        closed = true;

        interrupt( evictionThread );
        evictionThread = null;

        // Close the page swapper factory last. If this fails then we will still consider ourselves closed.
        swapperFactory.close();
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
    public long maxCachedPages()
    {
        return pages.getPageCount();
    }

    @Override
    public FileSystemAbstraction getCachedFileSystem()
    {
        return swapperFactory.getFileSystemAbstraction();
    }

    @Override
    public void reportEvents()
    {
        pageCursorTracerSupplier.get().reportEvents();
    }

    @Override
    public boolean fileSystemSupportsFileOperations()
    {
        // Default filesystem supports direct file access.
        return getCachedFileSystem() instanceof DefaultFileSystemAbstraction;
    }

    int getPageCacheId()
    {
        return pageCacheId;
    }

    long grabFreeAndExclusivelyLockedPage( PageFaultEvent faultEvent ) throws IOException
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
                long pageRef = cooperativelyEvict( faultEvent );
                if ( pageRef != 0 )
                {
                    return pageRef;
                }
            }
            else if ( current instanceof AtomicInteger )
            {
                int pageCount = pages.getPageCount();
                AtomicInteger counter = (AtomicInteger) current;
                int pageId = counter.get();
                if ( pageId < pageCount && counter.compareAndSet( pageId, pageId + 1 ) )
                {
                    return pages.deref( pageId );
                }
                if ( pageId >= pageCount )
                {
                    compareAndSetFreelistHead( current, null );
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
                    return freePage.pageRef;
                }
            }
        }
    }

    private long cooperativelyEvict( PageFaultEvent faultEvent ) throws IOException
    {
        int iterations = 0;
        int pageCount = pages.getPageCount();
        int clockArm = ThreadLocalRandom.current().nextInt( pageCount );
        boolean evicted = false;
        long pageRef;
        do
        {
            assertHealthy();
            if ( getFreelistHead() != null )
            {
                return 0;
            }

            if ( clockArm == pageCount )
            {
                if ( iterations == cooperativeEvictionLiveLockThreshold )
                {
                    throw cooperativeEvictionLiveLock();
                }
                iterations++;
                clockArm = 0;
            }

            pageRef = pages.deref( clockArm );
            if ( pages.isLoaded( pageRef ) && pages.decrementUsage( pageRef ) )
            {
                evicted = pages.tryEvict( pageRef, faultEvent );
            }
            clockArm++;
        }
        while ( !evicted );
        return pageRef;
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

    private void setFreelistHead( Object newFreelistHead )
    {
        UnsafeUtil.putObjectVolatile( this, freelistOffset, newFreelistHead );
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
            try ( EvictionRunEvent evictionRunEvent = pageCacheTracer.beginPageEvictions( pageCountToEvict ) )
            {
                clockArm = evictPages( pageCountToEvict, clockArm, evictionRunEvent );
            }
        }

        // The last thing we do, is signalling the shutdown of the cache via
        // the freelist. This signal is looked out for in grabFreePage.
        setFreelistHead( shutdownSignal );
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
            else if ( freelistHead.getClass() == AtomicInteger.class )
            {
                AtomicInteger counter = (AtomicInteger) freelistHead;
                long count = pages.getPageCount() - counter.get();
                if ( count < keepFree )
                {
                    return count < 0 ? keepFree : (int) (keepFree - count);
                }
            }
        }
    }

    int evictPages( int pageCountToEvict, int clockArm, EvictionRunEvent evictionRunEvent )
    {
        while ( pageCountToEvict > 0 && !closed )
        {
            if ( clockArm == pages.getPageCount() )
            {
                clockArm = 0;
            }

            if ( closed )
            {
                // The page cache has been shut down.
                return 0;
            }

            long pageRef = pages.deref( clockArm );
            if ( pages.isLoaded( pageRef ) && pages.decrementUsage( pageRef ) )
            {
                try
                {
                    pageCountToEvict--;
                    if ( pages.tryEvict( pageRef, evictionRunEvent ) )
                    {
                        clearEvictorException();
                        addFreePageToFreelist( pageRef );
                    }
                }
                catch ( IOException e )
                {
                    evictorException = e;
                }
                catch ( OutOfMemoryError oom )
                {
                    evictorException = oomException;
                }
                catch ( Throwable th )
                {
                    evictorException = new IOException(
                            "Eviction thread encountered a problem", th );
                }
            }

            clockArm++;
        }

        return clockArm;
    }

    void addFreePageToFreelist( long pageRef )
    {
        Object current;
        FreePage freePage = new FreePage( pageRef );
        do
        {
            current = getFreelistHead();
            if ( current instanceof AtomicInteger && ((AtomicInteger) current).get() > pages.getPageCount() )
            {
                current = null;
            }
            freePage.setNext( current );
        }
        while ( !compareAndSetFreelistHead( current, freePage ) );
    }

    void clearEvictorException()
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
        for ( int i = 0; i < pages.getPageCount(); i++ )
        {
            sb.append( ' ' );
            pages.toString( pages.deref( i ), sb );
            sb.append( '\n' );
        }
        sb.append( ']' ).append( '\n' );
        return sb.toString();
    }

    void vacuum( SwapperSet swappers )
    {
        if ( getFreelistHead() instanceof AtomicInteger && swappers.countAvailableIds() > 200 )
        {
            return; // We probably still have plenty of free pages left. Don't bother vacuuming just yet.
        }
        swappers.vacuum( swapperIds ->
        {
            int pageCount = pages.getPageCount();
            try ( EvictionRunEvent evictions = pageCacheTracer.beginPageEvictions( 0 ) )
            {
                for ( int i = 0; i < pageCount; i++ )
                {
                    long pageRef = pages.deref( i );
                    while ( swapperIds.test( pages.getSwapperId( pageRef ) ) )
                    {
                        if ( pages.tryEvict( pageRef, evictions ) )
                        {
                            addFreePageToFreelist( pageRef );
                            break;
                        }
                    }
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }
}
