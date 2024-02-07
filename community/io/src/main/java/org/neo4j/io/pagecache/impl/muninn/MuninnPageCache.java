/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.impl.muninn;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;
import static org.neo4j.io.pagecache.buffer.IOBufferFactory.DISABLED_BUFFER_FACTORY;
import static org.neo4j.io.pagecache.impl.muninn.PageList.getPageHorizon;
import static org.neo4j.scheduler.Group.FILE_IO_HELPER;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.util.FeatureToggles.getInteger;
import static org.neo4j.util.Preconditions.requireNonNegative;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

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
public class MuninnPageCache implements PageCache {
    public static final byte ZERO_BYTE = (byte) (flag(MuninnPageCache.class, "brandedZeroByte", false) ? 0x0f : 0);

    // The amount of memory we need for every page, both its buffer and its meta-data.
    private static final int MEMORY_USE_PER_PAGE = PAGE_SIZE + PageList.META_DATA_BYTES_PER_PAGE;

    // Keep this many pages free and ready for use in faulting.
    // This will be truncated to be no more than half of the number of pages
    // in the cache.
    private static final int percentPagesToKeepFree = getInteger(MuninnPageCache.class, "percentPagesToKeepFree", 5);

    // This is how many times that, during cooperative eviction, we'll iterate through the entire set of pages looking
    // for a page to evict, before we give up and throw CacheLiveLockException. This MUST be greater than 1.
    private static final int cooperativeEvictionLiveLockThreshold =
            getInteger(MuninnPageCache.class, "cooperativeEvictionLiveLockThreshold", 100);

    // This is a pre-allocated constant, so we can throw it without allocating any objects:
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final IOException oomException =
            new IOException("OutOfMemoryError encountered in the page cache background eviction thread");

    // This is used as a poison-pill signal in the freelist, to inform any
    // page faulting thread that it is now no longer possible to queue up and
    // wait for more pages to be evicted, because the page cache has been shut
    // down.
    private static final FreePage shutdownSignal = new FreePage(0);

    // A counter used to identify which background threads belong to which page cache.
    private static final AtomicInteger pageCacheIdCounter = new AtomicInteger();

    // Scheduler that runs all the background jobs for page cache.
    private final JobScheduler scheduler;
    private final SystemNanoClock clock;

    private static final List<OpenOption> ignoredOpenOptions = Arrays.asList(
            StandardOpenOption.APPEND, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE);

    // Used when trying to figure out number of available pages in a page cache. Could be returned from
    // tryGetNumberOfAvailablePages.
    private static final int UNKNOWN_PAGES_TO_EVICT = -1;

    private final int pageCacheId;
    private final PageSwapperFactory swapperFactory;
    private final int cachePageSize;
    private final int pageReservedBytes;
    private final int keepFree;
    private final PageCacheTracer pageCacheTracer;
    private final IOBufferFactory bufferFactory;
    private final int faultLockStriping;
    private final boolean preallocateStoreFiles;
    private final boolean enableEvictionThread;
    private final MemoryAllocator memoryAllocator;
    private final boolean closeAllocatorOnShutdown;
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
    @SuppressWarnings("unused") // accessed via VarHandle.
    private volatile Object freelist;

    private static final VarHandle FREE_LIST;

    private final ConcurrentHashMap<String, MuninnPagedFile> mappedFiles;

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

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            FREE_LIST = l.findVarHandle(MuninnPageCache.class, "freelist", Object.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Compute the amount of memory needed for a page cache with the given number of 8 KiB pages.
     * @param pageCount The number of pages
     * @return The memory required for the buffers and meta-data of the given number of pages
     */
    public static long memoryRequiredForPages(long pageCount) {
        return pageCount * MEMORY_USE_PER_PAGE;
    }

    public static class Configuration {
        private final MemoryAllocator memoryAllocator;
        private final SystemNanoClock clock;
        private final MemoryTracker memoryTracker;
        private final PageCacheTracer pageCacheTracer;
        private final int pageSize;
        private final IOBufferFactory bufferFactory;
        private final int faultLockStriping;
        private final boolean enableEvictionThread;
        private final boolean preallocateStoreFiles;
        private final int reservedPageSize;
        private final boolean closeAllocatorOnShutdown;

        private Configuration(
                MemoryAllocator memoryAllocator,
                SystemNanoClock clock,
                MemoryTracker memoryTracker,
                PageCacheTracer pageCacheTracer,
                int pageSize,
                IOBufferFactory bufferFactory,
                int faultLockStriping,
                boolean enableEvictionThread,
                boolean preallocateStoreFiles,
                int reservedPageSize,
                boolean closeAllocatorOnShutdown) {
            this.memoryAllocator = memoryAllocator;
            this.clock = clock;
            this.memoryTracker = memoryTracker;
            this.pageCacheTracer = pageCacheTracer;
            this.pageSize = pageSize;
            this.reservedPageSize = reservedPageSize;
            this.bufferFactory = bufferFactory;
            this.faultLockStriping = faultLockStriping;
            this.enableEvictionThread = enableEvictionThread;
            this.preallocateStoreFiles = preallocateStoreFiles;
            this.closeAllocatorOnShutdown = closeAllocatorOnShutdown;
        }

        /**
         * @param memoryAllocator the source of native memory the page cache should use
         */
        public Configuration memoryAllocator(MemoryAllocator memoryAllocator) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * @param clock {@link SystemNanoClock} to use for internal time keeping
         */
        public Configuration clock(SystemNanoClock clock) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * @param memoryTracker underlying buffers allocation memory tracker
         */
        public Configuration memoryTracker(MemoryTracker memoryTracker) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * @param pageCacheTracer global page cache tracer
         */
        public Configuration pageCacheTracer(PageCacheTracer pageCacheTracer) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * @param pageSize page size. Only ever use this in tests!
         */
        public Configuration pageSize(int pageSize) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * @param bufferFactory temporal flush buffer factories
         */
        public Configuration bufferFactory(IOBufferFactory bufferFactory) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * @param reservedPageBytes number of reserved bytes per page
         */
        public Configuration reservedPageBytes(int reservedPageBytes) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageBytes,
                    closeAllocatorOnShutdown);
        }

        /**
         * @param faultLockStriping size of the latch map for each paged file used for fault lock striping.
         */
        public Configuration faultLockStriping(int faultLockStriping) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * Disables the background eviction thread.
         */
        public Configuration disableEvictionThread() {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    false,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * Configure store files pre-allocation.
         */
        public Configuration preallocateStoreFiles(boolean preallocateStoreFiles) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }

        /**
         * Close memory allocator on page cache shutdown.
         * WARNING: when this option is set to true, leaked cursors can result in bad access and vm crash
         */
        public Configuration closeAllocatorOnShutdown(boolean closeAllocatorOnShutdown) {
            return new Configuration(
                    memoryAllocator,
                    clock,
                    memoryTracker,
                    pageCacheTracer,
                    pageSize,
                    bufferFactory,
                    faultLockStriping,
                    enableEvictionThread,
                    preallocateStoreFiles,
                    reservedPageSize,
                    closeAllocatorOnShutdown);
        }
    }

    /**
     * @param maxPages max number of pages cached in this page cache.
     * @return a new {@link Configuration} instance with default values and a {@link MemoryAllocator} for the given {@code maxPages}.
     */
    public static Configuration config(int maxPages) {
        return config(MemoryAllocator.createAllocator(memoryRequiredForPages(maxPages), EmptyMemoryTracker.INSTANCE));
    }

    /**
     * @param memoryAllocator memory allocator for the page cache.
     * @return a new {@link Configuration} instance with default values and the given {@link MemoryAllocator}.
     */
    public static Configuration config(MemoryAllocator memoryAllocator) {
        return new Configuration(
                memoryAllocator,
                Clocks.nanoClock(),
                EmptyMemoryTracker.INSTANCE,
                PageCacheTracer.NULL,
                PAGE_SIZE,
                DISABLED_BUFFER_FACTORY,
                LatchMap.faultLockStriping,
                true,
                true,
                RESERVED_BYTES,
                false);
    }

    /**
     * Create page cache.
     * @param swapperFactory page cache swapper factory
     * @param jobScheduler {@link JobScheduler} for scheduling of internal jobs
     * @param configuration additional configuration for the page cache
     */
    public MuninnPageCache(PageSwapperFactory swapperFactory, JobScheduler jobScheduler, Configuration configuration) {
        verifyHacks();
        verifyCachePageSizeIsPowerOfTwo(configuration.pageSize);
        requireNonNull(jobScheduler);
        int maxPages = calculatePageCount(configuration.memoryAllocator, configuration.pageSize);

        this.pageCacheId = pageCacheIdCounter.incrementAndGet();
        this.swapperFactory = swapperFactory;
        this.cachePageSize = configuration.pageSize;
        this.pageReservedBytes = requireNonNegative(configuration.reservedPageSize);
        this.keepFree = calculatePagesToKeepFree(maxPages);
        this.pageCacheTracer = configuration.pageCacheTracer;
        this.printExceptionsOnClose = true;
        this.bufferFactory = configuration.bufferFactory;
        this.victimPage = VictimPageReference.getVictimPage(cachePageSize, configuration.memoryTracker);
        this.pages = new PageList(
                maxPages,
                cachePageSize,
                configuration.memoryAllocator,
                new SwapperSet(),
                victimPage,
                getBufferAlignment(cachePageSize));
        this.scheduler = jobScheduler;
        this.clock = configuration.clock;
        this.faultLockStriping = configuration.faultLockStriping;
        this.enableEvictionThread = configuration.enableEvictionThread;
        this.preallocateStoreFiles = configuration.preallocateStoreFiles;
        this.memoryAllocator = configuration.memoryAllocator;
        this.closeAllocatorOnShutdown = configuration.closeAllocatorOnShutdown;
        setFreelistHead(new AtomicInteger());

        // Expose the total number of pages
        pageCacheTracer.maxPages(maxPages, cachePageSize);
        this.mappedFiles = new ConcurrentHashMap<>();
    }

    /**
     * If memory page size is larger than cache page size, alignment by memory page produces too much memory waste.
     * Therefore, we use cache page size as upper bound for alignment.
     */
    private static int getBufferAlignment(int cachePageSize) {
        return Math.min(UnsafeUtil.pageSize(), cachePageSize);
    }

    private static int calculatePagesToKeepFree(int maxPages) {
        // we can have number of pages that we want to keep free max at 50% of total pages
        int freePages = (int) (maxPages * ((float) Math.min(percentPagesToKeepFree, 50) / 100));
        // We can go as low as 30 (absolute number), as long as it does not exceed 50% of total pages
        int lowerBound = Math.min(maxPages / 2, 30);
        // We also want to have at most 100_000 free pages to avoid having page cache space wasted in PC is way too big
        return Math.max(lowerBound, Math.min(freePages, 100_000));
    }

    private static void verifyHacks() {
        // Make sure that we have access to theUnsafe.
        UnsafeUtil.assertHasUnsafe();
    }

    private static void verifyCachePageSizeIsPowerOfTwo(int cachePageSize) {
        if (!isPowerOfTwo(cachePageSize)) {
            throw new IllegalArgumentException("Cache page size must be a power of two, but was " + cachePageSize);
        }
    }

    private static int calculatePageCount(MemoryAllocator memoryAllocator, int cachePageSize) {
        long memoryPerPage = cachePageSize + PageList.META_DATA_BYTES_PER_PAGE;
        long maxPages = memoryAllocator.availableMemory() / memoryPerPage;
        int minimumPageCount = 2;
        if (maxPages < minimumPageCount) {
            throw new IllegalArgumentException(format(
                    "Page cache must have at least %s pages (%s bytes of memory), but was given %s pages.",
                    minimumPageCount, minimumPageCount * memoryPerPage, maxPages));
        }
        maxPages = Math.min(maxPages, PageList.MAX_PAGES);
        return Math.toIntExact(maxPages);
    }

    @Override
    public synchronized PagedFile map(
            Path path,
            int filePageSize,
            String databaseName,
            ImmutableSet<OpenOption> openOptions,
            IOController ioController,
            EvictionBouncer evictionBouncer,
            VersionStorage versionStorage)
            throws IOException {
        assertHealthy();
        ensureThreadsInitialised();
        if (filePageSize > cachePageSize) {
            throw new IllegalArgumentException("Cannot map files with a filePageSize (" + filePageSize
                    + ") that is greater than the " + "cachePageSize (" + cachePageSize + ")");
        }
        path = path.normalize();
        boolean createIfNotExists = false;
        boolean truncateExisting = false;
        boolean deleteOnClose = false;
        boolean anyPageSize = false;
        boolean useDirectIO = false;
        boolean littleEndian = true;
        boolean multiVersioned = false;
        boolean preallocation = preallocateStoreFiles;
        boolean contextVersionUpdates = false;
        for (OpenOption option : openOptions) {
            if (option.equals(StandardOpenOption.CREATE)) {
                createIfNotExists = true;
            } else if (option.equals(StandardOpenOption.TRUNCATE_EXISTING)) {
                truncateExisting = true;
            } else if (option.equals(StandardOpenOption.DELETE_ON_CLOSE)) {
                deleteOnClose = true;
            } else if (option.equals(PageCacheOpenOptions.ANY_PAGE_SIZE)) {
                anyPageSize = true;
            } else if (option.equals(PageCacheOpenOptions.DIRECT)) {
                useDirectIO = true;
            } else if (option.equals(PageCacheOpenOptions.BIG_ENDIAN)) {
                littleEndian = false;
            } else if (option.equals(PageCacheOpenOptions.MULTI_VERSIONED)) {
                multiVersioned = true;
            } else if (option.equals(PageCacheOpenOptions.NOPREALLOCATION)) {
                preallocation = false;
            } else if (option.equals(PageCacheOpenOptions.CONTEXT_VERSION_UPDATES)) {
                contextVersionUpdates = true;
            } else if (!ignoredOpenOptions.contains(option)) {
                throw new UnsupportedOperationException("Unsupported OpenOption: " + option);
            }
        }

        var filePath = path.toString();
        // find an existing mapping
        var current = mappedFiles.get(filePath);
        if (current != null) {
            if (current.pageSize() != filePageSize && !anyPageSize) {
                String msg = "Cannot map file " + path + " with " + "filePageSize "
                        + filePageSize + " bytes, " + "because it has already been mapped with a "
                        + "filePageSize of "
                        + current.pageSize() + " bytes.";
                throw new IllegalArgumentException(msg);
            }
            if (current.littleEndian != littleEndian) {
                throw new IllegalArgumentException("Cannot map file " + path + " with " + "littleEndian "
                        + littleEndian + ", " + "because it has already been mapped with a "
                        + "littleEndian "
                        + current.littleEndian);
            }
            if (current.multiVersioned != multiVersioned) {
                throw new IllegalArgumentException("Cannot map file " + path + " with " + "multiVersioned "
                        + multiVersioned + ", " + "because it has already been mapped with a "
                        + "multiVersioned "
                        + current.multiVersioned);
            }
            if (truncateExisting) {
                throw new UnsupportedOperationException("Cannot truncate a file that is already mapped");
            }
            current.incrementRefCount();
            current.setDeleteOnClose(deleteOnClose);
            return current;
        }

        if (filePageSize < Long.BYTES) {
            throw new IllegalArgumentException("Cannot map files with a filePageSize (" + filePageSize
                    + ") that is less than " + Long.BYTES + " bytes");
        }

        // there was no existing mapping
        var pagedFile = new MuninnPagedFile(
                path,
                this,
                filePageSize,
                swapperFactory,
                pageCacheTracer,
                createIfNotExists,
                truncateExisting,
                useDirectIO,
                preallocation,
                databaseName,
                faultLockStriping,
                ioController,
                evictionBouncer,
                multiVersioned,
                contextVersionUpdates,
                multiVersioned ? pageReservedBytes : 0,
                versionStorage,
                littleEndian);
        pagedFile.incrementRefCount();
        pagedFile.setDeleteOnClose(deleteOnClose);
        mappedFiles.put(filePath, pagedFile);
        pageCacheTracer.mappedFile(pagedFile.swapperId, pagedFile);
        return pagedFile;
    }

    @Override
    public synchronized Optional<PagedFile> getExistingMapping(Path path) throws IOException {
        assertHealthy();
        ensureThreadsInitialised();

        path = path.normalize();
        MuninnPagedFile pagedFile = tryGetMappingOrNull(path);
        if (pagedFile != null) {
            pagedFile.incrementRefCount();
            return Optional.of(pagedFile);
        }
        return Optional.empty();
    }

    private MuninnPagedFile tryGetMappingOrNull(Path path) {
        return mappedFiles.get(path.toString());
    }

    @Override
    public synchronized List<PagedFile> listExistingMappings() throws IOException {
        assertNotClosed();
        ensureThreadsInitialised();
        return mappedFiles.values().stream().map(PagedFile.class::cast).toList();
    }

    /**
     * Note: Must be called while synchronizing on the MuninnPageCache instance.
     */
    private void ensureThreadsInitialised() throws IOException {
        if (threadsInitialised) {
            return;
        }
        threadsInitialised = true;

        try {
            if (enableEvictionThread) {
                var monitoringParams = systemJob("Eviction of pages from the page cache");
                scheduler.schedule(Group.PAGE_CACHE_EVICTION, monitoringParams, new EvictionTask(this));
            }
        } catch (Exception e) {
            IOException exception = new IOException(e);
            try {
                close();
            } catch (Exception closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    synchronized void unmap(MuninnPagedFile file) {
        if (file.decrementRefCount()) {
            var filePath = file.path().toString();
            var current = mappedFiles.remove(filePath);
            if (current != null) {
                pageCacheTracer.unmappedFile(file.swapperId, file);
                flushAndCloseWithoutFail(file);
            }
        }
    }

    private void flushAndCloseWithoutFail(MuninnPagedFile file) {
        boolean flushedAndClosed = false;
        boolean printedFirstException = false;
        do {
            try {
                file.flushAndForceForClose();
                file.closeSwapper();
                flushedAndClosed = true;
            } catch (IOException e) {
                if (printExceptionsOnClose && !printedFirstException) {
                    printedFirstException = true;
                    try {
                        e.printStackTrace();
                    } catch (Exception ignore) {
                    }
                }
            }
        } while (!flushedAndClosed);
    }

    public void setPrintExceptionsOnClose(boolean enabled) {
        this.printExceptionsOnClose = enabled;
    }

    @Override
    public void flushAndForce(DatabaseFlushEvent flushEvent) throws IOException {
        var files = listExistingMappings();

        try (FileFlushEvent ignored = flushEvent.beginFileFlush()) {
            // When we flush whole page cache it can only happen on shutdown and we should be able to progress as fast
            // as we can with disabled io controller
            flushAllPagesParallel(files, IOController.DISABLED);
        }
        clearEvictorException();
    }

    private void flushAllPagesParallel(List<? extends PagedFile> files, IOController limiter) throws IOException {
        List<JobHandle<?>> flushes = new ArrayList<>(files.size());

        // Submit all flushes to the background thread
        for (PagedFile file : files) {
            flushes.add(scheduler.schedule(
                    FILE_IO_HELPER,
                    systemJob(
                            file.getDatabaseName(),
                            "Flushing changes to file '" + file.path().getFileName() + "'"),
                    () -> {
                        try {
                            flushFile((MuninnPagedFile) file, limiter);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }));
        }

        // Wait for all to complete
        for (JobHandle<?> flush : flushes) {
            try {
                flush.waitTermination();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        }
    }

    private void flushFile(MuninnPagedFile muninnPagedFile, IOController limiter) throws IOException {
        try (FileFlushEvent flushEvent = pageCacheTracer.beginFileFlush(muninnPagedFile.swapper);
                var buffer = bufferFactory.createBuffer()) {
            muninnPagedFile.flushAndForceInternal(flushEvent, false, limiter, buffer);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        var files = mappedFiles;
        if (!files.isEmpty()) {
            StringBuilder msg = new StringBuilder("Cannot close the PageCache while files are still mapped:");
            for (var file : files.values()) {
                int refCount = file.getRefCount();
                msg.append("\n\t");
                msg.append(file.path());
                msg.append(" (").append(refCount);
                msg.append(refCount == 1 ? " mapping)" : " mappings)");
            }
            throw new IllegalStateException(msg.toString());
        }

        closed = true;

        interrupt(evictionThread);
        evictionThread = null;
        if (closeAllocatorOnShutdown) {
            memoryAllocator.close();
        }
    }

    private static void interrupt(Thread thread) {
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void assertHealthy() throws IOException {
        assertNotClosed();
        IOException exception = evictorException;
        if (exception != null) {
            throw new IOException("Exception in the page eviction thread", exception);
        }
    }

    private void assertNotClosed() {
        if (closed) {
            throw new IllegalStateException("The PageCache has been shut down");
        }
    }

    @Override
    public int pageSize() {
        return cachePageSize;
    }

    @Override
    public int pageReservedBytes(ImmutableSet<OpenOption> openOptions) {
        return openOptions.contains(PageCacheOpenOptions.MULTI_VERSIONED) ? pageReservedBytes : 0;
    }

    @Override
    public long maxCachedPages() {
        return pages.getPageCount();
    }

    @Override
    public long freePages() {
        return getFreeListSize(pages, getFreelistHead());
    }

    @Override
    public IOBufferFactory getBufferFactory() {
        return bufferFactory;
    }

    int getPageCacheId() {
        return pageCacheId;
    }

    long grabFreeAndExclusivelyLockedPage(PageFaultEvent faultEvent) throws IOException {
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
        for (; ; ) {
            assertHealthy();
            current = getFreelistHead();
            if (current == null) {
                unparkEvictor();
                long pageRef = cooperativelyEvict(faultEvent);
                if (pageRef != 0) {
                    return pageRef;
                }
            } else if (current instanceof AtomicInteger counter) {
                int pageCount = pages.getPageCount();
                int pageId = counter.get();
                if (pageId < pageCount && counter.compareAndSet(pageId, pageId + 1)) {
                    faultEvent.freeListSize(pageCount - counter.get());
                    return pages.deref(pageId);
                }
                if (pageId >= pageCount) {
                    compareAndSetFreelistHead(current, null);
                }
            } else if (current instanceof FreePage freePage) {
                if (freePage == shutdownSignal) {
                    throw new IllegalStateException("The PageCache has been shut down.");
                }

                Object nextPage = freePage.next;
                if (compareAndSetFreelistHead(freePage, nextPage)) {
                    faultEvent.freeListSize(getFreeListSize(pages, nextPage));
                    return freePage.pageRef;
                }
            }
        }
    }

    private static int getFreeListSize(PageList pageList, Object next) {
        if (next instanceof FreePage) {
            return ((FreePage) next).count;
        } else if (next instanceof AtomicInteger) {
            return pageList.getPageCount() - ((AtomicInteger) next).get();
        } else {
            return 0;
        }
    }

    private long cooperativelyEvict(PageFaultEvent faultEvent) throws IOException {
        int iterations = 0;
        int pageCount = pages.getPageCount();
        int clockArm = ThreadLocalRandom.current().nextInt(pageCount);
        boolean evicted = false;
        long pageRef;
        do {
            assertHealthy();
            if (getFreelistHead() != null) {
                return 0;
            }

            if (clockArm == pageCount) {
                if (iterations == cooperativeEvictionLiveLockThreshold) {
                    throw cooperativeEvictionLiveLock();
                }
                iterations++;
                clockArm = 0;
            }

            pageRef = pages.deref(clockArm);
            if (PageList.isLoaded(pageRef) && PageList.decrementUsage(pageRef)) {
                evicted = pages.tryEvict(pageRef, faultEvent);
            }
            clockArm++;
        } while (!evicted);
        return pageRef;
    }

    private static CacheLiveLockException cooperativeEvictionLiveLock() {
        return new CacheLiveLockException(
                "Live-lock encountered when trying to cooperatively evict a page during page fault. "
                        + "This happens when we want to access a page that is not in memory, so it has to be faulted in, but "
                        + "there are no free memory pages available to accept the page fault, so we have to evict an existing "
                        + "page, but all the in-memory pages are currently locked by other accesses. If those other access are "
                        + "waiting for our page fault to make progress, then we have a live-lock, and the only way we can get "
                        + "out of it is by throwing this exception. This should be extremely rare, but can happen if the page "
                        + "cache size is tiny and the number of concurrently running transactions is very high. You should be "
                        + "able to get around this problem by increasing the amount of memory allocated to the page cache "
                        + "with the `server.memory.pagecache.size` setting. Please contact Neo4j support if you need help tuning "
                        + "your database.");
    }

    private void unparkEvictor() {
        if (evictorParked) {
            evictorParked = false;
            LockSupport.unpark(evictionThread);
        }
    }

    private void parkEvictor(long parkNanos) {
        // Only called from the background eviction thread!
        evictorParked = true;
        LockSupport.parkNanos(this, parkNanos);
        evictorParked = false;
    }

    private Object getFreelistHead() {
        return FREE_LIST.getVolatile(this);
    }

    private boolean compareAndSetFreelistHead(Object expected, Object update) {
        return FREE_LIST.compareAndSet(this, expected, update);
    }

    private void setFreelistHead(Object newFreelistHead) {
        FREE_LIST.setVolatile(this, newFreelistHead);
    }

    /**
     * Scan through all the pages, one by one, and decrement their usage stamps.
     * If a usage reaches zero, we try-write-locking it, and if we get that lock,
     * we evict the page. If we don't, we move on to the next page.
     * Once we have enough free pages, we park our thread. Page-faulting will
     * unpark our thread as needed.
     */
    void continuouslySweepPages() {
        evictionThread = Thread.currentThread();
        int clockArm = 0;

        while (!closed) {
            int pageCountToEvict = parkUntilEvictionRequired(keepFree);
            try (EvictionRunEvent evictionRunEvent = pageCacheTracer.beginPageEvictions(pageCountToEvict)) {
                clockArm = evictPages(pageCountToEvict, clockArm, evictionRunEvent);
            }
        }

        // The last thing we do, is signalling the shutdown of the cache via
        // the freelist. This signal is looked out for in grabFreePage.
        setFreelistHead(shutdownSignal);
    }

    private int parkUntilEvictionRequired(int keepFree) {
        // Park until we're either interrupted, or the number of free pages drops
        // bellow keepFree.
        long parkNanos = TimeUnit.MILLISECONDS.toNanos(10);
        for (; ; ) {
            parkEvictor(parkNanos);
            if (Thread.interrupted() || closed) {
                return 0;
            }

            int numberOfPagesToEvict = tryGetNumberOfPagesToEvict(keepFree);
            if (numberOfPagesToEvict != UNKNOWN_PAGES_TO_EVICT) {
                return numberOfPagesToEvict;
            }
        }
    }

    @VisibleForTesting
    int tryGetNumberOfPagesToEvict(int keepFree) {
        Object freelistHead = getFreelistHead();

        if (freelistHead == null) {
            return keepFree;
        } else if (freelistHead.getClass() == FreePage.class) {
            int availablePages = ((FreePage) freelistHead).count;
            if (availablePages < keepFree) {
                return keepFree - availablePages;
            }
        } else if (freelistHead.getClass() == AtomicInteger.class) {
            AtomicInteger counter = (AtomicInteger) freelistHead;
            long count = pages.getPageCount() - counter.get();
            if (count < keepFree) {
                return count < 0 ? keepFree : (int) (keepFree - count);
            }
        }
        return UNKNOWN_PAGES_TO_EVICT;
    }

    int evictPages(int pageEvictionAttempts, int clockArm, EvictionRunEvent evictionRunEvent) {
        while (pageEvictionAttempts > 0 && !closed) {
            if (clockArm == pages.getPageCount()) {
                clockArm = 0;
            }

            if (closed) {
                // The page cache has been shut down.
                return 0;
            }

            long pageRef = pages.deref(clockArm);
            if (PageList.isLoaded(pageRef) && PageList.decrementUsage(pageRef)) {
                try {
                    pageEvictionAttempts--;
                    if (pages.tryEvict(pageRef, evictionRunEvent)) {
                        clearEvictorException();
                        addFreePageToFreelist(pageRef, evictionRunEvent);
                    }
                } catch (IOException e) {
                    evictorException = e;
                } catch (OutOfMemoryError oom) {
                    evictorException = oomException;
                } catch (Throwable th) {
                    evictorException = new IOException("Eviction thread encountered a problem", th);
                }
            }

            clockArm++;
        }

        return clockArm;
    }

    @VisibleForTesting
    String describePages() {
        var result = new StringBuilder();
        var pageCount = pages.getPageCount();
        for (int pageCachePageId = 0; pageCachePageId < pageCount; pageCachePageId++) {
            var pageRef = pages.deref(pageCachePageId);
            result.append("[");
            result.append("PageCachePageId: ").append(pageCachePageId).append(", ");
            result.append("PageRef: ").append(Long.toHexString(pageRef)).append(", ");
            result.append("LockWord: ")
                    .append(Long.toBinaryString(UnsafeUtil.getLongVolatile(pageRef)))
                    .append(", ");
            result.append("Address: ")
                    .append(Long.toHexString(UnsafeUtil.getLongVolatile(pageRef + 8)))
                    .append(", ");
            result.append("LastTxId: ")
                    .append(Long.toHexString(UnsafeUtil.getLongVolatile(pageRef + 16)))
                    .append(", ");
            result.append("PageBinding: ").append(Long.toHexString(UnsafeUtil.getLongVolatile(pageRef + 24)));
            result.append("]\n");
        }
        return result.toString();
    }

    void addFreePageToFreelist(long pageRef, EvictionRunEvent evictions) {
        Object current;
        assert getPageHorizon(pageRef) == 0;
        FreePage freePage = new FreePage(pageRef);
        int pageCount = pages.getPageCount();
        do {
            current = getFreelistHead();
            if (current instanceof AtomicInteger && ((AtomicInteger) current).get() > pageCount) {
                current = null;
            }
            freePage.setNext(pageCount, current);
        } while (!compareAndSetFreelistHead(current, freePage));
        evictions.freeListSize(freePage.count);
    }

    void clearEvictorException() {
        if (evictorException != null) {
            evictorException = null;
        }
    }

    @Override
    public String toString() {
        int pagesToEvict = tryGetNumberOfPagesToEvict(keepFree);
        return format(
                "%s[pageCacheId:%d, pageSize:%d, pages:%d, pagesToEvict:%s]",
                getClass().getSimpleName(),
                pageCacheId,
                cachePageSize,
                pages.getPageCount(),
                pagesToEvict != UNKNOWN_PAGES_TO_EVICT ? String.valueOf(pagesToEvict) : "N/A");
    }

    void sweep(SwapperSet swappers) {
        swappers.sweep(swapperIds -> {
            int pageCount = pages.getPageCount();
            try (EvictionRunEvent evictionEvent = pageCacheTracer.beginEviction()) {
                for (int i = 0; i < pageCount; i++) {
                    long pageRef = pages.deref(i);
                    while (swapperIds.contains(PageList.getSwapperId(pageRef))) {
                        if (pages.tryEvict(pageRef, evictionEvent)) {
                            addFreePageToFreelist(pageRef, evictionEvent);
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    void startPreFetching(MuninnPageCursor cursor, CursorFactory cursorFactory) {
        PreFetcher preFetcher = new PreFetcher(cursor, cursorFactory, clock);
        var pagedFile = cursor.pagedFile;
        var fileName = pagedFile.swapper.path().getFileName();
        var monitoringParams = systemJob(pagedFile.databaseName, "Pre-fetching of file '" + fileName + "'");
        cursor.preFetcher = scheduler.schedule(Group.PAGE_CACHE_PRE_FETCHER, monitoringParams, preFetcher);
    }

    @VisibleForTesting
    int getKeepFree() {
        return keepFree;
    }
}
