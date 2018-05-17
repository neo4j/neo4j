/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.io.IOException;

import org.neo4j.io.mem.GrabAllocator;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionEventOpportunity;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static java.lang.String.format;
import static org.neo4j.util.FeatureToggles.flag;

/**
 * The PageList maintains the off-heap meta-data for the individual memory pages.
 * <p>
 * The meta-data for each page is the following:
 *
 * <table>
 * <tr><th>Bytes</th><th>Use</th></tr>
 * <tr><td>8</td><td>Sequence lock word.</td></tr>
 * <tr><td>8</td><td>Pointer to the memory page.</td></tr>
 * <tr><td>8</td><td>Last modified transaction id.</td></tr>
 * <tr><td>8</td><td>Page binding. The first 40 bits (5 bytes) are the file page id.
 * The following (low order) 21 bits (2 bytes and 5 bits) are the swapper id.
 * The last (lowest order) 3 bits are the page usage counter.</td></tr>
 * </table>
 */
class PageList
{
    private static final boolean forceSlowMemoryClear = flag( PageList.class, "forceSlowMemoryClear", false );

    static final int META_DATA_BYTES_PER_PAGE = 32;
    static final long MAX_PAGES = Integer.MAX_VALUE;

    private static final int UNBOUND_LAST_MODIFIED_TX_ID = -1;
    private static final long MAX_USAGE_COUNT = 4;
    private static final int SHIFT_FILE_PAGE_ID = 24;
    private static final int SHIFT_SWAPPER_ID = 3;
    private static final int SHIFT_PARTIAL_FILE_PAGE_ID = SHIFT_FILE_PAGE_ID - SHIFT_SWAPPER_ID;
    private static final long MASK_USAGE_COUNT = (1L << SHIFT_SWAPPER_ID) - 1L;
    private static final long MASK_NOT_USAGE_COUNT = ~MASK_USAGE_COUNT;
    private static final long MASK_NOT_FILE_PAGE_ID = (1L << SHIFT_FILE_PAGE_ID) - 1L;
    private static final long MASK_SHIFTED_SWAPPER_ID = MASK_NOT_FILE_PAGE_ID >>> SHIFT_SWAPPER_ID;
    private static final long MASK_NOT_SWAPPER_ID = ~(MASK_SHIFTED_SWAPPER_ID << SHIFT_SWAPPER_ID);
    private static final long UNBOUND_PAGE_BINDING = PageCursor.UNBOUND_PAGE_ID << SHIFT_FILE_PAGE_ID;

    // 40 bits for file page id
    private static final long MAX_FILE_PAGE_ID = (1L << Long.SIZE - SHIFT_FILE_PAGE_ID) - 1L;

    private static final int OFFSET_LOCK_WORD = 0; // 8 bytes.
    private static final int OFFSET_ADDRESS = 8; // 8 bytes.
    private static final int OFFSET_LAST_TX_ID = 16; // 8 bytes.
    // The high 5 bytes of the page binding are the file page id.
    // The 21 following lower bits are the swapper id.
    // And the last 3 low bits are the usage counter.
    private static final int OFFSET_PAGE_BINDING = 24; // 8 bytes.

    private final int pageCount;
    private final int cachePageSize;
    private final MemoryAllocator memoryAllocator;
    private final SwapperSet swappers;
    private final long victimPageAddress;
    private final long baseAddress;
    private final long bufferAlignment;

    PageList( int pageCount, int cachePageSize, MemoryAllocator memoryAllocator, SwapperSet swappers,
              long victimPageAddress, long bufferAlignment )
    {
        this.pageCount = pageCount;
        this.cachePageSize = cachePageSize;
        this.memoryAllocator = memoryAllocator;
        this.swappers = swappers;
        this.victimPageAddress = victimPageAddress;
        long bytes = ((long) pageCount) * META_DATA_BYTES_PER_PAGE;

        long bytesToFree = GrabAllocator.leakedBytesCounter.get();
        long currentThreadID = Thread.currentThread().getId();
        if ( bytesToFree > 0 )
        {
            System.out.println( currentThreadID + ":have " + bytesToFree + " bytes to free, but open new page cache already." );
        }
        else
        {
            System.out.println( currentThreadID + ":no bytes kept. Good to allocate." );
        }
        this.baseAddress = memoryAllocator.allocateAligned( bytes, Long.BYTES );

        this.bufferAlignment = bufferAlignment;
        clearMemory( baseAddress, pageCount );
    }

    /**
     * This copy-constructor is useful for classes that want to extend the {@code PageList} class to inline its fields.
     * All data and state will be shared between this and the given {@code PageList}. This means that changing the page
     * list state through one has the same effect as changing it through the other – they are both effectively the same
     * object.
     *
     * @param pageList The {@code PageList} instance whose state to copy.
     */
    PageList( PageList pageList )
    {
        this.pageCount = pageList.pageCount;
        this.cachePageSize = pageList.cachePageSize;
        this.memoryAllocator = pageList.memoryAllocator;
        this.swappers = pageList.swappers;
        this.victimPageAddress = pageList.victimPageAddress;
        this.baseAddress = pageList.baseAddress;
        this.bufferAlignment = pageList.bufferAlignment;
    }

    private void clearMemory( long baseAddress, long pageCount )
    {
        long memcpyChunkSize = UnsafeUtil.pageSize();
        long metaDataEntriesPerChunk = memcpyChunkSize / META_DATA_BYTES_PER_PAGE;
        if ( pageCount < metaDataEntriesPerChunk || forceSlowMemoryClear )
        {
            clearMemorySimple( baseAddress, pageCount );
        }
        else
        {
            clearMemoryFast( baseAddress, pageCount, memcpyChunkSize, metaDataEntriesPerChunk );
        }
        UnsafeUtil.fullFence(); // Guarantee the visibility of the cleared memory.
    }

    private void clearMemorySimple( long baseAddress, long pageCount )
    {
        long address = baseAddress - Long.BYTES;
        long initialLockWord = OffHeapPageLock.initialLockWordWithExclusiveLock();
        for ( long i = 0; i < pageCount; i++ )
        {
            UnsafeUtil.putLong( address += Long.BYTES, initialLockWord ); // lock word
            UnsafeUtil.putLong( address += Long.BYTES, 0 ); // pointer
            UnsafeUtil.putLong( address += Long.BYTES, 0 ); // last tx id
            UnsafeUtil.putLong( address += Long.BYTES, UNBOUND_PAGE_BINDING );
        }
    }

    private void clearMemoryFast( long baseAddress, long pageCount, long memcpyChunkSize, long metaDataEntriesPerChunk )
    {
        // Initialise one chunk worth of data.
        clearMemorySimple( baseAddress, metaDataEntriesPerChunk );
        // Since all entries contain the same data, we can now copy this chunk over and over.
        long chunkCopies = pageCount / metaDataEntriesPerChunk - 1;
        long address = baseAddress + memcpyChunkSize;
        for ( int i = 0; i < chunkCopies; i++ )
        {
            UnsafeUtil.copyMemory( baseAddress, address, memcpyChunkSize );
            address += memcpyChunkSize;
        }
        // Finally fill in the tail.
        long tailCount = pageCount % metaDataEntriesPerChunk;
        clearMemorySimple( address, tailCount );
    }

    /**
     * @return The capacity of the page list.
     */
    int getPageCount()
    {
        return pageCount;
    }

    SwapperSet getSwappers()
    {
        return swappers;
    }

    /**
     * Turn a {@code pageId} into a {@code pageRef} that can be used for accessing and manipulating the given page
     * using the other methods in this class.
     *
     * @param pageId The {@code pageId} to turn into a {@code pageRef}.
     * @return A {@code pageRef} which is an opaque, internal and direct pointer to the meta-data of the given memory
     * page.
     */
    long deref( int pageId )
    {
        //noinspection UnnecessaryLocalVariable
        long id = pageId; // convert to long to avoid int multiplication
        return baseAddress + (id * META_DATA_BYTES_PER_PAGE);
    }

    int toId( long pageRef )
    {
        // >> 5 is equivalent to dividing by 32, META_DATA_BYTES_PER_PAGE.
        return (int) ((pageRef - baseAddress) >> 5);
    }

    private long offLastModifiedTransactionId( long pageRef )
    {
        return pageRef + OFFSET_LAST_TX_ID;
    }

    private long offLock( long pageRef )
    {
        return pageRef + OFFSET_LOCK_WORD;
    }

    private long offAddress( long pageRef )
    {
        return pageRef + OFFSET_ADDRESS;
    }

    private long offPageBinding( long pageRef )
    {
        return pageRef + OFFSET_PAGE_BINDING;
    }

    long tryOptimisticReadLock( long pageRef )
    {
        return OffHeapPageLock.tryOptimisticReadLock( offLock( pageRef ) );
    }

    boolean validateReadLock( long pageRef, long stamp )
    {
        return OffHeapPageLock.validateReadLock( offLock( pageRef ), stamp );
    }

    boolean isModified( long pageRef )
    {
        return OffHeapPageLock.isModified( offLock( pageRef ) );
    }

    boolean isExclusivelyLocked( long pageRef )
    {
        return OffHeapPageLock.isExclusivelyLocked( offLock( pageRef ) );
    }

    boolean tryWriteLock( long pageRef )
    {
        return OffHeapPageLock.tryWriteLock( offLock( pageRef ) );
    }

    void unlockWrite( long pageRef )
    {
        OffHeapPageLock.unlockWrite( offLock( pageRef ) );
    }

    long unlockWriteAndTryTakeFlushLock( long pageRef )
    {
        return OffHeapPageLock.unlockWriteAndTryTakeFlushLock( offLock( pageRef ) );
    }

    boolean tryExclusiveLock( long pageRef )
    {
        return OffHeapPageLock.tryExclusiveLock( offLock( pageRef ) );
    }

    long unlockExclusive( long pageRef )
    {
        return OffHeapPageLock.unlockExclusive( offLock( pageRef ) );
    }

    void unlockExclusiveAndTakeWriteLock( long pageRef )
    {
        OffHeapPageLock.unlockExclusiveAndTakeWriteLock( offLock( pageRef ) );
    }

    long tryFlushLock( long pageRef )
    {
        return OffHeapPageLock.tryFlushLock( offLock( pageRef ) );
    }

    void unlockFlush( long pageRef, long stamp, boolean success )
    {
        OffHeapPageLock.unlockFlush( offLock( pageRef ), stamp, success );
    }

    void explicitlyMarkPageUnmodifiedUnderExclusiveLock( long pageRef )
    {
        OffHeapPageLock.explicitlyMarkPageUnmodifiedUnderExclusiveLock( offLock( pageRef ) );
    }

    int getCachePageSize()
    {
        return cachePageSize;
    }

    long getAddress( long pageRef )
    {
        return UnsafeUtil.getLong( offAddress( pageRef ) );
    }

    void initBuffer( long pageRef )
    {
        if ( getAddress( pageRef ) == 0L )
        {
            long addr = memoryAllocator.allocateAligned( getCachePageSize(), bufferAlignment );
            UnsafeUtil.putLong( offAddress( pageRef ), addr );
        }
    }

    private byte getUsageCounter( long pageRef )
    {
        return (byte) (UnsafeUtil.getLongVolatile( offPageBinding( pageRef ) ) & MASK_USAGE_COUNT);
    }

    /**
     * Increment the usage stamp to at most 4.
     **/
    void incrementUsage( long pageRef )
    {
        // This is intentionally left benignly racy for performance.
        long address = offPageBinding( pageRef );
        long v = UnsafeUtil.getLongVolatile( address );
        long usage = v & MASK_USAGE_COUNT;
        if ( usage < MAX_USAGE_COUNT ) // avoid cache sloshing by not doing a write if counter is already maxed out
        {
            usage++;
            // Use compareAndSwapLong to only actually store the updated count if nothing else changed
            // in this word-line. The word-line is shared with the file page id, and the swapper id.
            // Those fields are updated under guard of the exclusive lock, but we *might* race with
            // that here, and in that case we would never want a usage counter update to clobber a page
            // binding update.
            UnsafeUtil.compareAndSwapLong( null, address, v, (v & MASK_NOT_USAGE_COUNT) + usage );
        }
    }

    /**
     * Decrement the usage stamp. Returns true if it reaches 0.
     **/
    boolean decrementUsage( long pageRef )
    {
        // This is intentionally left benignly racy for performance.
        long address = offPageBinding( pageRef );
        long v = UnsafeUtil.getLongVolatile( address );
        long usage = v & MASK_USAGE_COUNT;
        if ( usage > 0 )
        {
            usage--;
            // See `incrementUsage` about why we use `compareAndSwapLong`.
            UnsafeUtil.compareAndSwapLong( null, address, v, (v & MASK_NOT_USAGE_COUNT) + usage );
        }
        return usage == 0;
    }

    long getFilePageId( long pageRef )
    {
        long filePageId = UnsafeUtil.getLong( offPageBinding( pageRef ) ) >>> SHIFT_FILE_PAGE_ID;
        return filePageId == MAX_FILE_PAGE_ID ? PageCursor.UNBOUND_PAGE_ID : filePageId;
    }

    private void setFilePageId( long pageRef, long filePageId )
    {
        if ( filePageId > MAX_FILE_PAGE_ID )
        {
            throw new IllegalArgumentException(
                    format( "File page id: %s is bigger then max supported value %s.", filePageId, MAX_FILE_PAGE_ID ) );
        }
        long address = offPageBinding( pageRef );
        long v = UnsafeUtil.getLong( address );
        filePageId = (filePageId << SHIFT_FILE_PAGE_ID) + (v & MASK_NOT_FILE_PAGE_ID);
        UnsafeUtil.putLong( address, filePageId );
    }

    long getLastModifiedTxId( long pageRef )
    {
        return UnsafeUtil.getLongVolatile( offLastModifiedTransactionId( pageRef ) );
    }

    /**
     * @return return last modifier transaction id and resets it to {@link #UNBOUND_LAST_MODIFIED_TX_ID}
     */
    long getAndResetLastModifiedTransactionId( long pageRef )
    {
        return UnsafeUtil.getAndSetLong( null, offLastModifiedTransactionId( pageRef ), UNBOUND_LAST_MODIFIED_TX_ID );
    }

    void setLastModifiedTxId( long pageRef, long modifierTxId )
    {
        UnsafeUtil.compareAndSetMaxLong( null, offLastModifiedTransactionId( pageRef ), modifierTxId );
    }

    int getSwapperId( long pageRef )
    {
        long v = UnsafeUtil.getLong( offPageBinding( pageRef ) ) >>> SHIFT_SWAPPER_ID;
        return (int) (v & MASK_SHIFTED_SWAPPER_ID); // 21 bits.
    }

    private void setSwapperId( long pageRef, int swapperId )
    {
        swapperId = swapperId << SHIFT_SWAPPER_ID;
        long address = offPageBinding( pageRef );
        long v = UnsafeUtil.getLong( address ) & MASK_NOT_SWAPPER_ID;
        UnsafeUtil.putLong( address, v + swapperId );
    }

    boolean isLoaded( long pageRef )
    {
        return getFilePageId( pageRef ) != PageCursor.UNBOUND_PAGE_ID;
    }

    boolean isBoundTo( long pageRef, int swapperId, long filePageId )
    {
        long address = offPageBinding( pageRef );
        long expectedBinding = (filePageId << SHIFT_PARTIAL_FILE_PAGE_ID) + swapperId;
        long actualBinding = UnsafeUtil.getLong( address ) >>> SHIFT_SWAPPER_ID;
        return expectedBinding == actualBinding;
    }

    void fault( long pageRef, PageSwapper swapper, int swapperId, long filePageId, PageFaultEvent event )
            throws IOException
    {
        if ( swapper == null )
        {
            throw swapperCannotBeNull();
        }
        int currentSwapper = getSwapperId( pageRef );
        long currentFilePageId = getFilePageId( pageRef );
        if ( filePageId == PageCursor.UNBOUND_PAGE_ID || !isExclusivelyLocked( pageRef )
             || currentSwapper != 0 || currentFilePageId != PageCursor.UNBOUND_PAGE_ID )
        {
            throw cannotFaultException( pageRef, swapper, swapperId, filePageId, currentSwapper, currentFilePageId );
        }
        // Note: It is important that we assign the filePageId before we swap
        // the page in. If the swapping fails, the page will be considered
        // loaded for the purpose of eviction, and will eventually return to
        // the freelist. However, because we don't assign the swapper until the
        // swapping-in has succeeded, the page will not be considered bound to
        // the file page, so any subsequent thread that finds the page in their
        // translation table will re-do the page fault.
        setFilePageId( pageRef, filePageId ); // Page now considered isLoaded()
        long bytesRead = swapper.read( filePageId, getAddress( pageRef ), cachePageSize );
        event.addBytesRead( bytesRead );
        event.setCachePageId( toId( pageRef ) );
        setSwapperId( pageRef, swapperId ); // Page now considered isBoundTo( swapper, filePageId )
    }

    private static IllegalArgumentException swapperCannotBeNull()
    {
        return new IllegalArgumentException( "swapper cannot be null" );
    }

    private static IllegalStateException cannotFaultException( long pageRef, PageSwapper swapper, int swapperId,
                                                               long filePageId, int currentSwapper,
                                                               long currentFilePageId )
    {
        String msg = format(
                "Cannot fault page {filePageId = %s, swapper = %s (swapper id = %s)} into " +
                "cache page %s. Already bound to {filePageId = " +
                "%s, swapper id = %s}.",
                filePageId, swapper, swapperId, pageRef, currentFilePageId, currentSwapper );
        return new IllegalStateException( msg );
    }

    boolean tryEvict( long pageRef, EvictionEventOpportunity evictionOpportunity ) throws IOException
    {
        if ( tryExclusiveLock( pageRef ) )
        {
            if ( isLoaded( pageRef ) )
            {
                try ( EvictionEvent evictionEvent = evictionOpportunity.beginEviction() )
                {
                    evict( pageRef, evictionEvent );
                    return true;
                }
            }
            unlockExclusive( pageRef );
        }
        return false;
    }

    private void evict( long pageRef, EvictionEvent evictionEvent ) throws IOException
    {
        long filePageId = getFilePageId( pageRef );
        evictionEvent.setFilePageId( filePageId );
        evictionEvent.setCachePageId( pageRef );
        int swapperId = getSwapperId( pageRef );
        if ( swapperId != 0 )
        {
            // If the swapper id is non-zero, then the page was not only loaded, but also bound, and possibly modified.
            SwapperSet.SwapperMapping swapperMapping = swappers.getAllocation( swapperId );
            if ( swapperMapping != null )
            {
                // The allocation can be null if the file has been unmapped, but there are still pages
                // lingering in the cache that were bound to file page in that file.
                PageSwapper swapper = swapperMapping.swapper;
                evictionEvent.setSwapper( swapper );

                if ( isModified( pageRef ) )
                {
                    flushModifiedPage( pageRef, evictionEvent, filePageId, swapper );
                }
                swapper.evicted( filePageId );
            }
        }
        clearBinding( pageRef );
    }

    private void flushModifiedPage( long pageRef, EvictionEvent evictionEvent, long filePageId, PageSwapper swapper )
            throws IOException
    {
        FlushEvent flushEvent = evictionEvent.flushEventOpportunity().beginFlush( filePageId, pageRef, swapper );
        try
        {
            long address = getAddress( pageRef );
            long bytesWritten = swapper.write( filePageId, address );
            explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
            flushEvent.addBytesWritten( bytesWritten );
            flushEvent.addPagesFlushed( 1 );
            flushEvent.done();
        }
        catch ( IOException e )
        {
            unlockExclusive( pageRef );
            flushEvent.done( e );
            evictionEvent.threwException( e );
            throw e;
        }
    }

    private void clearBinding( long pageRef )
    {
        UnsafeUtil.putLong( offPageBinding( pageRef ), UNBOUND_PAGE_BINDING );
    }

    void toString( long pageRef, StringBuilder sb )
    {
        sb.append( "Page[ id = " ).append( toId( pageRef ) );
        sb.append( ", address = " ).append( getAddress( pageRef ) );
        sb.append( ", filePageId = " ).append( getFilePageId( pageRef ) );
        sb.append( ", swapperId = " ).append( getSwapperId( pageRef ) );
        sb.append( ", usageCounter = " ).append( getUsageCounter( pageRef ) );
        sb.append( " ] " ).append( OffHeapPageLock.toString( offLock( pageRef ) ) );
    }

    public void close()
    {
        memoryAllocator.freeMemory();
    }
}
