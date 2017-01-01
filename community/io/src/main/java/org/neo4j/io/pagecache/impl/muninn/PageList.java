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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.MemoryManager;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static java.lang.String.format;

/**
 * The PageList maintains the off-heap meta-data for the individual memory pages.
 *
 * The meta-data for each page is the following:
 *
 * <table>
 *     <tr><th>Bytes</th><th>Use</th></tr>
 *     <tr><td>8</td><td>Sequence lock word.</td></tr>
 *     <tr><td>8</td><td>Pointer to the memory page.</td></tr>
 *     <tr><td>8</td><td>File page id.</td></tr>
 *     <tr><td>4</td><td>Page swapper id.</td></tr>
 *     <tr><td>1</td><td>Usage stamp. Optimistically incremented; truncated to a max of 4.</td></tr>
 *     <tr><td>3</td><td>Padding.</td></tr>
 * </table>
 */
class PageList
{
    private static final int META_DATA_BYTES_PER_PAGE = 32;
    private static final int OFFSET_LOCK_WORD = 0; // 8 bytes
    private static final int OFFSET_ADDRESS = 8; // 8 bytes
    private static final int OFFSET_FILE_PAGE_ID = 16; // 8 bytes
    private static final int OFFSET_SWAPPER_ID = 24; // 4 bytes
    private static final int OFFSET_USAGE_COUNTER = 28; // 1 byte
    // todo it's possible to reduce the overhead of the individual page to just 24 bytes,
    // todo because the file page id can be represented with 5 bytes (enough to address 8-4 PBs),
    // todo and then the usage counter can use the high bits of that word, and the swapper id
    // todo can use the rest (2 bytes or 20 bits).
    // todo we can alternatively also make use of the lower 12 bits of the address field, because
    // todo the addresses are page aligned, and we can assume them to be at least 4096 bytes in size.

    private final long pageCount;
    private final int cachePageSize;
    private final MemoryManager memoryManager;
    private final SwapperSet swappers;
    private final long victimPageAddress;
    private final long baseAddress;

    PageList( long pageCount, int cachePageSize, MemoryManager memoryManager, SwapperSet swappers, long victimPageAddress )
    {
        this.pageCount = pageCount;
        this.cachePageSize = cachePageSize;
        this.memoryManager = memoryManager;
        this.swappers = swappers;
        this.victimPageAddress = victimPageAddress;
        long bytes = pageCount * META_DATA_BYTES_PER_PAGE;
        this.baseAddress = memoryManager.allocateAligned( bytes );
        clearMemory( baseAddress, pageCount );
    }

    /**
     * This copy-constructor is useful for classes that want to extend the {@code PageList} class to inline its fields.
     * All data and state will be shared between this and the given {@code PageList}. This means that changing the page
     * list state through one has the same effect as changing it through the other – they are both effectively the same
     * object.
     * @param pageList The {@code PageList} instance whose state to copy.
     */
    PageList( PageList pageList )
    {
        this.pageCount = pageList.pageCount;
        this.cachePageSize = pageList.cachePageSize;
        this.memoryManager = pageList.memoryManager;
        this.swappers = pageList.swappers;
        this.victimPageAddress = pageList.victimPageAddress;
        this.baseAddress = pageList.baseAddress;
    }

    private void clearMemory( long baseAddress, long pageCount )
    {
        long address = baseAddress - 8;
        for ( long i = 0; i < pageCount; i++ )
        {
            UnsafeUtil.putLong( address += 8, 0 ); // lock word
            UnsafeUtil.putLong( address += 8, 0 ); // pointer
            UnsafeUtil.putLong( address += 8, PageCursor.UNBOUND_PAGE_ID ); // file page id
            UnsafeUtil.putLong( address += 8, 0 ); // rest
        }
        UnsafeUtil.fullFence(); // Guarantee the visibility of the cleared memory
    }

    /**
     * Turn a {@code pageId} into a {@code pageRef} that can be used for accessing and manipulating the given page
     * using the other methods in this class.
     * @param pageId The {@code pageId} to turn into a {@code pageRef}.
     * @return A {@code pageRef} which is an opaque, internal and direct pointer to the meta-data of the given memory
     * page.
     */
    public long deref( long pageId )
    {
        return baseAddress + pageId * META_DATA_BYTES_PER_PAGE;
    }

    private long offLock( long pageRef )
    {
        return pageRef + OFFSET_LOCK_WORD;
    }

    private long offAddress( long pageRef )
    {
        return pageRef + OFFSET_ADDRESS;
    }

    private long offUsage( long pageRef )
    {
        return pageRef + OFFSET_USAGE_COUNTER;
    }

    private long offFilePageId( long pageRef )
    {
        return pageRef + OFFSET_FILE_PAGE_ID;
    }

    private long offSwapperId( long pageRef )
    {
        return pageRef + OFFSET_SWAPPER_ID;
    }

    public long tryOptimisticReadLock( long pageRef )
    {
        return OffHeapPageLock.tryOptimisticReadLock( offLock( pageRef ) );
    }

    public boolean validateReadLock( long pageRef, long stamp )
    {
        return OffHeapPageLock.validateReadLock( offLock( pageRef ), stamp );
    }

    public boolean isModified( long pageRef )
    {
        return OffHeapPageLock.isModified( offLock( pageRef ) );
    }

    public boolean isExclusivelyLocked( long pageRef )
    {
        return OffHeapPageLock.isExclusivelyLocked( offLock( pageRef ) );
    }

    public boolean tryWriteLock( long pageRef )
    {
        return OffHeapPageLock.tryWriteLock( offLock( pageRef ) );
    }

    public void unlockWrite( long pageRef )
    {
        OffHeapPageLock.unlockWrite( offLock( pageRef ) );
    }

    public boolean tryExclusiveLock( long pageRef )
    {
        return OffHeapPageLock.tryExclusiveLock( offLock( pageRef ) );
    }

    public long unlockExclusive( long pageRef )
    {
        return OffHeapPageLock.unlockExclusive( offLock( pageRef ) );
    }

    public void unlockExclusiveAndTakeWriteLock( long pageRef )
    {
        OffHeapPageLock.unlockExclusiveAndTakeWriteLock( offLock( pageRef ) );
    }

    public long tryFlushLock( long pageRef )
    {
        return OffHeapPageLock.tryFlushLock( offLock( pageRef ) );
    }

    public void unlockFlush( long pageRef, long stamp, boolean success )
    {
        OffHeapPageLock.unlockFlush( offLock( pageRef ), stamp, success );
    }

    public void explicitlyMarkPageUnmodifiedUnderExclusiveLock( long pageRef )
    {
        OffHeapPageLock.explicitlyMarkPageUnmodifiedUnderExclusiveLock( offLock( pageRef ) );
    }

    public int getCachePageSize()
    {
        return cachePageSize;
    }

    public long getAddress( long pageRef )
    {
        return UnsafeUtil.getLong( offAddress( pageRef ) );
    }

    public void initBuffer( long pageRef )
    {
        if ( getAddress( pageRef ) == 0L )
        {
            long addr = memoryManager.allocateAligned( getCachePageSize() );
            UnsafeUtil.putLong( offAddress( pageRef ), addr );
        }
    }

    private byte getUsageCounter( long pageRef )
    {
        return UnsafeUtil.getByteVolatile( offUsage( pageRef ) );
    }

    private void setUsageCounter( long pageRef, byte count )
    {
        UnsafeUtil.putByteVolatile( offUsage( pageRef ), count );
    }

    /**
     * Increment the usage stamp to at most 4.
     **/
    public void incrementUsage( long pageRef )
    {
        // This is intentionally left benignly racy for performance.
        byte usage = getUsageCounter( pageRef );
        if ( usage < 4 ) // avoid cache sloshing by not doing a write if counter is already maxed out
        {
            usage++;
            setUsageCounter( pageRef, usage );
        }
    }

    /**
     * Decrement the usage stamp. Returns true if it reaches 0.
     **/
    public boolean decrementUsage( long pageRef )
    {
        // This is intentionally left benignly racy for performance.
        byte usage = getUsageCounter( pageRef );
        if ( usage > 0 )
        {
            usage--;
            setUsageCounter( pageRef, usage );
        }
        return usage == 0;
    }

    public long getFilePageId( long pageRef )
    {
        return UnsafeUtil.getLong( offFilePageId( pageRef ) );
    }

    private void setFilePageId( long pageRef, long filePageId )
    {
        UnsafeUtil.putLong( offFilePageId( pageRef ), filePageId );
    }

    public int getSwapperId( long pageRef )
    {
        return UnsafeUtil.getInt( offSwapperId( pageRef ) );
    }

    private void setSwapperId( long pageRef, int swapperId )
    {
        UnsafeUtil.putInt( offSwapperId( pageRef ), swapperId );
    }

    public boolean isLoaded( long pageRef )
    {
        return getFilePageId( pageRef ) != PageCursor.UNBOUND_PAGE_ID;
    }

    public boolean isBoundTo( long pageRef, int swapperId, long filePageId )
    {
        return getSwapperId( pageRef ) == swapperId && getFilePageId( pageRef ) == filePageId;
    }

    public void fault( long pageRef, PageSwapper swapper, int swapperId, long filePageId, PageFaultEvent event )
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
        event.setCachePageId( (int) pageRef );
        setSwapperId( pageRef, swapperId ); // Page now considered isBoundTo( swapper, filePageId )
    }

    private static IllegalArgumentException swapperCannotBeNull()
    {
        return new IllegalArgumentException( "swapper cannot be null" );
    }

    private static IllegalStateException cannotFaultException( long pageRef, PageSwapper swapper, int swapperId,
                                                        long filePageId, int currentSwapper, long currentFilePageId )
    {
        String msg = format(
                "Cannot fault page {filePageId = %s, swapper = %s (swapper id = %s)} into " +
                "cache page %s. Already bound to {filePageId = " +
                "%s, swapper id = %s}.",
                filePageId, swapper, swapperId, pageRef, currentFilePageId, currentSwapper );
        return new IllegalStateException( msg );
    }

    public boolean tryEvict( long pageRef ) throws IOException
    {
        if ( tryExclusiveLock( pageRef ) )
        {
            if ( isLoaded( pageRef ) )
            {
                int swapperId = getSwapperId( pageRef );
                SwapperSet.Allocation allocation = swappers.getAllocation( swapperId );
                PageSwapper swapper = allocation.swapper;
                long filePageId = getFilePageId( pageRef );
                if ( isModified( pageRef ) )
                {
                    long address = getAddress( pageRef );
                    swapper.write( filePageId, address, allocation.filePageSize );
                    explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
                }
                swapper.evicted( filePageId, null );
                clearBinding( pageRef );
                return true;
            }
            else
            {
                unlockExclusive( pageRef );
            }
        }
        return false;
    }

    private void clearBinding( long pageRef )
    {
        setFilePageId( pageRef, PageCursor.UNBOUND_PAGE_ID );
        setSwapperId( pageRef, 0 );
    }
}
