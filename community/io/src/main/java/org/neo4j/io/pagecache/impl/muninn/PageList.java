/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
 *
 * The meta-data for each page is the following:
 *
 * <table>
 *     <tr><th>Bytes</th><th>Use</th></tr>
 *     <tr><td>8</td><td>Sequence lock word.</td></tr>
 *     <tr><td>8</td><td>Pointer to the memory page.</td></tr>
 *     <tr><td>8</td><td>Last modified transaction id.</td></tr>
 *     <tr><td>5</td><td>File page id.</td></tr>
 *     <tr><td>1</td><td>Usage stamp. Optimistically incremented; truncated to a max of 4.</td></tr>
 *     <tr><td>2</td><td>Page swapper id.</td></tr>
 * </table>
 */
class PageList
{
    private static final boolean forceSlowMemoryClear = flag( PageList.class, "forceSlowMemoryClear", false );

    public static final int META_DATA_BYTES_PER_PAGE = 32;
    public static final long MAX_PAGES = Integer.MAX_VALUE;

    private static final int UNBOUND_LAST_MODIFIED_TX_ID = -1;
    private static final int UNSIGNED_BYTE_MASK = 0xFF;
    private static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;

    // 40 bits for file page id
    private static final long MAX_FILE_PAGE_ID = 0b11111111_11111111_11111111_11111111_11111111L;

    private static final int OFFSET_LOCK_WORD = 0; // 8 bytes
    private static final int OFFSET_ADDRESS = 8; // 8 bytes
    private static final int OFFSET_LAST_TX_ID = 16; // 8 bytes
    private static final int OFFSET_FILE_PAGE_ID = 24; // 5 bytes
    private static final int OFFSET_USAGE_COUNTER = 29; // 1 byte
    private static final int OFFSET_SWAPPER_ID = 30; // 2 bytes

    // todo we can alternatively also make use of the lower 12 bits of the address field, because
    // todo the addresses are page aligned, and we can assume them to be at least 4096 bytes in size.

    // xxx Thinking about it some more, it might even be possible to get down to just 16 bytes per page.
    // xxx We cannot change the lock word, but if we change the eviction scheme to always go through the translation
    // xxx tables to find pages, then we won't need the file page id. The address ought to have its lower 13 bits free.
    // xxx Two of these can go to the usage counter, and the rest to the swapper id, for up to 2048 swappers.
    // xxx Do we even need the swapper id at this point? If we can already infer the file page id, the same should be
    // xxx true for the swapper.
    // xxx The trouble with this idea is that we won't be able to seamlessly turn the translation tables into hash
    // xxx tables when they get too big - at least not easily - since the index into the table no longer corresponds to
    // xxx the file page id. We might still be able to get away with it, if with the page id we also keep a counter for
    // xxx how many times the file page id loop around the translation table before arriving at the given entry.
    // xxx That is, what the entry index should be multiplied by to get the file page id. To do this, we would, however,
    // xxx have to either grab bits from the page id space, or make each entry more than 4 bytes. This will all depend
    // xxx on where the cut-off point is. That is, what the max entry capacity for a translation table should be.
    // xxx One potential cut-off point could be 2^29. That many 4 byte translation table entries would take up 2 GiB.
    // xxx If we can then somehow put the wrap-around counter in the page meta-data by, for instance, taking a byte
    // xxx from the bits in the address that we are no longer using for the swapper id, then we can support 255
    // xxx wrap-arounds with bits to spare. This will allow us to address files that are up to 1 peta-byte in size.
    // xxx At such extremes we'd only be able to keep up to 1/256th of the file in memory, which is 4 TiB, which in
    // xxx turn is 1/8th of the 8192 * 2^32 = 32 TiB max memory capacity. To increase the potential utilisation, we can
    // xxx raise the cut-off point to up to 2^32, which would require 16 GiBs of memory to represent.
    // xxx Since we know up front how many pages we have at our disposal, and that 32 TiB of RAM is far from common,
    // xxx we can place our preferred cut-off point at one or two bit-widths higher than the required bits to address
    // xxx the memory. This would keep the risk of collisions down. Hopefully to a somewhat reasonable level.
    // xxx Actually, if we don't need the swapper id in the page list anymore, then we could use those 11 spare bits
    // xxx to store the wrap-around counter. We'd have to consult the page list before we know whether we've found the
    // xxx correct translation table entry or not, but that is hopefully a rare occurrence. We can also use a few of the
    // xxx bits to indicate the entry offset from its ideal location, such that collisions don't necessarily have to
    // xxx cause an existing entry to be evicted. The offset can either be as a difference from the given file page id –
    // xxx which might not work so well with scans, for instance – or it can be the levels of hashing, where a zero
    // xxx would mean that the file page id is unhashed (directly addressed), and any number above this is the number of
    // xxx recursive hashes that the file page id has gone through in search of a free translation table entry.
    // xxx Not having the swapper id will, however, make it impossible to reconstruct the translation tables from the
    // xxx page list, which would be required in order to resume a page cache from stored memory, e.g. a /dev/shm file.
    // xxx Perhaps, if we stored the buffers base address separately (the address of the first page buffer we allocate),
    // xxx and we then assume that 47 bits is enough to address all the memory we need (128 TiB of RAM addressable),
    // xxx then we would be able to get 64-47=17 spare bits. 10 of those bits could go to the swapper id (1024 swappers
    // xxx possible), 2 bits goes to the usage counter, 4 bits goes to the wrap-around counter allowing us to map files
    // xxx up to 512 TiB in size, and one bit for whether the file page id was rehashed or not.
    // xxx Store segments, if implemented, might change the dynamics around a bit; we might get an upper bound on file
    // xxx sizes, and instead have a lot more files. In such a case, we might want to drop the wrap-around idea for
    // xxx translation tables entirely, and instead invest those bits into the swapper id.

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
        this.baseAddress = memoryAllocator.allocateAligned( bytes, Long.BYTES );
        this.bufferAlignment = bufferAlignment;
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
            UnsafeUtil.putLong( address += Long.BYTES, MAX_FILE_PAGE_ID );
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
    public int getPageCount()
    {
        return pageCount;
    }

    public SwapperSet getSwappers()
    {
        return swappers;
    }

    /**
     * Turn a {@code pageId} into a {@code pageRef} that can be used for accessing and manipulating the given page
     * using the other methods in this class.
     * @param pageId The {@code pageId} to turn into a {@code pageRef}.
     * @return A {@code pageRef} which is an opaque, internal and direct pointer to the meta-data of the given memory
     * page.
     */
    public long deref( int pageId )
    {
        //noinspection UnnecessaryLocalVariable
        long id = pageId; // convert to long to avoid int multiplication
        return baseAddress + (id * META_DATA_BYTES_PER_PAGE);
    }

    public int toId( long pageRef )
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

    public long unlockWriteAndTryTakeFlushLock( long pageRef )
    {
        return OffHeapPageLock.unlockWriteAndTryTakeFlushLock( offLock( pageRef ) );
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
            long addr = memoryAllocator.allocateAligned( getCachePageSize(), bufferAlignment );
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
        int highByte = UnsafeUtil.getByte( offFilePageId( pageRef ) ) & UNSIGNED_BYTE_MASK;
        long lowInt = UnsafeUtil.getInt( offFilePageId( pageRef ) + Byte.BYTES ) & UNSIGNED_INT_MASK;
        long filePageId = (((long) highByte) << Integer.SIZE) | lowInt;
        return filePageId == MAX_FILE_PAGE_ID ? PageCursor.UNBOUND_PAGE_ID : filePageId;
    }

    private void setFilePageId( long pageRef, long filePageId )
    {
        if ( filePageId > MAX_FILE_PAGE_ID )
        {
            throw new IllegalArgumentException(
                    format( "File page id: %s is bigger then max supported value %s.", filePageId, MAX_FILE_PAGE_ID ) );
        }
        byte highByte = (byte) (filePageId >> Integer.SIZE);
        UnsafeUtil.putByte( offFilePageId( pageRef ), highByte );
        UnsafeUtil.putInt( offFilePageId( pageRef ) + Byte.BYTES, (int) filePageId );
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

    public short getSwapperId( long pageRef )
    {
        return UnsafeUtil.getShort( offSwapperId( pageRef ) );
    }

    private void setSwapperId( long pageRef, short swapperId )
    {
        UnsafeUtil.putShort( offSwapperId( pageRef ), swapperId );
    }

    public boolean isLoaded( long pageRef )
    {
        return getFilePageId( pageRef ) != PageCursor.UNBOUND_PAGE_ID;
    }

    public boolean isBoundTo( long pageRef, short swapperId, long filePageId )
    {
        return getSwapperId( pageRef ) == swapperId && getFilePageId( pageRef ) == filePageId;
    }

    public void fault( long pageRef, PageSwapper swapper, short swapperId, long filePageId, PageFaultEvent event )
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

    private static IllegalStateException cannotFaultException( long pageRef, PageSwapper swapper, short swapperId,
                                                        long filePageId, int currentSwapper, long currentFilePageId )
    {
        String msg = format(
                "Cannot fault page {filePageId = %s, swapper = %s (swapper id = %s)} into " +
                "cache page %s. Already bound to {filePageId = " +
                "%s, swapper id = %s}.",
                filePageId, swapper, swapperId, pageRef, currentFilePageId, currentSwapper );
        return new IllegalStateException( msg );
    }

    public boolean tryEvict( long pageRef, EvictionEventOpportunity evictionOpportunity ) throws IOException
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
        short swapperId = getSwapperId( pageRef );
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

    protected void clearBinding( long pageRef )
    {
        setFilePageId( pageRef, PageCursor.UNBOUND_PAGE_ID );
        setSwapperId( pageRef, (short) 0 );
    }

    public String toString( long pageRef )
    {
        StringBuilder sb = new StringBuilder();
        toString( pageRef, sb );
        return sb.toString();
    }

    public void toString( long pageRef, StringBuilder sb )
    {
        sb.append( "Page[ id = " ).append( toId( pageRef ) );
        sb.append( ", address = " ).append( getAddress( pageRef ) );
        sb.append( ", filePageId = " ).append( getFilePageId( pageRef ) );
        sb.append( ", swapperId = " ).append( getSwapperId( pageRef ) );
        sb.append( ", usageCounter = " ).append( getUsageCounter( pageRef ) );
        sb.append( " ] " ).append( OffHeapPageLock.toString( offLock( pageRef ) ) );
    }
}
