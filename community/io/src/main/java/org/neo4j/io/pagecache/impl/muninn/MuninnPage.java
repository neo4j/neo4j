/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.MemoryManager;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static java.lang.String.format;

final class MuninnPage extends SequenceLock implements Page
{
    private static final long usageStampOffset = UnsafeUtil.getFieldOffset( MuninnPage.class, "usageStamp" );

    // The sign bit is used as a dirty flag for the page.
    // The other 7 bits are used as an exponent for computing the cache page size (as a power of two).
    private byte cachePageHeader;

    // We keep this reference to prevent the MemoryManager from becoming
    // finalizable until all our pages are finalizable or collected.
    private final MemoryManager memoryManager;

    private long pointer;

    // Optimistically incremented; occasionally truncated to a max of 4.
    // accessed through unsafe
    @SuppressWarnings( "unused" )
    private volatile byte usageStamp;

    // Next pointer in the freelist of available pages. This is either a
    // MuninnPage object, or a FreePage object. See the comment on the
    // MuninnPageCache.freelist field.
    public Object nextFree;

    private PageSwapper swapper;
    private long filePageId = PageCursor.UNBOUND_PAGE_ID;

    MuninnPage( int cachePageSize, MemoryManager memoryManager )
    {
        this.cachePageHeader = (byte) (31 - Integer.numberOfLeadingZeros( cachePageSize ));
        this.memoryManager = memoryManager;
        getCachePageId(); // initialize our identity hashCode
    }

    public int getCachePageId()
    {
        return System.identityHashCode( this );
    }

    @Override
    public int size()
    {
        return 1 << (cachePageHeader & 0x7F);
    }

    @Override
    public long address()
    {
        return pointer;
    }

    /**
     * NOTE: Should be called under a page lock.
     */
    boolean isDirty()
    {
        return (cachePageHeader & ~0x7F) != 0;
    }

    public void markAsDirty()
    {
        cachePageHeader |= ~0x7F;
    }

    public void markAsClean()
    {
        cachePageHeader &= 0x7F;
    }

    /** Increment the usage stamp to at most 4. */
    public void incrementUsage()
    {
        // This is intentionally left benignly racy for performance.
        byte usage = getUsageCounter();
        if ( usage < 4 ) // avoid cache sloshing by not doing a write if counter is already maxed out
        {
            usage <<= 1;
            usage++; // Raise at least one bit in case it was all zeros.
            usage &= 0x0F;
            UnsafeUtil.putByteVolatile( this, usageStampOffset, usage );
        }
    }

    /** Decrement the usage stamp. Returns true if it reaches 0. */
    public boolean decrementUsage()
    {
        // This is intentionally left benignly racy for performance.
        byte usage = getUsageCounter();
        usage >>>= 1;
        UnsafeUtil.putByteVolatile( this, usageStampOffset, usage );
        return usage == 0;
    }

    private byte getUsageCounter()
    {
        return UnsafeUtil.getByteVolatile( this, usageStampOffset );
    }

    /**
     * NOTE: This method must be called while holding an exclusive lock on the page.
     */
    public void flush( FlushEventOpportunity flushOpportunity ) throws IOException
    {
        if ( swapper != null && isDirty() )
        {
            // The page is bound and has stuff to flush
            doFlush( swapper, filePageId, flushOpportunity );
        }
    }

    private void doFlush(
            PageSwapper swapper,
            long filePageId,
            FlushEventOpportunity flushOpportunity ) throws IOException
    {
        FlushEvent event = flushOpportunity.beginFlush( filePageId, getCachePageId(), swapper );
        try
        {
            long bytesWritten = swapper.write( filePageId, this );
            markAsClean();
            event.addBytesWritten( bytesWritten );
            event.done();
        }
        catch ( IOException e )
        {
            event.done( e );
            throw e;
        }
    }

    void markAsLoaded( long filePageId )
    {
        this.filePageId = filePageId;
    }

    void markAsBound( PageSwapper swapper )
    {
        this.swapper = swapper;
    }

    /**
     * NOTE: This method MUST be called while holding the exclusive page lock.
     */
    public void fault(
            PageSwapper swapper,
            long filePageId,
            PageFaultEvent faultEvent ) throws IOException
    {
        if ( this.swapper != null || this.filePageId != PageCursor.UNBOUND_PAGE_ID )
        {
            String msg = format(
                    "Cannot fault page {filePageId = %s, swapper = %s} into " +
                    "cache page %s. Already bound to {filePageId = " +
                    "%s, swapper = %s}.",
                    filePageId, swapper, getCachePageId(), this.filePageId, this.swapper );
            throw new IllegalStateException( msg );
        }

        // Note: It is important that we assign the filePageId before we swap
        // the page in. If the swapping fails, the page will be considered
        // loaded for the purpose of eviction, and will eventually return to
        // the freelist. However, because we don't assign the swapper until the
        // swapping-in has succeeded, the page will not be considered bound to
        // the file page, so any subsequent thread that finds the page in their
        // translation table will re-do the page fault.
        this.filePageId = filePageId; // Page now considered isLoaded()
        long bytesRead = swapper.read( filePageId, this );
        faultEvent.addBytesRead( bytesRead );
        faultEvent.setCachePageId( getCachePageId() );
        this.swapper = swapper; // Page now considered isBoundTo( swapper, filePageId )
    }

    /**
     * NOTE: This method MUST be called while holding the page write lock.
     */
    public void evict( EvictionEvent evictionEvent ) throws IOException
    {
        long filePageId = this.filePageId;
        evictionEvent.setCachePageId( getCachePageId() );
        evictionEvent.setFilePageId( filePageId );
        PageSwapper swapper = this.swapper;
        evictionEvent.setSwapper( swapper );

        flush( evictionEvent.flushEventOpportunity() );
        this.filePageId = PageCursor.UNBOUND_PAGE_ID;

        this.swapper = null;
        if ( swapper != null )
        {
            // The swapper can be null if the last page fault
            // that page threw an exception.
            swapper.evicted( filePageId, this );
        }
    }

    public boolean isLoaded()
    {
        return filePageId != PageCursor.UNBOUND_PAGE_ID;
    }

    public boolean isBoundTo( PageSwapper swapper, long filePageId )
    {
        return this.swapper == swapper && this.filePageId == filePageId;
    }

    /**
     * NOTE: This method MUST be called while holding the page write lock.
     */
    public void initBuffer()
    {
        if ( pointer == 0 )
        {
            pointer = memoryManager.allocateAligned( size() );
        }
    }

    public long getFilePageId()
    {
        return filePageId;
    }

    @Override
    public String toString()
    {
        return format( "MuninnPage@%x[%s -> %x, filePageId = %s%s, swapper = %s, usage counter = %s, %s]",
                hashCode(), getCachePageId(), pointer, filePageId, isDirty() ? ", dirty" : "",
                swapper, getUsageCounter(), super.toString() );
    }
}
