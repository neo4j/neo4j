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

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

abstract class MuninnPageCursor implements PageCursor
{
    // Size of the respective primitive types in bytes.
    private static final int SIZE_OF_BYTE = Byte.BYTES;
    private static final int SIZE_OF_SHORT = Short.BYTES;
    private static final int SIZE_OF_INT = Integer.BYTES;
    private static final int SIZE_OF_LONG = Long.BYTES;

    protected MuninnPagedFile pagedFile;
    protected PageSwapper swapper;
    protected PageCacheTracer tracer;
    protected MuninnPage page;
    protected PinEvent pinEvent;
    protected long pageId;
    protected int pf_flags;
    protected long currentPageId;
    protected long nextPageId;
    protected long lockStamp;
    private long pointer;
    private int size;

    private boolean claimed;
    private int offset;

    public final void initialise( MuninnPagedFile pagedFile, long pageId, int pf_flags )
    {
        this.pagedFile = pagedFile;
        this.swapper = pagedFile.swapper;
        this.tracer = pagedFile.tracer;
        this.pageId = pageId;
        this.pf_flags = pf_flags;
    }

    public final void markAsClaimed()
    {
        claimed = true;
    }

    public final void assertUnclaimed()
    {
        if ( claimed )
        {
            throw new IllegalStateException(
                    "Cannot operate on more than one PageCursor at a time," +
                            " because it is prone to deadlocks" );
        }
    }

    @Override
    public final void rewind()
    {
        nextPageId = pageId;
        currentPageId = UNBOUND_PAGE_ID;
    }

    public final void reset( MuninnPage page )
    {
        this.page = page;
        this.offset = 0;
        this.pointer = page.address();
        this.size = page.size();
        pinEvent.setCachePageId( page.getCachePageId() );
    }

    @Override
    public final boolean next( long pageId ) throws IOException
    {
        nextPageId = pageId;
        return next();
    }

    @Override
    public final void close()
    {
        unpinCurrentPage();
        pagedFile = null;
        claimed = false;
    }

    /**
     * Must be called by {@link #unpinCurrentPage()}.
     */
    protected void clearPageState()
    {
        size = 0; // make all future bound checks fail
        lockStamp = 0; // make sure not to accidentally keep a lock state around
        page = null; // make all future page navigation fail
    }

    @Override
    public final long getCurrentPageId()
    {
        return currentPageId;
    }

    @Override
    public final int getCurrentPageSize()
    {
        return currentPageId == UNBOUND_PAGE_ID?
               UNBOUND_PAGE_SIZE : pagedFile.pageSize();
    }

    @Override
    public final File getCurrentFile()
    {
        return currentPageId == UNBOUND_PAGE_ID? null : pagedFile.file();
    }

    /**
     * Pin the desired file page to this cursor, page faulting it into memory if it isn't there already.
     * @param filePageId The file page id we want to pin this cursor to.
     * @param exclusive 'true' if we will be taking an exclusive lock on the page as part of the pin.
     * @throws IOException if anything goes wrong with the pin, most likely during a page fault.
     */
    protected void pin( long filePageId, boolean exclusive ) throws IOException
    {
        pinEvent = tracer.beginPin( exclusive, filePageId, swapper );
        int chunkId = MuninnPagedFile.computeChunkId( filePageId );
        // The chunkOffset is the addressing offset into the chunk array object for the relevant array slot. Using
        // this, we can access the array slot with Unsafe.
        long chunkOffset = MuninnPagedFile.computeChunkOffset( filePageId );
        Object[][] tt = pagedFile.translationTable;
        if ( tt.length <= chunkId )
        {
            tt = expandTranslationTableCapacity( chunkId );
        }
        Object[] chunk = tt[chunkId];

        // Now, if the reference in the chunk slot is a latch, we wait on it and look up again (in a loop, since the
        // page might get evicted right after the page fault completes). If we find a page, we lock it and check its
        // binding (since it might get evicted and faulted into something else in the time between our look up and
        // our locking of the page). If the reference is null or it referred to a page that had wrong bindings, we CAS
        // in a latch. If that CAS succeeds, we page fault, set the slot to the faulted in page and open the latch.
        // If the CAS failed, we retry the look up and start over from the top.
        Object item;
        do
        {
            item = UnsafeUtil.getObjectVolatile( chunk, chunkOffset );
            if ( item != null && item.getClass() == MuninnPage.class )
            {
                // We got *a* page, but we might be racing with eviction. To cope with that, we have to take some
                // kind of lock on the page, and check that it is indeed bound to what we expect. If not, then it has
                // been evicted, and possibly even page faulted into something else. In this case, we discard the
                // item and try again, as the eviction thread would have set the chunk array slot to null.
                MuninnPage page = (MuninnPage) item;
                lockPage( page );
                if ( !page.isBoundTo( swapper, filePageId ) )
                {
                    unlockPage( page );
                    item = null;
                }
            }
            else if ( item == null )
            {
                // Looks like there's no mapping, so we'd like to do a page fault.
                item = initiatePageFault( filePageId, chunkOffset, chunk );
            }
            else
            {
                // We found a latch, so someone else is already doing a page fault for this page. So we'll just wait
                // for them to finish, and grab the page then.
                item = awaitPageFault( item );
            }
        }
        while ( item == null );
        pinCursorToPage( (MuninnPage) item, filePageId, swapper );
    }

    private Object[][] expandTranslationTableCapacity( int chunkId )
    {
        return pagedFile.expandCapacity( chunkId );
    }

    private Object initiatePageFault( long filePageId, long chunkOffset, Object[] chunk )
            throws IOException
    {
        BinaryLatch latch = new BinaryLatch();
        Object item = null;
        if ( UnsafeUtil.compareAndSwapObject( chunk, chunkOffset, null, latch ) )
        {
            // We managed to inject our latch, so we now own the right to perform the page fault. We also
            // have a duty to eventually release and remove the latch, no matter what happens now.
            item = pageFault( filePageId, swapper, chunkOffset, chunk, latch );
        }
        return item;
    }

    private Object awaitPageFault( Object item )
    {
        BinaryLatch latch = (BinaryLatch) item;
        latch.await();
        return null;
    }

    private MuninnPage pageFault(
            long filePageId, PageSwapper swapper, long chunkOffset, Object[] chunk, BinaryLatch latch )
            throws IOException
    {
        // We are page faulting. This is a critical time, because we currently have the given latch in the chunk array
        // slot that we are faulting into. We MUST make sure to release that latch, and remove it from the chunk, no
        // matter what happens. Otherwise other threads will get stuck waiting forever for our page fault to finish.
        // If we manage to get a free page to fault into, then we will also be taking a write lock on that page, to
        // protect it against concurrent eviction as we assigning a binding to the page. If anything goes wrong, then
        // we must make sure to release that write lock as well.
        PageFaultEvent faultEvent = pinEvent.beginPageFault();
        MuninnPage page;
        long stamp;
        try
        {
            // The grabFreePage method might throw.
            page = pagedFile.grabFreePage( faultEvent );

            // We got a free page, and we know that we have race-free access to it. Well, it's not entirely race
            // free, because other paged files might have it in their translation tables (or rather, their reads of
            // their translation tables might race with eviction) and try to pin it.
            // However, they will all fail because when they try to pin, the page will either be 1) free, 2) bound to
            // our file, or 3) the page is write locked.
            stamp = page.writeLock();
        }
        catch ( Throwable throwable )
        {
            // Make sure to unstuck the page fault latch.
            UnsafeUtil.putObjectVolatile( chunk, chunkOffset, null );
            latch.release();
            faultEvent.done( throwable );
            pinEvent.done();
            // We don't need to worry about the 'stamp' here, because the writeLock call is uninterruptible, so it
            // can't really fail.
            throw throwable;
        }
        try
        {
            // Check if we're racing with unmapping. We have the page lock
            // here, so the unmapping would have already happened. We do this
            // check before page.fault(), because that would otherwise reopen
            // the file channel.
            assertPagedFileStillMappedAndGetIdOfLastPage();
            page.initBuffer();
            page.fault( swapper, filePageId, faultEvent );
        }
        catch ( Throwable throwable )
        {
            // Make sure to unlock the page, so the eviction thread can pick up our trash.
            page.unlockWrite( stamp );
            // Make sure to unstuck the page fault latch.
            UnsafeUtil.putObjectVolatile( chunk, chunkOffset, null );
            latch.release();
            faultEvent.done( throwable );
            pinEvent.done();
            throw throwable;
        }
        convertPageFaultLock( page, stamp );
        UnsafeUtil.putObjectVolatile( chunk, chunkOffset, page );
        latch.release();
        faultEvent.done();
        return page;
    }

    protected long assertPagedFileStillMappedAndGetIdOfLastPage()
    {
        return pagedFile.getLastPageId();
    }

    protected abstract void unpinCurrentPage();

    protected abstract void convertPageFaultLock( MuninnPage page, long stamp );

    protected abstract void pinCursorToPage( MuninnPage page, long filePageId, PageSwapper swapper );

    protected abstract void lockPage( MuninnPage page );

    protected abstract void unlockPage( MuninnPage page );

    // --- IO methods:

    private void checkBounds( int position )
    {
        if ( position > size )
        {
            throw new IndexOutOfBoundsException( getOutOfBoundsMessage( position ) );
        }
    }

    private String getOutOfBoundsMessage( int position )
    {
        if ( size > 0 )
        {
            return "Position " + position + " is greater than the upper " +
                   "page size bound of " + size;
        }
        else
        {
            return "The PageCursor is not bound to a page. " +
                   "Maybe next() returned false or was not called, or the cursor has been closed.";
        }
    }

    @Override
    public final byte getByte()
    {
        checkBounds( offset + SIZE_OF_BYTE );
        byte b = UnsafeUtil.getByte( pointer + offset );
        offset++;
        return b;
    }

    @Override
    public byte getByte( int offset )
    {
        checkBounds( offset + SIZE_OF_BYTE );
        return UnsafeUtil.getByte( pointer + offset );
    }

    @Override
    public void putByte( byte value )
    {
        checkBounds( offset + SIZE_OF_BYTE );
        UnsafeUtil.putByte( pointer + offset, value );
        offset++;
    }

    @Override
    public void putByte( int offset, byte value )
    {
        checkBounds( offset + SIZE_OF_BYTE );
        UnsafeUtil.putByte( pointer + offset, value );
    }

    @Override
    public long getLong()
    {
        long value = getLong( offset );
        offset += SIZE_OF_LONG;
        return value;
    }

    @Override
    public long getLong( int offset )
    {
        checkBounds( offset + SIZE_OF_LONG );
        long value;
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            value = UnsafeUtil.getLong( pointer + offset );
            if ( !UnsafeUtil.storeByteOrderIsNative )
            {
                value = Long.reverseBytes( value );
            }
        }
        else
        {
            value = getLongBigEndian( offset );
        }
        return value;
    }

    private long getLongBigEndian( int offset )
    {
        long p = pointer + offset;
        long a = UnsafeUtil.getByte( p     ) & 0xFF;
        long b = UnsafeUtil.getByte( p + 1 ) & 0xFF;
        long c = UnsafeUtil.getByte( p + 2 ) & 0xFF;
        long d = UnsafeUtil.getByte( p + 3 ) & 0xFF;
        long e = UnsafeUtil.getByte( p + 4 ) & 0xFF;
        long f = UnsafeUtil.getByte( p + 5 ) & 0xFF;
        long g = UnsafeUtil.getByte( p + 6 ) & 0xFF;
        long h = UnsafeUtil.getByte( p + 7 ) & 0xFF;
        return (a << 56) | (b << 48) | (c << 40) | (d << 32) | (e << 24) | (f << 16) | (g << 8) | h;
    }

    @Override
    public void putLong( long value )
    {
        putLong( offset, value );
        offset += SIZE_OF_LONG;
    }

    @Override
    public void putLong( int offset, long value )
    {
        checkBounds( offset + SIZE_OF_LONG );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            long p = pointer + offset;
            UnsafeUtil.putLong( p, UnsafeUtil.storeByteOrderIsNative ? value : Long.reverseBytes( value ) );
        }
        else
        {
            putLongBigEndian( value, offset );
        }
    }

    private void putLongBigEndian( long value, int offset )
    {
        long p = pointer + offset;
        UnsafeUtil.putByte( p    , (byte)( value >> 56 ) );
        UnsafeUtil.putByte( p + 1, (byte)( value >> 48 ) );
        UnsafeUtil.putByte( p + 2, (byte)( value >> 40 ) );
        UnsafeUtil.putByte( p + 3, (byte)( value >> 32 ) );
        UnsafeUtil.putByte( p + 4, (byte)( value >> 24 ) );
        UnsafeUtil.putByte( p + 5, (byte)( value >> 16 ) );
        UnsafeUtil.putByte( p + 6, (byte)( value >> 8  ) );
        UnsafeUtil.putByte( p + 7, (byte)( value       ) );
    }

    @Override
    public int getInt()
    {
        int i = getInt( offset );
        offset += SIZE_OF_INT;
        return i;
    }

    @Override
    public int getInt( int offset )
    {
        checkBounds( offset + SIZE_OF_INT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            int x = UnsafeUtil.getInt( pointer + offset );
            return UnsafeUtil.storeByteOrderIsNative ? x : Integer.reverseBytes( x );
        }
        return getIntBigEndian( offset );
    }

    private int getIntBigEndian( int offset )
    {
        long p = pointer + offset;
        int a = UnsafeUtil.getByte( p     ) & 0xFF;
        int b = UnsafeUtil.getByte( p + 1 ) & 0xFF;
        int c = UnsafeUtil.getByte( p + 2 ) & 0xFF;
        int d = UnsafeUtil.getByte( p + 3 ) & 0xFF;
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    @Override
    public void putInt( int value )
    {
        putInt( offset, value );
        offset += SIZE_OF_INT;
    }

    @Override
    public void putInt( int offset, int value )
    {
        checkBounds( offset + SIZE_OF_INT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            long p = pointer + offset;
            UnsafeUtil.putInt( p, UnsafeUtil.storeByteOrderIsNative ? value : Integer.reverseBytes( value ) );
        }
        else
        {
            putIntBigEndian( value, offset );
        }
    }

    private void putIntBigEndian( int value, int offset )
    {
        long p = pointer + offset;
        UnsafeUtil.putByte( p    , (byte)( value >> 24 ) );
        UnsafeUtil.putByte( p + 1, (byte)( value >> 16 ) );
        UnsafeUtil.putByte( p + 2, (byte)( value >> 8  ) );
        UnsafeUtil.putByte( p + 3, (byte)( value       ) );
    }

    @Override
    public long getUnsignedInt()
    {
        return getInt() & 0xFFFFFFFFL;
    }

    @Override
    public long getUnsignedInt( int offset )
    {
        return getInt( offset ) & 0xFFFFFFFFL;
    }

    @Override
    public void getBytes( byte[] data )
    {
        getBytes( data, 0, data.length );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        checkBounds( offset + length );
        long address = pointer + offset;
        for ( int i = 0; i < length; i++ )
        {
            data[arrayOffset + i] = UnsafeUtil.getByte( address + i );
        }
        offset += length;
    }

    @Override
    public final void putBytes( byte[] data )
    {
        putBytes( data, 0, data.length );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        checkBounds( offset + length );
        long address = pointer + offset;
        for ( int i = 0; i < length; i++ )
        {
            byte b = data[arrayOffset + i];
            UnsafeUtil.putByte( address + i, b );
        }
        offset += length;
    }

    @Override
    public final short getShort()
    {
        short s = getShort( offset );
        offset += SIZE_OF_SHORT;
        return s;
    }

    @Override
    public short getShort( int offset )
    {
        checkBounds( offset + SIZE_OF_SHORT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            short x = UnsafeUtil.getShort( pointer + offset );
            return UnsafeUtil.storeByteOrderIsNative ? x : Short.reverseBytes( x );
        }
        return getShortBigEndian( offset );
    }

    private short getShortBigEndian( int offset )
    {
        long p = pointer + offset;
        short a = (short) (UnsafeUtil.getByte( p     ) & 0xFF);
        short b = (short) (UnsafeUtil.getByte( p + 1 ) & 0xFF);
        return (short) ((a << 8) | b);
    }

    @Override
    public void putShort( short value )
    {
        putShort( offset, value );
        offset += SIZE_OF_SHORT;
    }

    @Override
    public void putShort( int offset, short value )
    {
        checkBounds( offset + SIZE_OF_SHORT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            long p = pointer + offset;
            UnsafeUtil.putShort( p, UnsafeUtil.storeByteOrderIsNative ? value : Short.reverseBytes( value ) );
        }
        else
        {
            putShortBigEndian( value, offset );
        }
    }

    private void putShortBigEndian( short value, int offset )
    {
        long p = pointer + offset;
        UnsafeUtil.putByte( p    , (byte)( value >> 8 ) );
        UnsafeUtil.putByte( p + 1, (byte)( value      ) );
    }

    @Override
    public void setOffset( int offset )
    {
        if ( offset < 0 )
        {
            throw new IndexOutOfBoundsException();
        }
        this.offset = offset;
    }

    @Override
    public final int getOffset()
    {
        return offset;
    }
}
