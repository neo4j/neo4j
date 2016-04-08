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

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

abstract class MuninnPageCursor implements PageCursor
{
    private static final boolean tracePinnedCachePageId =
            flag( MuninnPageCursor.class, "tracePinnedCachePageId", false );

    // Size of the respective primitive types in bytes.
    private static final int SIZE_OF_BYTE = Byte.BYTES;
    private static final int SIZE_OF_SHORT = Short.BYTES;
    private static final int SIZE_OF_INT = Integer.BYTES;
    private static final int SIZE_OF_LONG = Long.BYTES;

    private final long victimPage;
    protected MuninnPagedFile pagedFile;
    protected PageSwapper swapper;
    protected PageCacheTracer tracer;
    protected MuninnPage page;
    protected PinEvent pinEvent;
    protected long pageId;
    protected int pf_flags;
    protected long currentPageId;
    protected long nextPageId;
    protected PageCursor linkedCursor;
    private long pointer;
    private int pageSize;
    private int filePageSize;
    private int offset;
    private boolean outOfBounds;

    MuninnPageCursor( long victimPage )
    {
        this.victimPage = victimPage;
        pointer = victimPage;
    }

    final void initialiseFile( MuninnPagedFile pagedFile )
    {
        this.swapper = pagedFile.swapper;
        this.tracer = pagedFile.tracer;
    }

    final void initialiseFlags( MuninnPagedFile pagedFile, long pageId, int pf_flags )
    {
        this.pagedFile = pagedFile;
        this.pageId = pageId;
        this.pf_flags = pf_flags;
        this.filePageSize = pagedFile.filePageSize;
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
        this.pageSize = filePageSize;
        if ( tracePinnedCachePageId )
        {
            pinEvent.setCachePageId( page.getCachePageId() );
        }
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
        releaseCursor();
        closeLinkedCursorIfAny();
        // We null out the pagedFile field to allow it and its (potentially big) translation table to be garbage
        // collected when the file is unmapped, since the cursors can stick around in thread local caches, etc.
        pagedFile = null;
    }

    private void closeLinkedCursorIfAny()
    {
        if ( linkedCursor != null )
        {
            linkedCursor.close();
            linkedCursor = null;
        }
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        closeLinkedCursorIfAny();
        linkedCursor =  pagedFile.io( pageId, pf_flags );
        return linkedCursor;
    }

    /**
     * Must be called by {@link #unpinCurrentPage()}.
     */
    void clearPageState()
    {
        pointer = victimPage; // make all future page access go to the victim page
        pageSize = 0; // make all future bound checks fail
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
     * @param writeLock 'true' if we will be taking a write lock on the page as part of the pin.
     * @throws IOException if anything goes wrong with the pin, most likely during a page fault.
     */
    protected void pin( long filePageId, boolean writeLock ) throws IOException
    {
        pinEvent = tracer.beginPin( writeLock, filePageId, swapper );
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
                boolean locked = tryLockPage( page );
                if ( locked & page.isBoundTo( swapper, filePageId ) )
                {
                    pinCursorToPage( page, filePageId, swapper );
                    return;
                }
                if ( locked )
                {
                    unlockPage( page );
                }
                item = null;
            }
            else
            {
                item = uncommonPin( item, filePageId, chunkOffset, chunk );
            }
        }
        while ( item == null );
        pinCursorToPage( (MuninnPage) item, filePageId, swapper );
    }

    private Object[][] expandTranslationTableCapacity( int chunkId )
    {
        return pagedFile.expandCapacity( chunkId );
    }

    private Object uncommonPin( Object item, long filePageId, long chunkOffset, Object[] chunk ) throws IOException
    {
        if ( item == null )
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
        return item;
    }

    private Object initiatePageFault( long filePageId, long chunkOffset, Object[] chunk ) throws IOException
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
        try
        {
            // The grabFreePage method might throw.
            page = pagedFile.grabFreeAndExclusivelyLockedPage( faultEvent );

            // We got a free page, and we know that we have race-free access to it. Well, it's not entirely race
            // free, because other paged files might have it in their translation tables (or rather, their reads of
            // their translation tables might race with eviction) and try to pin it.
            // However, they will all fail because when they try to pin, because the page will be exclusively locked
            // and possibly bound to our page.
        }
        catch ( Throwable throwable )
        {
            // Make sure to unstuck the page fault latch.
            abortPageFault( throwable, chunk, chunkOffset, latch, faultEvent );
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
            page.unlockExclusive();
            // Make sure to unstuck the page fault latch.
            abortPageFault( throwable, chunk, chunkOffset, latch, faultEvent );
            throw throwable;
        }
        // Put the page in the translation table before we undo the exclusive lock, as we could otherwise race with
        // eviction, and the onEvict callback expects to find a MuninnPage object in the table.
        UnsafeUtil.putObjectVolatile( chunk, chunkOffset, page );
        // Once we page has been published to the translation table, we can convert our exclusive lock to whatever we
        // need for the page cursor.
        convertPageFaultLock( page );
        latch.release();
        faultEvent.done();
        return page;
    }

    private void abortPageFault( Throwable throwable, Object[] chunk, long chunkOffset,
                                 BinaryLatch latch,
                                 PageFaultEvent faultEvent ) throws IOException
    {
        UnsafeUtil.putObjectVolatile( chunk, chunkOffset, null );
        latch.release();
        faultEvent.done( throwable );
        pinEvent.done();
    }

    long assertPagedFileStillMappedAndGetIdOfLastPage()
    {
        return pagedFile.getLastPageId();
    }

    protected abstract void unpinCurrentPage();

    protected abstract void convertPageFaultLock( MuninnPage page );

    protected abstract void pinCursorToPage( MuninnPage page, long filePageId, PageSwapper swapper );

    protected abstract boolean tryLockPage( MuninnPage page );

    protected abstract void unlockPage( MuninnPage page );

    protected abstract void releaseCursor();

    // --- IO methods:

    /**
     * Compute a pointer that guarantees (assuming {@code size} is less than or equal to {@link #pageSize}) that the
     * page access will be within the bounds of the page.
     * This might mean that the pointer won't point to where one might naively expect, but will instead be
     * truncated to point within the page. In this case, an overflow has happened and the {@link #outOfBounds}
     * flag will be raised.
     */
    private long getBoundedPointer( int offset, int size )
    {
        long can = pointer + offset;
        long lim = pointer + pageSize - size;
        long ref = Math.min( can, lim );
        ref = Math.max( ref, pointer );
        outOfBounds |= ref != can | lim < pointer;
        return ref;
    }

    @Override
    public final byte getByte()
    {
        long p = getBoundedPointer( offset, SIZE_OF_BYTE );
        byte b = UnsafeUtil.getByte( p );
        offset++;
        return b;
    }

    @Override
    public byte getByte( int offset )
    {
        long p = getBoundedPointer( offset, SIZE_OF_BYTE );
        return UnsafeUtil.getByte( p );
    }

    @Override
    public void putByte( byte value )
    {
        long p = getBoundedPointer( offset, SIZE_OF_BYTE );
        UnsafeUtil.putByte( p, value );
        offset++;
    }

    @Override
    public void putByte( int offset, byte value )
    {
        long p = getBoundedPointer( offset, SIZE_OF_BYTE );
        UnsafeUtil.putByte( p, value );
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
        long p = getBoundedPointer( offset, SIZE_OF_LONG );
        long value;
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            value = UnsafeUtil.getLong( p );
            if ( !UnsafeUtil.storeByteOrderIsNative )
            {
                value = Long.reverseBytes( value );
            }
        }
        else
        {
            value = getLongBigEndian( p );
        }
        return value;
    }

    private long getLongBigEndian( long p )
    {
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
        long p = getBoundedPointer( offset, SIZE_OF_LONG );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            UnsafeUtil.putLong( p, UnsafeUtil.storeByteOrderIsNative ? value : Long.reverseBytes( value ) );
        }
        else
        {
            putLongBigEndian( value, p );
        }
    }

    private void putLongBigEndian( long value, long p )
    {
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
        long p = getBoundedPointer( offset, SIZE_OF_INT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            int x = UnsafeUtil.getInt( p );
            return UnsafeUtil.storeByteOrderIsNative ? x : Integer.reverseBytes( x );
        }
        return getIntBigEndian( p );
    }

    private int getIntBigEndian( long p )
    {
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
        long p = getBoundedPointer( offset, SIZE_OF_INT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            UnsafeUtil.putInt( p, UnsafeUtil.storeByteOrderIsNative ? value : Integer.reverseBytes( value ) );
        }
        else
        {
            putIntBigEndian( value, p );
        }
    }

    private void putIntBigEndian( int value, long p )
    {
        UnsafeUtil.putByte( p    , (byte)( value >> 24 ) );
        UnsafeUtil.putByte( p + 1, (byte)( value >> 16 ) );
        UnsafeUtil.putByte( p + 2, (byte)( value >> 8  ) );
        UnsafeUtil.putByte( p + 3, (byte)( value       ) );
    }

    @Override
    public void getBytes( byte[] data )
    {
        getBytes( data, 0, data.length );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        long p = getBoundedPointer( offset, length );
        if ( !outOfBounds )
        {
            for ( int i = 0; i < length; i++ )
            {
                data[arrayOffset + i] = UnsafeUtil.getByte( p + i );
            }
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
        long p = getBoundedPointer( offset, length );
        if ( !outOfBounds )
        {
            for ( int i = 0; i < length; i++ )
            {
                byte b = data[arrayOffset + i];
                UnsafeUtil.putByte( p + i, b );
            }
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
        long p = getBoundedPointer( offset, SIZE_OF_SHORT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            short x = UnsafeUtil.getShort( p );
            return UnsafeUtil.storeByteOrderIsNative ? x : Short.reverseBytes( x );
        }
        return getShortBigEndian( p );
    }

    private short getShortBigEndian( long p )
    {
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
        long p = getBoundedPointer( offset, SIZE_OF_SHORT );
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            UnsafeUtil.putShort( p, UnsafeUtil.storeByteOrderIsNative ? value : Short.reverseBytes( value ) );
        }
        else
        {
            putShortBigEndian( value, p );
        }
    }

    private void putShortBigEndian( short value, long p )
    {
        UnsafeUtil.putByte( p    , (byte)( value >> 8 ) );
        UnsafeUtil.putByte( p + 1, (byte)( value      ) );
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        int sourcePageSize = getCurrentPageSize();
        int targetPageSize = targetCursor.getCurrentPageSize();
        if ( targetCursor.getClass() != MuninnWritePageCursor.class )
        {
            throw new IllegalArgumentException( "Target cursor must be writable" );
        }
        if ( sourceOffset >= 0
             & targetOffset >= 0
             & sourceOffset < sourcePageSize
             & targetOffset < targetPageSize
             & lengthInBytes > 0 )
        {
            MuninnPageCursor cursor = (MuninnPageCursor) targetCursor;
            int remainingSource = sourcePageSize - sourceOffset;
            int remainingTarget = targetPageSize - targetOffset;
            int bytes = Math.min( lengthInBytes, Math.min( remainingSource, remainingTarget ) );
            UnsafeUtil.copyMemory( pointer + sourceOffset, cursor.pointer + targetOffset, bytes );
            return bytes;
        }
        outOfBounds = true;
        return 0;
    }

    @Override
    public void setOffset( int offset )
    {
        this.offset = offset;
    }

    @Override
    public final int getOffset()
    {
        return offset;
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        boolean b = outOfBounds;
        outOfBounds = false;
        return b | (linkedCursor != null && linkedCursor.checkAndClearBoundsFlag());
    }

    @Override
    public void raiseOutOfBounds()
    {
        outOfBounds = true;
    }
}
