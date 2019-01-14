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
import java.nio.ByteBuffer;
import java.util.Objects;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.neo4j.io.pagecache.PagedFile.PF_EAGER_FLUSH;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPagedFile.UNMAPPED_TTE;
import static org.neo4j.util.FeatureToggles.flag;

abstract class MuninnPageCursor extends PageCursor
{
    private static final boolean usePreciseCursorErrorStackTraces =
            flag( MuninnPageCursor.class, "usePreciseCursorErrorStackTraces", false );

    private static final boolean boundsCheck = flag( MuninnPageCursor.class, "boundsCheck", true );

    // Size of the respective primitive types in bytes.
    private static final int SIZE_OF_BYTE = Byte.BYTES;
    private static final int SIZE_OF_SHORT = Short.BYTES;
    private static final int SIZE_OF_INT = Integer.BYTES;
    private static final int SIZE_OF_LONG = Long.BYTES;

    private final long victimPage;
    private final PageCursorTracer tracer;
    protected MuninnPagedFile pagedFile;
    protected PageSwapper swapper;
    protected int swapperId;
    protected long pinnedPageRef;
    protected PinEvent pinEvent;
    protected long pageId;
    protected int pf_flags;
    protected boolean eagerFlush;
    protected boolean noFault;
    protected boolean noGrow;
    protected long currentPageId;
    protected long nextPageId;
    protected MuninnPageCursor linkedCursor;
    private long pointer;
    private int pageSize;
    private int filePageSize;
    protected final VersionContextSupplier versionContextSupplier;
    private int offset;
    private boolean outOfBounds;
    private boolean isLinkedCursor;
    // This is a String with the exception message if usePreciseCursorErrorStackTraces is false, otherwise it is a
    // CursorExceptionWithPreciseStackTrace with the message and stack trace pointing more or less directly at the
    // offending code.
    private Object cursorException;

    MuninnPageCursor( long victimPage, PageCursorTracer tracer, VersionContextSupplier versionContextSupplier )
    {
        this.victimPage = victimPage;
        this.pointer = victimPage;
        this.tracer = tracer;
        this.versionContextSupplier = versionContextSupplier;
    }

    final void initialise( MuninnPagedFile pagedFile, long pageId, int pf_flags )
    {
        this.swapper = pagedFile.swapper;
        this.swapperId = pagedFile.swapperId;
        this.filePageSize = pagedFile.filePageSize;
        this.pagedFile = pagedFile;
        this.pageId = pageId;
        this.pf_flags = pf_flags;
        this.eagerFlush = isFlagRaised( pf_flags, PF_EAGER_FLUSH );
        this.noFault = isFlagRaised( pf_flags, PF_NO_FAULT );
        this.noGrow = noFault | isFlagRaised( pf_flags, PagedFile.PF_NO_GROW );
    }

    private boolean isFlagRaised( int flagSet, int flag )
    {
        return (flagSet & flag) == flag;
    }

    @Override
    public final void rewind()
    {
        nextPageId = pageId;
        currentPageId = UNBOUND_PAGE_ID;
    }

    public final void reset( long pageRef )
    {
        this.pinnedPageRef = pageRef;
        this.offset = 0;
        this.pointer = pagedFile.getAddress( pageRef );
        this.pageSize = filePageSize;
        pinEvent.setCachePageId( pagedFile.toId( pageRef ) );
    }

    @Override
    public final boolean next( long pageId ) throws IOException
    {
        if ( currentPageId == pageId )
        {
            verifyContext();
            return true;
        }
        nextPageId = pageId;
        return next();
    }

    void verifyContext()
    {
        VersionContext versionContext = versionContextSupplier.getVersionContext();
        long lastClosedTransactionId = versionContext.lastClosedTransactionId();
        if ( lastClosedTransactionId == Long.MAX_VALUE )
        {
            return;
        }
        if ( isPotentiallyReadingDirtyData( lastClosedTransactionId ) )
        {
            versionContext.markAsDirty();
        }
    }

    /**
     * We reading potentially dirty data in case if our page last modification version is higher then
     * requested lastClosedTransactionId; or for this page file we already evict some page with version that is higher
     * then requested lastClosedTransactionId. In this case we can't be sure that data of current page satisfying
     * visibility requirements and we pessimistically will assume that we reading dirty data.
     * @param lastClosedTransactionId last closed transaction id
     * @return true in case if we reading potentially dirty data for requested lastClosedTransactionId.
     */
    private boolean isPotentiallyReadingDirtyData( long lastClosedTransactionId )
    {
        return pagedFile.getLastModifiedTxId( pinnedPageRef ) > lastClosedTransactionId ||
                pagedFile.getHighestEvictedTransactionId() > lastClosedTransactionId;
    }

    @Override
    public final void close()
    {
        if ( pagedFile == null )
        {
            return; // already closed
        }
        closeLinks( this );
    }

    private void closeLinks( MuninnPageCursor cursor )
    {
        while ( cursor != null && cursor.pagedFile != null )
        {
            cursor.unpinCurrentPage();
            // We null out the pagedFile field to allow it and its (potentially big) translation table to be garbage
            // collected when the file is unmapped, since the cursors can stick around in thread local caches, etc.
            cursor.pagedFile = null;
            cursor = cursor.linkedCursor;
        }
    }

    private void closeLinkedCursorIfAny()
    {
        if ( linkedCursor != null )
        {
            closeLinks( linkedCursor );
        }
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        closeLinkedCursorIfAny();
        MuninnPagedFile pf = pagedFile;
        if ( pf == null )
        {
            // This cursor has been closed
            throw new IllegalStateException( "Cannot open linked cursor on closed page cursor" );
        }
        if ( linkedCursor != null )
        {
            linkedCursor.initialise( pf, pageId, pf_flags );
            linkedCursor.rewind();
        }
        else
        {
            linkedCursor = (MuninnPageCursor) pf.io( pageId, pf_flags );
            linkedCursor.isLinkedCursor = true;
        }
        return linkedCursor;
    }

    /**
     * Must be called by {@link #unpinCurrentPage()}.
     */
    void clearPageCursorState()
    {
        // We don't need to clear the pointer field, because setting the page size to 0 will make all future accesses
        // go out of bounds, which in turn imply that they will always end up accessing the victim page anyway.
        clearPageReference();
        currentPageId = UNBOUND_PAGE_ID;
        cursorException = null;
    }

    void clearPageReference()
    {
        // Make all future bounds checks fail, and send future accesses to the victim page.
        pageSize = 0;
        // Decouple us from the memory page, so we avoid messing with the page meta-data.
        pinnedPageRef = 0;
    }

    @Override
    public final long getCurrentPageId()
    {
        return currentPageId;
    }

    @Override
    public final int getCurrentPageSize()
    {
        return currentPageId == UNBOUND_PAGE_ID ? UNBOUND_PAGE_SIZE : pagedFile.pageSize();
    }

    @Override
    public final File getCurrentFile()
    {
        return currentPageId == UNBOUND_PAGE_ID ? null : pagedFile.file();
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
        int[][] tt = pagedFile.translationTable;
        if ( tt.length <= chunkId )
        {
            tt = expandTranslationTableCapacity( chunkId );
        }
        int[] chunk = tt[chunkId];

        // Now, if the reference in the chunk slot is a latch, we wait on it and look up again (in a loop, since the
        // page might get evicted right after the page fault completes). If we find a page, we lock it and check its
        // binding (since it might get evicted and faulted into something else in the time between our look up and
        // our locking of the page). If the reference is null or it referred to a page that had wrong bindings, we CAS
        // in a latch. If that CAS succeeds, we page fault, set the slot to the faulted in page and open the latch.
        // If the CAS failed, we retry the look up and start over from the top.
        for (;;)
        {
            int mappedPageId = UnsafeUtil.getIntVolatile( chunk, chunkOffset );
            if ( mappedPageId != UNMAPPED_TTE )
            {
                // We got *a* page, but we might be racing with eviction. To cope with that, we have to take some
                // kind of lock on the page, and check that it is indeed bound to what we expect. If not, then it has
                // been evicted, and possibly even page faulted into something else. In this case, we discard the
                // item and try again, as the eviction thread would have set the chunk array slot to null.
                long pageRef = pagedFile.deref( mappedPageId );
                boolean locked = tryLockPage( pageRef );
                if ( locked & pagedFile.isBoundTo( pageRef, swapperId, filePageId ) )
                {
                    pinCursorToPage( pageRef, filePageId, swapper );
                    pinEvent.hit();
                    return;
                }
                if ( locked )
                {
                    unlockPage( pageRef );
                }
            }
            else
            {
                if ( uncommonPin( filePageId, chunkOffset, chunk ) )
                {
                    return;
                }
            }
        }
    }

    private int[][] expandTranslationTableCapacity( int chunkId )
    {
        return pagedFile.expandCapacity( chunkId );
    }

    private boolean uncommonPin( long filePageId, long chunkOffset, int[] chunk ) throws IOException
    {
        if ( noFault )
        {
            // The only page state that needs to be cleared is the currentPageId, since it was set prior to pin.
            currentPageId = UNBOUND_PAGE_ID;
            return true;
        }
        // Looks like there's no mapping, so we'd like to do a page fault.
        LatchMap.Latch latch = pagedFile.pageFaultLatches.takeOrAwaitLatch( filePageId );
        if ( latch != null )
        {
            // We managed to inject our latch, so we now own the right to perform the page fault. We also
            // have a duty to eventually release and remove the latch, no matter what happens now.
            // However, we first have to double-check that a page fault did not complete in-between our initial
            // check in the translation table, and us getting a latch.
            if ( UnsafeUtil.getIntVolatile( chunk, chunkOffset ) == UNMAPPED_TTE )
            {
                // Sweet, we didn't race with any other fault on this translation table entry.
                long pageRef = pageFault( filePageId, swapper, chunkOffset, chunk, latch );
                pinCursorToPage( pageRef, filePageId, swapper );
                return true;
            }
            // Oops, looks like we raced with another page fault on this file page.
            // Let's release our latch and retry the pin.
            latch.release();
        }
        // We found a latch, so someone else is already doing a page fault for this page.
        // The `takeOrAwaitLatch` already waited for this latch to be released on our behalf,
        // so now we just have to do another iteration of the loop to see what's in the translation table now.
        return false;
    }

    private long pageFault(
            long filePageId, PageSwapper swapper, long chunkOffset, int[] chunk, LatchMap.Latch latch )
            throws IOException
    {
        // We are page faulting. This is a critical time, because we currently have the given latch in the chunk array
        // slot that we are faulting into. We MUST make sure to release that latch, and remove it from the chunk, no
        // matter what happens. Otherwise other threads will get stuck waiting forever for our page fault to finish.
        // If we manage to get a free page to fault into, then we will also be taking a write lock on that page, to
        // protect it against concurrent eviction as we assigning a binding to the page. If anything goes wrong, then
        // we must make sure to release that write lock as well.
        PageFaultEvent faultEvent = pinEvent.beginPageFault();
        long pageRef;
        try
        {
            // The grabFreePage method might throw.
            pageRef = pagedFile.grabFreeAndExclusivelyLockedPage( faultEvent );

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
            pagedFile.initBuffer( pageRef );
            pagedFile.fault( pageRef, swapper, pagedFile.swapperId, filePageId, faultEvent );
        }
        catch ( Throwable throwable )
        {
            // Make sure to unlock the page, so the eviction thread can pick up our trash.
            pagedFile.unlockExclusive( pageRef );
            // Make sure to unstuck the page fault latch.
            abortPageFault( throwable, chunk, chunkOffset, latch, faultEvent );
            throw throwable;
        }
        // Put the page in the translation table before we undo the exclusive lock, as we could otherwise race with
        // eviction, and the onEvict callback expects to find a MuninnPage object in the table.
        UnsafeUtil.putIntVolatile( chunk, chunkOffset, pagedFile.toId( pageRef ) );
        // Once we page has been published to the translation table, we can convert our exclusive lock to whatever we
        // need for the page cursor.
        convertPageFaultLock( pageRef );
        latch.release();
        faultEvent.done();
        return pageRef;
    }

    private void abortPageFault( Throwable throwable, int[] chunk, long chunkOffset,
                                 LatchMap.Latch latch,
                                 PageFaultEvent faultEvent )
    {
        UnsafeUtil.putIntVolatile( chunk, chunkOffset, UNMAPPED_TTE );
        latch.release();
        faultEvent.done( throwable );
        pinEvent.done();
    }

    long assertPagedFileStillMappedAndGetIdOfLastPage() throws FileIsNotMappedException
    {
        return pagedFile.getLastPageId();
    }

    protected abstract void unpinCurrentPage();

    protected abstract void convertPageFaultLock( long pageRef );

    protected abstract void pinCursorToPage( long pageRef, long filePageId, PageSwapper swapper )
            throws FileIsNotMappedException;

    protected abstract boolean tryLockPage( long pageRef );

    protected abstract void unlockPage( long pageRef );

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
        long p = pointer;
        long can = p + offset;
        if ( boundsCheck )
        {
            if ( can + size > p + pageSize || can < p )
            {
                outOfBounds = true;
                // Return the victim page when we are out of bounds, since at this point we can't tell if the pointer
                // will be used for reading or writing.
                return victimPage;
            }
        }
        return can;
    }

    /**
     * Compute a pointer that guarantees (assuming {@code size} is less than or equal to {@link #pageSize}) that the
     * page access will be within the bounds of the page.
     * This works just like {@link #getBoundedPointer(int, int)}, except in terms of the current {@link #offset}.
     * This version is faster when applicable, because it can ignore the <em>page underflow</em> case.
     */
    private long nextBoundedPointer( int size )
    {
        int offset = this.offset;
        long can = pointer + offset;
        if ( boundsCheck )
        {
            if ( offset + size > pageSize )
            {
                outOfBounds = true;
                // Return the victim page when we are out of bounds, since at this point we can't tell if the pointer
                // will be used for reading or writing.
                return victimPage;
            }
        }
        return can;
    }

    @Override
    public final byte getByte()
    {
        long p = nextBoundedPointer( SIZE_OF_BYTE );
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
        long p = nextBoundedPointer( SIZE_OF_BYTE );
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
        long p = nextBoundedPointer( SIZE_OF_LONG );
        long value = getLongAt( p );
        offset += SIZE_OF_LONG;
        return value;
    }

    @Override
    public long getLong( int offset )
    {
        long p = getBoundedPointer( offset, SIZE_OF_LONG );
        return getLongAt( p );
    }

    private long getLongAt( long p )
    {
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
        long p = nextBoundedPointer( SIZE_OF_LONG );
        putLongAt( p, value );
        offset += SIZE_OF_LONG;
    }

    @Override
    public void putLong( int offset, long value )
    {
        long p = getBoundedPointer( offset, SIZE_OF_LONG );
        putLongAt( p, value );
    }

    private void putLongAt( long p, long value )
    {
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
        UnsafeUtil.putByte( p + 7, (byte) value );
    }

    @Override
    public int getInt()
    {
        long p = nextBoundedPointer( SIZE_OF_INT );
        int i = getIntAt( p );
        offset += SIZE_OF_INT;
        return i;
    }

    @Override
    public int getInt( int offset )
    {
        long p = getBoundedPointer( offset, SIZE_OF_INT );
        return getIntAt( p );
    }

    private int getIntAt( long p )
    {
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
        long p = nextBoundedPointer( SIZE_OF_INT );
        putIntAt( p, value );
        offset += SIZE_OF_INT;
    }

    @Override
    public void putInt( int offset, int value )
    {
        long p = getBoundedPointer( offset, SIZE_OF_INT );
        putIntAt( p, value );
    }

    private void putIntAt( long p, int value )
    {
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
        UnsafeUtil.putByte( p + 3, (byte) value );
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
    public void putBytes( int bytes, byte value )
    {
        long p = getBoundedPointer( offset, bytes );
        if ( !outOfBounds )
        {
            UnsafeUtil.setMemory( p, bytes, value );
        }
        offset += bytes;
    }

    @Override
    public final short getShort()
    {
        long p = nextBoundedPointer( SIZE_OF_SHORT );
        short s = getShortAt( p );
        offset += SIZE_OF_SHORT;
        return s;
    }

    @Override
    public short getShort( int offset )
    {
        long p = getBoundedPointer( offset, SIZE_OF_SHORT );
        return getShortAt( p );
    }

    private short getShortAt( long p )
    {
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
        long p = nextBoundedPointer( SIZE_OF_SHORT );
        putShortAt( p, value );
        offset += SIZE_OF_SHORT;
    }

    @Override
    public void putShort( int offset, short value )
    {
        long p = getBoundedPointer( offset, SIZE_OF_SHORT );
        putShortAt( p, value );
    }

    private void putShortAt( long p, short value )
    {
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
        UnsafeUtil.putByte( p + 1, (byte) value );
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
             & lengthInBytes >= 0 )
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
    public int copyTo( int sourceOffset, ByteBuffer buf )
    {
        if ( buf.getClass() == UnsafeUtil.directByteBufferClass && buf.isDirect() && !buf.isReadOnly() )
        {
            // We expect that the mutable direct byte buffer is implemented with a class that is distinct from the
            // non-mutable (read-only) and non-direct (on-heap) byte buffers. By comparing class object instances,
            // we also implicitly assume that the classes are loaded by the same class loader, which should be
            // trivially true in almost all practical cases.
            // If our expectations are not met, then the additional isDirect and !isReadOnly checks will send all
            // calls to the byte-wise-copy fallback.
            return copyToDirectByteBuffer( sourceOffset, buf );
        }
        return copyToByteBufferByteWise( sourceOffset, buf );
    }

    private int copyToDirectByteBuffer( int sourceOffset, ByteBuffer buf )
    {
        int pos = buf.position();
        int bytesToCopy = Math.min( buf.limit() - pos, pageSize - sourceOffset );
        long source = pointer + sourceOffset;
        if ( sourceOffset < getCurrentPageSize() & sourceOffset >= 0 )
        {
            long target = UnsafeUtil.getDirectByteBufferAddress( buf );
            UnsafeUtil.copyMemory( source, target + pos, bytesToCopy );
            buf.position( pos + bytesToCopy );
        }
        else
        {
            outOfBounds = true;
        }
        return bytesToCopy;
    }

    private int copyToByteBufferByteWise( int sourceOffset, ByteBuffer buf )
    {
        int bytesToCopy = Math.min( buf.limit() - buf.position(), pageSize - sourceOffset );
        for ( int i = 0; i < bytesToCopy; i++ )
        {
            byte b = getByte( sourceOffset + i );
            buf.put( b );
        }
        return bytesToCopy;
    }

    @Override
    public void shiftBytes( int sourceStart, int length, int shift )
    {
        int sourceEnd = sourceStart + length;
        int targetStart = sourceStart + shift;
        int targetEnd = sourceStart + length + shift;
        if ( sourceStart < 0
                | sourceEnd > filePageSize
                | targetStart < 0
                | targetEnd > filePageSize
                | length < 0 )
        {
            outOfBounds = true;
            return;
        }

        if ( shift < 0 )
        {
            unsafeShiftLeft( sourceStart, sourceEnd, length, shift );
        }
        else
        {
            unsafeShiftRight( sourceEnd, sourceStart, length, shift );
        }
    }

    private void unsafeShiftLeft( int fromPos, int toPos, int length, int shift )
    {
        int longSteps = length >> 3;
        if ( UnsafeUtil.allowUnalignedMemoryAccess && longSteps > 0 )
        {
            for ( int i = 0; i < longSteps; i++ )
            {
                long x = UnsafeUtil.getLong( pointer + fromPos );
                UnsafeUtil.putLong( pointer + fromPos + shift, x );
                fromPos += Long.BYTES;
            }
        }

        while ( fromPos < toPos )
        {
            byte b = UnsafeUtil.getByte( pointer + fromPos );
            UnsafeUtil.putByte( pointer + fromPos + shift, b );
            fromPos++;
        }
    }

    private void unsafeShiftRight( int fromPos, int toPos, int length, int shift )
    {
        int longSteps = length >> 3;
        if ( UnsafeUtil.allowUnalignedMemoryAccess && longSteps > 0 )
        {
            for ( int i = 0; i < longSteps; i++ )
            {
                fromPos -= Long.BYTES;
                long x = UnsafeUtil.getLong( pointer + fromPos );
                UnsafeUtil.putLong( pointer + fromPos + shift, x );
            }
        }

        while ( fromPos > toPos )
        {
            fromPos--;
            byte b = UnsafeUtil.getByte( pointer + fromPos );
            UnsafeUtil.putByte( pointer + fromPos + shift, b );
        }
    }

    @Override
    public void setOffset( int offset )
    {
        this.offset = offset;
        if ( offset < 0 | offset > filePageSize )
        {
            this.offset = 0;
            outOfBounds = true;
        }
    }

    @Override
    public final int getOffset()
    {
        return offset;
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        MuninnPageCursor cursor = this;
        boolean result = false;
        do
        {
            result |= cursor.outOfBounds;
            cursor.outOfBounds = false;
            cursor = cursor.linkedCursor;
        }
        while ( cursor != null );
        return result;
    }

    @Override
    public void checkAndClearCursorException() throws CursorException
    {
        MuninnPageCursor cursor = this;
        do
        {
            Object error = cursor.cursorException;
            if ( error != null )
            {
                clearCursorError( cursor );
                if ( usePreciseCursorErrorStackTraces )
                {
                    throw (CursorExceptionWithPreciseStackTrace) error;
                }
                else
                {
                    throw new CursorException( (String) error );
                }
            }
            cursor = cursor.linkedCursor;
        }
        while ( cursor != null );
    }

    @Override
    public void clearCursorException()
    {
        clearCursorError( this );
    }

    private void clearCursorError( MuninnPageCursor cursor )
    {
        while ( cursor != null )
        {
            cursor.cursorException = null;
            cursor = cursor.linkedCursor;
        }
    }

    @Override
    public void raiseOutOfBounds()
    {
        outOfBounds = true;
    }

    @Override
    public void setCursorException( String message )
    {
        Objects.requireNonNull( message );
        if ( usePreciseCursorErrorStackTraces )
        {
            this.cursorException = new CursorExceptionWithPreciseStackTrace( message );
        }
        else
        {
            this.cursorException = message;
        }
    }

    @Override
    public void zapPage()
    {
        if ( pageSize == 0 )
        {
            // if this page has been closed then pageSize == 0 and we must adhere to making writes
            // trigger outOfBounds when closed
            outOfBounds = true;
        }
        else
        {
            UnsafeUtil.setMemory( pointer, pageSize, (byte) 0 );
        }
    }

    @Override
    public boolean isWriteLocked()
    {
        return isFlagRaised( pf_flags, PF_SHARED_WRITE_LOCK );
    }
}
