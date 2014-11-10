/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.jsr166e.StampedLock;
import org.neo4j.io.pagecache.monitoring.EvictionEvent;
import org.neo4j.io.pagecache.monitoring.FlushEvent;
import org.neo4j.io.pagecache.monitoring.FlushEventOpportunity;
import org.neo4j.io.pagecache.monitoring.PageFaultEvent;

import static org.neo4j.io.pagecache.impl.muninn.UnsafeUtil.allowUnalignedMemoryAccess;
import static org.neo4j.io.pagecache.impl.muninn.UnsafeUtil.storeByteOrderIsNative;

final class MuninnPage extends StampedLock implements Page
{
    private static final Constructor<?> directBufferCtor;
    private static final long usageStampOffset = UnsafeUtil.getFieldOffset( MuninnPage.class, "usageStamp" );
    static {
        Constructor<?> ctor = null;
        try
        {
            Class<?> dbb = Class.forName( "java.nio.DirectByteBuffer" );
            ctor = dbb.getDeclaredConstructor( Long.TYPE, Integer.TYPE );
            ctor.setAccessible( true );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        directBufferCtor = ctor;
    }

    private final int cachePageSize;
    private final int cachePageId;

    // We keep this reference to prevent the MemoryReleaser from becoming
    // finalizable until all our pages are finalizable or collected.
    private final MemoryReleaser memoryReleaser;

    private long pointer;

    // Optimistically incremented; occasionally truncated to a max of 5.
    // accessed through unsafe
    private volatile byte usageStamp;

    // Next pointer in the freelist of available pages. This is either a
    // MuninnPage object, or a FreePage object. See the comment on the
    // MuninnPageCache.freelist field.
    public Object nextFree;

    private PageSwapper swapper;
    private long filePageId = PageCursor.UNBOUND_PAGE_ID;
    private boolean dirty;

    public MuninnPage( int cachePageSize, int cachePageId, MemoryReleaser memoryReleaser )
    {
        this.cachePageSize = cachePageSize;
        this.cachePageId = cachePageId;
        this.memoryReleaser = memoryReleaser;
    }

    private void checkBounds( int position )
    {
        if ( position > cachePageSize )
        {
            String msg = "Position " + position + " is greater than the upper " +
                    "page size bound of " + cachePageSize;
            throw new IndexOutOfBoundsException( msg );
        }
    }

    public byte getByte( int offset )
    {
        checkBounds( offset + 1 );
        return UnsafeUtil.getByte( pointer + offset );
    }

    public void putByte( byte value, int offset )
    {
        checkBounds( offset + 1 );
        UnsafeUtil.putByte( pointer + offset, value );
    }

    public long getLong( int offset )
    {
        checkBounds( offset + 8 );
        if ( allowUnalignedMemoryAccess )
        {
            long x = UnsafeUtil.getLong( pointer + offset );
            return storeByteOrderIsNative ? x : Long.reverseBytes( x );
        }
        return getLongBigEndian( offset );
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

    public void putLong( long value, int offset )
    {
        checkBounds( offset + 8 );
        if ( allowUnalignedMemoryAccess )
        {
            long p = pointer + offset;
            UnsafeUtil.putLong( p, storeByteOrderIsNative ? value : Long.reverseBytes( value ) );
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

    public int getInt( int offset )
    {
        checkBounds( offset + 4 );
        if ( allowUnalignedMemoryAccess )
        {
            int x = UnsafeUtil.getInt( pointer + offset );
            return storeByteOrderIsNative ? x : Integer.reverseBytes( x );
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

    public void putInt( int value, int offset )
    {
        checkBounds( offset + 4 );
        if ( allowUnalignedMemoryAccess )
        {
            long p = pointer + offset;
            UnsafeUtil.putInt( p, storeByteOrderIsNative ? value : Integer.reverseBytes( value ) );
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

    public short getShort( int offset )
    {
        checkBounds( offset + 2 );
        if ( allowUnalignedMemoryAccess )
        {
            short x = UnsafeUtil.getShort( pointer + offset );
            return storeByteOrderIsNative ? x : Short.reverseBytes( x );
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

    public void putShort( short value, int offset )
    {
        checkBounds( offset + 2 );
        if ( allowUnalignedMemoryAccess )
        {
            long p = pointer + offset;
            UnsafeUtil.putShort( p, storeByteOrderIsNative ? value : Short.reverseBytes( value ) );
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
    public void getBytes( byte[] data, int offset )
    {
        checkBounds( offset + data.length );
        long address = pointer + offset;
        int length = data.length;
        for ( int i = 0; i < length; i++ )
        {
            data[i] = UnsafeUtil.getByte( address );
            address++;
        }
    }

    @Override
    public void putBytes( byte[] data, int offset )
    {
        checkBounds( offset + data.length );
        long address = pointer + offset;
        int length = data.length;
        for ( int i = 0; i < length; i++ )
        {
            UnsafeUtil.putByte( address, data[i] );
            address++;
        }
    }

    /** Increment the usage stamp to at most 5. */
    public void incrementUsage()
    {
        byte usage = UnsafeUtil.getByteVolatile( this, usageStampOffset );
        if ( usage < 5 )
        {
            // A Java 8 alternative:
//            UnsafeUtil.getAndAddInt( this, usageStampOffset, 1 );

            // Java 7:
            UnsafeUtil.putByteVolatile( this, usageStampOffset, (byte) (usage + 1) );
        }
    }

    /** Decrement the usage stamp. Returns true if it reaches 0. */
    public boolean decrementUsage()
    {
        byte usage = UnsafeUtil.getByteVolatile( this, usageStampOffset );
        if ( usage > 0 )
        {
            // A Java 8 alternative:
//            return UnsafeUtil.getAndAddInt( this, usageStampOffset, -1 ) <= 1;

            // Java 7:
            usage--;
            UnsafeUtil.putByteVolatile( this, usageStampOffset, usage );
            return usage == 0;
        }
        return true;
    }

    /**
     * NOTE: This method must be called while holding the page write lock.
     * This method assumes that initBuffer() has already been called at least once.
     */
    @Override
    public int swapIn( StoreChannel channel, long offset, int length ) throws IOException
    {
        assert isWriteLocked() : "swapIn requires write lock to protect the cache page";
        int readTotal = 0;
        try
        {
            ByteBuffer bufferProxy = (ByteBuffer) directBufferCtor.newInstance(
                    pointer, cachePageSize );
            bufferProxy.clear();
            bufferProxy.limit( length );
            int read;
            do
            {
                read = channel.read( bufferProxy, offset + readTotal );
            }
            while ( read != -1 && (readTotal += read) < length );

            // Zero-fill the rest.
            while ( bufferProxy.position() < bufferProxy.limit() )
            {
                bufferProxy.put( (byte) 0 );
            }
            return readTotal;
        }
        catch ( ClosedChannelException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            String msg = String.format(
                    "Read failed after %s of %s bytes from offset %s",
                    readTotal, length, offset );
            throw new IOException( msg, e );
        }
    }

    /**
     * NOTE: This method must be called while holding at least the page read lock.
     * This method assumes that initBuffer() has already been called at least once.
     */
    @Override
    public void swapOut( StoreChannel channel, long offset, int length ) throws IOException
    {
        assert isReadLocked() || isWriteLocked() : "swapOut requires lock";
        try
        {
            ByteBuffer bufferProxy = (ByteBuffer) directBufferCtor.newInstance(
                    pointer, cachePageSize );
            bufferProxy.clear();
            bufferProxy.limit( length );
            channel.writeAll( bufferProxy, offset );
        }
        catch ( IOException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new IOException( e );
        }
    }

    /**
     * NOTE: This method must be called while holding a pessimistic lock on the page.
     */
    public void flush( FlushEventOpportunity flushOpportunity ) throws IOException
    {
        if ( swapper != null && dirty )
        {
            // The page is bound and has stuff to flush
            doFlush( swapper, filePageId, flushOpportunity );
        }
    }

    /**
     * NOTE: This method must be called while holding a pessimistic lock on the page.
     */
    public void flush(
            PageSwapper swapper,
            long filePageId,
            FlushEventOpportunity flushOpportunity ) throws IOException
    {
        if ( dirty && this.swapper == swapper && this.filePageId == filePageId )
        {
            // The page is bound to the given swapper and has stuff to flush
            doFlush( swapper, filePageId, flushOpportunity );
        }
    }

    private void doFlush(
            PageSwapper swapper,
            long filePageId,
            FlushEventOpportunity flushOpportunity ) throws IOException
    {
        FlushEvent event = flushOpportunity.beginFlush( filePageId, cachePageId, swapper );
        try
        {
            int bytesWritten = swapper.write( filePageId, this );
            dirty = false;
            event.addBytesWritten( bytesWritten );
            event.done();
        }
        catch ( IOException e )
        {
            event.done( e );
            throw e;
        }
    }

    public void markAsDirty()
    {
        dirty = true;
    }

    /**
     * NOTE: This method MUST be called while holding the page write lock.
     */
    public void fault(
            PageSwapper swapper,
            long filePageId,
            PageFaultEvent faultEvent ) throws IOException
    {
        assert isWriteLocked(): "Cannot fault page without write-lock";
        if ( this.swapper != null || this.filePageId != PageCursor.UNBOUND_PAGE_ID )
        {
            String msg = String.format(
                    "Cannot fault page {filePageId = %s, swapper = %s} into " +
                            "cache page %s. Already bound to {filePageId = " +
                            "%s, swapper = %s}.",
                    filePageId, swapper, cachePageId, this.filePageId, this.swapper );
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
        int bytesRead = swapper.read( filePageId, this );
        faultEvent.addBytesRead( bytesRead );
        faultEvent.setCachePageId( cachePageId );
        this.swapper = swapper; // Page now considered isBoundTo( swapper, filePageId )
    }

    /**
     * NOTE: This method MUST be called while holding the page write lock.
     */
    public void evict( EvictionEvent evictionEvent ) throws IOException
    {
        assert isWriteLocked(): "Cannot evict page without write-lock";
        long filePageId = this.filePageId;
        evictionEvent.setCachePageId( cachePageId );
        evictionEvent.setFilePageId( filePageId );

        flush( evictionEvent.flushEventOpportunity() );
        UnsafeUtil.setMemory( pointer, cachePageSize, (byte) 0 );
        this.filePageId = PageCursor.UNBOUND_PAGE_ID;

        PageSwapper swapper = this.swapper;
        this.swapper = null;

        if ( swapper != null )
        {
            // The swapper can be null if the last page fault
            // that page threw an exception.
            swapper.evicted( filePageId, this );
            evictionEvent.setSwapper( swapper );
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
        assert isWriteLocked(): "Cannot initBuffer without write-lock";
        if ( pointer == 0 )
        {
            pointer = UnsafeUtil.malloc( cachePageSize );
            memoryReleaser.registerPointer( cachePageId, pointer );
        }
    }

    public PageSwapper getSwapper()
    {
        return swapper;
    }

    public long getFilePageId()
    {
        return filePageId;
    }

    @Override
    public int getCachePageId()
    {
        return cachePageId;
    }

    @Override
    public String toString()
    {
        return String.format( "MuninnPage@%x[%s -> %x, filePageId = %s%s, swapper = %s]%s",
                hashCode(), cachePageId, pointer, filePageId, (dirty? ", dirty" : ""),
                swapper, getLockStateString() );
    }
}
