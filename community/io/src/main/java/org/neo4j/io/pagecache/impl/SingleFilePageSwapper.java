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
package org.neo4j.io.pagecache.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static java.lang.String.format;

/**
 * A simple PageSwapper implementation that directs all page swapping to a
 * single file on the file system.
 *
 * It additionally tracks the file size precisely, to avoid calling into the
 * file system whenever the size of the given file is queried.
 */
public class SingleFilePageSwapper implements PageSwapper
{
    private static int defaultChannelStripePower()
    {
        int vcores = Runtime.getRuntime().availableProcessors();
        // Find the lowest 2's exponent that can accommodate 'vcores'
        int stripePower = 32 - Integer.numberOfLeadingZeros( vcores - 1 );
        return Math.min( 64, Math.max( 1, stripePower ) );
    }

    // Exponent of 2 of how many channels we open per file:
    private static final int channelStripePower = Integer.getInteger(
            "org.neo4j.io.pagecache.implSingleFilePageSwapper.channelStripePower",
            defaultChannelStripePower() );

    // Exponent of 2 of how many consecutive pages go to the same stripe
    private static final int channelStripeShift = Integer.getInteger(
            "org.neo4j.io.pagecache.implSingleFilePageSwapper.channelStripeShift", 4 );

    private static final int channelStripeCount = 1 << channelStripePower;
    private static final int channelStripeMask = channelStripeCount - 1;

    private static final long fileSizeOffset =
            UnsafeUtil.getFieldOffset( SingleFilePageSwapper.class, "fileSize" );

    private static final ThreadLocal<ByteBuffer> proxyCache = new ThreadLocal<>();

    private static ByteBuffer proxy( long buffer, int bufferLength ) throws IOException
    {
        ByteBuffer buf = proxyCache.get();
        if ( buf != null )
        {
            UnsafeUtil.initDirectByteBuffer( buf, buffer, bufferLength );
            return buf;
        }
        return createAndGetNewBuffer( buffer, bufferLength );
    }

    private static ByteBuffer createAndGetNewBuffer( long buffer, int bufferLength ) throws IOException
    {
        ByteBuffer buf;
        try
        {
            buf = UnsafeUtil.newDirectByteBuffer( buffer, bufferLength );
        }
        catch ( Exception e )
        {
            throw new IOException( e );
        }
        proxyCache.set( buf );
        return buf;
    }

    private final FileSystemAbstraction fs;
    private final File file;
    private final int filePageSize;
    private volatile PageEvictionCallback onEviction;
    private final StoreChannel[] channels;

    // Guarded by synchronized(this). See tryReopen() and close().
    private boolean closed;

    @SuppressWarnings( "unused" ) // Accessed through unsafe
    private volatile long fileSize;

    public SingleFilePageSwapper(
            File file,
            FileSystemAbstraction fs,
            int filePageSize,
            PageEvictionCallback onEviction ) throws IOException
    {
        this.fs = fs;
        this.file = file;
        this.channels = new StoreChannel[channelStripeCount];
        for ( int i = 0; i < channelStripeCount; i++ )
        {
            channels[i] = fs.open( file, "rw" );
        }
        this.filePageSize = filePageSize;
        this.onEviction = onEviction;
        increaseFileSizeTo( channels[0].size() );
    }

    private void increaseFileSizeTo( long newFileSize )
    {
        long currentFileSize;
        do
        {
            currentFileSize = getCurrentFileSize();
        }
        while ( currentFileSize < newFileSize && !UnsafeUtil.compareAndSwapLong(
                this, fileSizeOffset, currentFileSize, newFileSize ) );
    }

    private long getCurrentFileSize()
    {
        return UnsafeUtil.getLongVolatile( this, fileSizeOffset );
    }

    private void setCurrentFileSize( long size )
    {
        UnsafeUtil.putLongVolatile( this, fileSizeOffset, size );
    }

    private StoreChannel channel( long filePageId )
    {
        int stripe = stripe( filePageId );
        return channels[stripe];
    }

    private static int stripe( long filePageId )
    {
        return (int) (filePageId >>> channelStripeShift) & channelStripeMask;
    }

    private int swapIn( StoreChannel channel, Page page, long fileOffset, int filePageSize ) throws IOException
    {
        int cachePageSize = page.size();
        long address = page.address();
        int readTotal = 0;
        try
        {
            ByteBuffer bufferProxy = proxy( address, filePageSize );
            int read;
            do
            {
                read = channel.read( bufferProxy, fileOffset + readTotal );
            }
            while ( read != -1 && (readTotal += read) < filePageSize );

            // Zero-fill the rest.
            assert readTotal >= 0 && filePageSize <= cachePageSize && readTotal <= filePageSize: format(
                    "pointer = %h, readTotal = %s, length = %s, page size = %s",
                    address, readTotal, filePageSize, cachePageSize );
            UnsafeUtil.setMemory( address + readTotal, filePageSize - readTotal, MuninnPageCache.ZERO_BYTE );
            return readTotal;
        }
        catch ( IOException e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            String msg = format(
                    "Read failed after %s of %s bytes from fileOffset %s",
                    readTotal, filePageSize, fileOffset );
            throw new IOException( msg, e );
        }
    }

    private int swapOut( Page page, long fileOffset, StoreChannel channel ) throws IOException
    {
        try
        {
            ByteBuffer bufferProxy = proxy( page.address(), filePageSize );
            channel.writeAll( bufferProxy, fileOffset );
        }
        catch ( IOException e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            throw new IOException( e );
        }
        return filePageSize;
    }

    private void clear( Page page )
    {
        UnsafeUtil.setMemory( page.address(), page.size(), MuninnPageCache.ZERO_BYTE );
    }

    @Override
    public int read( long filePageId, Page page ) throws IOException
    {
        long fileOffset = pageIdToPosition( filePageId );
        try
        {
            if ( fileOffset < getCurrentFileSize() )
            {
                return swapIn( channel( filePageId ), page, fileOffset, filePageSize );
            }
            else
            {
                clear( page );
            }
        }
        catch ( ClosedChannelException e )
        {
            // AsynchronousCloseException is a subclass of
            // ClosedChannelException, and ClosedByInterruptException is in
            // turn a subclass of AsynchronousCloseException.
            tryReopen( filePageId, e );
            boolean interrupted = Thread.interrupted();
            // Recurse because this is hopefully a very rare occurrence.
            int bytesRead = read( filePageId, page );
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
            return bytesRead;
        }
        return 0;
    }

    @Override
    public int write( long filePageId, Page page ) throws IOException
    {
        long fileOffset = pageIdToPosition( filePageId );
        increaseFileSizeTo( fileOffset + filePageSize );
        try
        {
            StoreChannel channel = channel( filePageId );
            return swapOut( page, fileOffset, channel );
        }
        catch ( ClosedChannelException e )
        {
            // AsynchronousCloseException is a subclass of
            // ClosedChannelException, and ClosedByInterruptException is in
            // turn a subclass of AsynchronousCloseException.
            tryReopen( filePageId, e );
            boolean interrupted = Thread.interrupted();
            // Recurse because this is hopefully a very rare occurrence.
            int bytesWritten = write( filePageId, page );
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
            return bytesWritten;
        }
    }

    @Override
    public void evicted( long filePageId, Page page )
    {
        PageEvictionCallback callback = this.onEviction;
        if ( callback != null )
        {
            callback.onEvict( filePageId, page );
        }
    }

    @Override
    public File file()
    {
        return file;
    }

    private long pageIdToPosition( long pageId )
    {
        return filePageSize * pageId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        SingleFilePageSwapper that = (SingleFilePageSwapper) o;

        return file.equals( that.file );

    }

    @Override
    public int hashCode()
    {
        return file.hashCode();
    }

    /**
     * Reopens the channel if it has been closed and the close() method on
     * this swapper has not been called. In other words, if the channel has
     * been "accidentally" closed by an interrupt or the like.
     *
     * If the channel has been explicitly closed with the PageSwapper#close()
     * method, then this method will re-throw the passed-in exception.
     *
     * If the reopening of the file fails with an exception for some reason,
     * then that exception is added as a suppressed exception to the passed in
     * ClosedChannelException, and the CCE is then rethrown.
     */
    private synchronized void tryReopen( long filePageId, ClosedChannelException closedException ) throws ClosedChannelException
    {
        int stripe = stripe( filePageId );
        StoreChannel channel = channels[stripe];
        if ( channel.isOpen() )
        {
            // Someone got ahead of us, presumably. Nothing to do.
            return;
        }

        if ( closed )
        {
            // We've been explicitly closed, so we shouldn't reopen the
            // channel.
            throw closedException;
        }

        try
        {
            channels[stripe] = fs.open( file, "rw" );
        }
        catch ( IOException e )
        {
            closedException.addSuppressed( e );
            throw closedException;
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        closed = true;
        for ( StoreChannel channel : channels )
        {
            channel.close();
        }

        // Eagerly relinquish our reference to the onEviction callback, because even though
        // we've closed the PagedFile at this point, there are likely still pages in the cache that are bound to this
        // swapper, and will stay bound, until the eviction threads eventually gets around to kicking them out.
        // It is especially important to null out the onEviction callback field, because it is in turn holding on to
        // the striped translation table, which can be a rather large structure.
        onEviction = null;
    }

    @Override
    public void force() throws IOException
    {
        int tokenFilePageId = 0;
        try
        {
            channel( tokenFilePageId ).force( false );
        }
        catch ( ClosedChannelException e )
        {
            // AsynchronousCloseException is a subclass of
            // ClosedChannelException, and ClosedByInterruptException is in
            // turn a subclass of AsynchronousCloseException.
            tryReopen( tokenFilePageId, e );
            boolean interrupted = Thread.interrupted();
            // Recurse because this is hopefully a very rare occurrence.
            force();
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public long getLastPageId() throws IOException
    {
        long channelSize = getCurrentFileSize();
        if ( channelSize == 0 )
        {
            return PageCursor.UNBOUND_PAGE_ID;
        }
        long div = channelSize / filePageSize;
        long mod = channelSize % filePageSize;
        return mod == 0? div - 1 : div;
    }

    @Override
    public void truncate() throws IOException
    {
        setCurrentFileSize( 0 );
        int tokenFilePageId = 0;
        try
        {
            channel( tokenFilePageId ).truncate( 0 );
        }
        catch ( ClosedChannelException e )
        {
            // AsynchronousCloseException is a subclass of
            // ClosedChannelException, and ClosedByInterruptException is in
            // turn a subclass of AsynchronousCloseException.
            tryReopen( tokenFilePageId, e );
            boolean interrupted = Thread.interrupted();
            // Recurse because this is hopefully a very rare occurrence.
            truncate();
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString()
    {
        return "SingleFilePageSwapper{" +
                "filePageSize=" + filePageSize +
                ", file=" + file +
                '}';
    }
}
