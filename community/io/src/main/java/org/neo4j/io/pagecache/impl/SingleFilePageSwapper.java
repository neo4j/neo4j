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
package org.neo4j.io.pagecache.impl;

import org.apache.commons.lang3.SystemUtils;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.StoreFileChannelUnwrapper;
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
    private static final int tokenChannelStripe = 0;
    private static final long tokenFilePageId = 0;

    private static final long fileSizeOffset =
            UnsafeUtil.getFieldOffset( SingleFilePageSwapper.class, "fileSize" );

    private static final ThreadLocal<ByteBuffer> proxyCache = new ThreadLocal<>();
    private static final MethodHandle positionLockGetter = getPositionLockGetter();

    private static MethodHandle getPositionLockGetter()
    {
        try
        {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Field field = FileChannelImpl.class.getDeclaredField( "positionLock" );
            field.setAccessible( true );
            return lookup.unreflectGetter( field );
        }
        catch ( Exception e )
        {
            return null;
        }
    }

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
    private FileLock fileLock;
    private final boolean hasPositionLock;

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
        increaseFileSizeTo( channels[tokenChannelStripe].size() );

        try
        {
            acquireLock();
        }
        catch ( IOException e )
        {
            closeAndCollectExceptions( 0, e );
        }
        hasPositionLock = channels[0].getClass() == StoreFileChannel.class
                && StoreFileChannelUnwrapper.unwrap( channels[0] ).getClass() == sun.nio.ch.FileChannelImpl.class;
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

    private void acquireLock() throws IOException
    {
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            // We don't take file locks on the individual store files on Windows, because once you've taking
            // a file lock on a channel, you can only do IO on that file through that channel. This would
            // mean that we can't stripe our FileChannels on Windows, which is the platform that needs striped
            // channels the most because of lack of pwrite and pread support.
            // This is generally fine, because the StoreLocker and the lock file will protect the store from
            // being opened by multiple instances at the same time anyway.
            return;
        }

        try
        {
            fileLock = channels[tokenChannelStripe].tryLock();
            if ( fileLock == null )
            {
                throw new FileLockException( file );
            }
        }
        catch ( OverlappingFileLockException e )
        {
            throw new FileLockException( file, e );
        }
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
        long address = page.address();
        try
        {
            ByteBuffer bufferProxy = proxy( address, filePageSize );
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
    public long read( long filePageId, Page page ) throws IOException
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
            long bytesRead = read( filePageId, page );
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
            return bytesRead;
        }
        return 0;
    }

    @Override
    public long read( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException
    {
        if ( positionLockGetter != null && hasPositionLock )
        {
            try
            {
                return readPositionedVectoredToFileChannel( startFilePageId, pages, arrayOffset, length );
            }
            catch ( IOException ioe )
            {
                throw ioe;
            }
            catch ( Exception ignore )
            {
                // There's a lot of reflection going on in that method. We ignore everything that can go wrong, and
                // isn't exactly an IOException. Instead, we'll try our fallback code and see what it says.
            }
        }
        return readPositionedVectoredFallback( startFilePageId, pages, arrayOffset, length );
    }

    private long readPositionedVectoredToFileChannel(
            long startFilePageId, Page[] pages, int arrayOffset, int length ) throws Exception
    {
        long fileOffset = pageIdToPosition( startFilePageId );
        FileChannel channel = unwrappedChannel( startFilePageId );
        ByteBuffer[] srcs = convertToByteBuffers( pages, arrayOffset, length );
        long bytesRead = lockPositionReadVector(
                startFilePageId, channel, fileOffset, srcs );
        if ( bytesRead == -1 )
        {
            for ( Page page : pages )
            {
                UnsafeUtil.setMemory( page.address(), filePageSize, MuninnPageCache.ZERO_BYTE );
            }
            return 0;
        }
        else if ( bytesRead < filePageSize * length )
        {
            int pagesRead = (int) (bytesRead / filePageSize);
            int bytesReadIntoLastReadPage = (int) (bytesRead % filePageSize);
            int pagesNeedingZeroing = length - pagesRead;
            for ( int i = 0; i < pagesNeedingZeroing; i++ )
            {
                Page page = pages[arrayOffset + pagesRead + i];
                long bytesToZero = filePageSize;
                long address = page.address();
                if ( i == 0 )
                {
                    address += bytesReadIntoLastReadPage;
                    bytesToZero -= bytesReadIntoLastReadPage;
                }
                UnsafeUtil.setMemory( address, bytesToZero, MuninnPageCache.ZERO_BYTE );
            }
        }
        return bytesRead;
    }

    private long lockPositionReadVector(
            long filePageId, FileChannel channel, long fileOffset, ByteBuffer[] srcs ) throws IOException
    {
        try
        {
            long toRead = filePageSize * (long) srcs.length;
            long read, readTotal = 0;
            synchronized ( positionLock( channel ) )
            {
                channel.position( fileOffset );
                do
                {
                    read = channel.read( srcs );
                }
                while ( read != -1 && (readTotal += read) < toRead );
                return readTotal;
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
            channel = unwrappedChannel( filePageId );
            long bytesWritten = lockPositionReadVector( filePageId, channel, fileOffset, srcs );
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
            return bytesWritten;
        }
    }

    private int readPositionedVectoredFallback(
            long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException
    {
        int bytes = 0;
        for ( int i = 0; i < length; i++ )
        {
            bytes += read( startFilePageId + i, pages[arrayOffset + i] );
        }
        return bytes;
    }

    @Override
    public long write( long filePageId, Page page ) throws IOException
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
            long bytesWritten = write( filePageId, page );
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
            return bytesWritten;
        }
    }

    @Override
    public long write( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException
    {
        if ( positionLockGetter != null && hasPositionLock )
        {
            try
            {
                return writePositionedVectoredToFileChannel( startFilePageId, pages, arrayOffset, length );
            }
            catch ( IOException ioe )
            {
                throw ioe;
            }
            catch ( Exception ignore )
            {
                // There's a lot of reflection going on in that method. We ignore everything that can go wrong, and
                // isn't exactly an IOException. Instead, we'll try our fallback code and see what it says.
            }
        }
        return writePositionVectoredFallback( startFilePageId, pages, arrayOffset, length );
    }

    private long writePositionedVectoredToFileChannel(
            long startFilePageId, Page[] pages, int arrayOffset, int length ) throws Exception
    {
        long fileOffset = pageIdToPosition( startFilePageId );
        increaseFileSizeTo( fileOffset + (filePageSize * length) );
        FileChannel channel = unwrappedChannel( startFilePageId );
        ByteBuffer[] srcs = convertToByteBuffers( pages, arrayOffset, length );
        return lockPositionWriteVector( startFilePageId, channel, fileOffset, srcs );
    }

    private ByteBuffer[] convertToByteBuffers( Page[] pages, int arrayOffset, int length ) throws Exception
    {
        ByteBuffer[] buffers = new ByteBuffer[length];
        for ( int i = 0; i < length; i++ )
        {
            Page page = pages[arrayOffset + i];
            buffers[i] = UnsafeUtil.newDirectByteBuffer( page.address(), filePageSize );
        }
        return buffers;
    }

    private FileChannel unwrappedChannel( long startFilePageId )
    {
        StoreChannel storeChannel = channel( startFilePageId );
        return StoreFileChannelUnwrapper.unwrap( storeChannel );
    }

    private long lockPositionWriteVector(
            long filePageId, FileChannel channel, long fileOffset, ByteBuffer[] srcs ) throws IOException
    {
        try
        {
            long toWrite = filePageSize * (long) srcs.length;
            long bytesWritten = 0;
            synchronized ( positionLock( channel ) )
            {
                channel.position( fileOffset );
                do
                {
                    bytesWritten += channel.write( srcs );
                }
                while ( bytesWritten < toWrite );
                return bytesWritten;
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
            channel = unwrappedChannel( filePageId );
            long bytesWritten = lockPositionWriteVector( filePageId, channel, fileOffset, srcs );
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
            return bytesWritten;
        }
    }

    private Object positionLock( FileChannel channel )
    {
        sun.nio.ch.FileChannelImpl impl = (FileChannelImpl) channel;
        try
        {
            return (Object) positionLockGetter.invokeExact( impl );
        }
        catch ( Throwable th )
        {
            throw new LinkageError( "No getter for FileChannel.positionLock", th );
        }
    }

    private int writePositionVectoredFallback( long startFilePageId, Page[] pages, int arrayOffset, int length )
            throws IOException
    {
        int bytes = 0;
        for ( int i = 0; i < length; i++ )
        {
            bytes += write( startFilePageId + i, pages[arrayOffset + i] );
        }
        return bytes;
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
            if ( stripe == tokenChannelStripe )
            {
                // The closing of a FileChannel also releases all associated file locks.
                acquireLock();
            }
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
        try
        {
            closeAndCollectExceptions( 0, null );
        }
        finally
        {
            // Eagerly relinquish our reference to the onEviction callback, because even though
            // we've closed the PagedFile at this point, there are likely still pages in the cache that are bound to
            // this swapper, and will stay bound, until the eviction threads eventually gets around to kicking them out.
            // It is especially important to null out the onEviction callback field, because it is in turn holding on to
            // the striped translation table, which can be a rather large structure.
            onEviction = null;
        }
    }

    private void closeAndCollectExceptions( int channelIndex, IOException exception ) throws IOException
    {
        if ( channelIndex == channels.length )
        {
            if ( exception != null )
            {
                throw exception;
            }
            return;
        }

        try
        {
            channels[channelIndex].close();
        }
        catch ( IOException e )
        {
            if ( exception == null )
            {
                exception = e;
            }
            else
            {
                exception.addSuppressed( e );
            }
        }
        closeAndCollectExceptions( channelIndex + 1, exception );
    }

    @Override
    public void force() throws IOException
    {
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
