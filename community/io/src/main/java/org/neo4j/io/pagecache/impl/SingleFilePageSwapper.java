/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.pagecache.impl;

import com.sun.nio.file.ExtendedOpenOption;
import org.apache.commons.lang3.SystemUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;

import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.util.FeatureToggles;

import static org.apache.commons.lang3.SystemUtils.IS_OS_LINUX;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.WRITE_OPTIONS;
import static org.neo4j.io.fs.FileSystemAbstraction.INVALID_FILE_DESCRIPTOR;

/**
 * A simple PageSwapper implementation that directs all page swapping to a
 * single file on the file system.
 *
 * It additionally tracks the file size precisely, to avoid calling into the
 * file system whenever the size of the given file is queried.
 */
public class SingleFilePageSwapper implements PageSwapper
{
    private static final boolean PREALLOCATE_MAPPED_FILES = FeatureToggles.flag( SingleFilePageSwapper.class, "PREALLOCATE_MAPPED_FILES", true );
    private static final long FILE_SIZE_OFFSET = UnsafeUtil.getFieldOffset( SingleFilePageSwapper.class, "fileSize" );
    private static final ThreadLocal<ByteBuffer> PROXY_CACHE = new ThreadLocal<>();

    private static ByteBuffer proxy( long buffer, int bufferLength ) throws IOException
    {
        ByteBuffer buf = PROXY_CACHE.get();
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
        PROXY_CACHE.set( buf );
        return buf;
    }

    private final FileSystemAbstraction fs;
    private final Path path;
    private final int filePageSize;
    private final Set<OpenOption> openOptions;
    private volatile PageEvictionCallback onEviction;
    private StoreChannel channel;
    private FileLock fileLock;
    private final boolean hasPositionLock;

    // Guarded by synchronized(this). See tryReopen() and close().
    private boolean closed;

    @SuppressWarnings( "unused" ) // Accessed through unsafe
    private volatile long fileSize;

    SingleFilePageSwapper( Path path, FileSystemAbstraction fs, int filePageSize, PageEvictionCallback onEviction, boolean useDirectIO ) throws IOException
    {
        this.fs = fs;
        this.path = path;

        var options = new ArrayList<>( WRITE_OPTIONS );
        if ( useDirectIO )
        {
            validateDirectIOPossibility( path, filePageSize );
            options.add( ExtendedOpenOption.DIRECT );
        }
        openOptions = Set.copyOf( options );
        channel = createStoreChannel();

        this.filePageSize = filePageSize;
        this.onEviction = onEviction;
        increaseFileSizeTo( channel.size() );

        try
        {
            acquireLock();
        }
        catch ( IOException e )
        {
            try
            {
                channel.close();
            }
            catch ( IOException ioe )
            {
                e.addSuppressed( ioe );
            }
            throw e;
        }
        hasPositionLock = channel.hasPositionLock();
    }

    private StoreChannel createStoreChannel() throws IOException
    {
        var storeChannel = fs.open( path, openOptions );
        storeChannel.tryMakeUninterruptible();
        return storeChannel;
    }

    private void validateDirectIOPossibility( Path file, int filePageSize ) throws IOException
    {
        if ( !IS_OS_LINUX )
        {
            throw new IllegalArgumentException( "DirectIO support is available only on Linux." );
        }
        final long blockSize = fs.getBlockSize( file );
        long value = filePageSize / blockSize;
        if ( value * blockSize != filePageSize )
        {
            throw new IllegalArgumentException( "Direct IO can be used only when page cache page size is a multiplier of a block size. "
                    + "File page size: " + filePageSize + ", block size: " + blockSize );
        }
    }

    private void increaseFileSizeTo( long newFileSize )
    {
        long currentFileSize;
        do
        {
            currentFileSize = getCurrentFileSize();
        }
        while ( currentFileSize < newFileSize && !UnsafeUtil.compareAndSwapLong(
                this, FILE_SIZE_OFFSET, currentFileSize, newFileSize ) );
    }

    private long getCurrentFileSize()
    {
        return UnsafeUtil.getLongVolatile( this, FILE_SIZE_OFFSET );
    }

    private void setCurrentFileSize( long size )
    {
        UnsafeUtil.putLongVolatile( this, FILE_SIZE_OFFSET, size );
    }

    private void acquireLock() throws IOException
    {
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            // We don't take file locks on the individual store files on Windows, because once you've taking
            // a file lock on a channel, you can only do IO on that file through that channel.
            return;
        }

        try
        {
            fileLock = channel.tryLock();
            if ( fileLock == null )
            {
                throw new FileLockException( path );
            }
        }
        catch ( OverlappingFileLockException e )
        {
            throw new FileLockException( path, e );
        }
    }

    private int swapIn( long bufferAddress, long fileOffset, int bufferSize ) throws IOException
    {
        int readTotal = 0;
        try
        {
            ByteBuffer bufferProxy = proxy( bufferAddress, bufferSize );
            int read;
            do
            {
                read = channel.read( bufferProxy, fileOffset + readTotal );
            }
            while ( read != -1 && (readTotal += read) < bufferSize );

            // Zero-fill the rest.
            int rest = bufferSize - readTotal;
            if ( rest > 0 )
            {
                UnsafeUtil.setMemory( bufferAddress + readTotal, rest, MuninnPageCache.ZERO_BYTE );
            }
            return readTotal;
        }
        catch ( IOException e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            throw new IOException( formatSwapInErrorMessage( fileOffset, bufferSize, readTotal ), e );
        }
    }

    private static String formatSwapInErrorMessage( long fileOffset, int size, int readTotal )
    {
        return "Read failed after " + readTotal + " of " + size + " bytes from fileOffset " + fileOffset + ".";
    }

    private int swapOut( long bufferAddress, long fileOffset, int bufferLength ) throws IOException
    {
        try
        {
            ByteBuffer bufferProxy = proxy( bufferAddress, bufferLength );
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
        return bufferLength;
    }

    private void clear( long bufferAddress, int bufferSize )
    {
        UnsafeUtil.setMemory( bufferAddress, bufferSize, MuninnPageCache.ZERO_BYTE );
    }

    @Override
    public long read( long filePageId, long bufferAddress ) throws IOException
    {
        return read( filePageId, bufferAddress, filePageSize );
    }

    @Override
    public long read( long filePageId, long bufferAddress, int bufferLength ) throws IOException
    {
        try ( Retry retry = new Retry() )
        {
            do
            {
                try
                {
                    long fileOffset = pageIdToPosition( filePageId );
                    if ( fileOffset < getCurrentFileSize() )
                    {
                        return swapIn( bufferAddress, fileOffset, bufferLength );
                    }

                    clear( bufferAddress, bufferLength );
                    return 0;
                }
                catch ( ClosedChannelException e )
                {
                    retry.caught( e );
                }
            }
            while ( retry.shouldRetry() );
        }
        return -1;
    }

    @Override
    public long read( long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length ) throws IOException
    {
        if ( length == 0 )
        {
            return 0;
        }

        try ( Retry retry = new Retry() )
        {
            do
            {
                try
                {
                    if ( hasPositionLock )
                    {
                        return readPositionedVectoredToFileChannel( startFilePageId, bufferAddresses, bufferLengths, length );
                    }
                    return readPositionedVectoredFallback( startFilePageId, bufferAddresses, bufferLengths, length );
                }
                catch ( ClosedChannelException e )
                {
                    retry.caught( e );
                }
            }
            while ( retry.shouldRetry() );
        }
        return -1;
    }

    private long readPositionedVectoredToFileChannel( long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length ) throws IOException
    {
        long fileOffset = pageIdToPosition( startFilePageId );
        long bytesToRead = countBuffersLengths( bufferLengths, length );
        ByteBuffer[] srcs = convertToByteBuffers( bufferAddresses, bufferLengths, length );
        long bytesRead = lockPositionReadVector( fileOffset, srcs, bytesToRead );
        if ( bytesRead == -1 )
        {
            for ( int i = 0; i < length; i++ )
            {
                UnsafeUtil.setMemory( bufferAddresses[i], bufferLengths[i], MuninnPageCache.ZERO_BYTE );
            }
            return 0;
        }
        else if ( bytesRead < bytesToRead )
        {
            long bytesToKeep = bytesRead;
            for ( int bufferIndex = 0; bufferIndex < length; bufferIndex++ )
            {
                int bufferLength = bufferLengths[bufferIndex];
                if ( bytesToKeep > bufferLength )
                {
                    bytesToKeep = Math.subtractExact( bytesToKeep, bufferLength );
                }
                else
                {
                    UnsafeUtil.setMemory( bufferAddresses[bufferIndex] + bytesToKeep, bufferLength - bytesToKeep, MuninnPageCache.ZERO_BYTE );
                    bytesToKeep = 0;
                }
            }
        }
        return bytesRead;
    }

    private long countBuffersLengths( int[] bufferLengths, int length )
    {
        long bytesToRead = 0;
        for ( int i = 0; i < length; i++ )
        {
            bytesToRead += bufferLengths[i];
        }
        return bytesToRead;
    }

    private long lockPositionReadVector( long fileOffset, ByteBuffer[] srcs, long bytesToRead ) throws IOException
    {
        long read;
        long readTotal = 0;
        synchronized ( channel.getPositionLock() )
        {
            setPositionUnderLock( fileOffset );
            do
            {
                read = channel.read( srcs );
            }
            while ( read != -1 && (readTotal += read) < bytesToRead );
            return readTotal;
        }
    }

    private int readPositionedVectoredFallback( long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length ) throws IOException
    {
        int bytes = 0;
        long filePageId = startFilePageId;
        for ( int i = 0; i < length; i++ )
        {
            long address = bufferAddresses[i];
            int bufferLength = bufferLengths[i];
            bytes += read( filePageId, address, bufferLength );
            filePageId += bufferLength / filePageSize;
        }
        return bytes;
    }

    @Override
    public long write( long filePageId, long bufferAddress ) throws IOException
    {
        return write( filePageId, bufferAddress, filePageSize );
    }

    @Override
    public long write( long filePageId, long bufferAddress, int bufferLength ) throws IOException
    {
        long fileOffset = pageIdToPosition( filePageId );
        increaseFileSizeTo( fileOffset + bufferLength );

        try ( Retry retry = new Retry() )
        {
            do
            {
                try
                {
                    return swapOut( bufferAddress, fileOffset, bufferLength );
                }
                catch ( ClosedChannelException e )
                {
                    retry.caught( e );
                }
            }
            while ( retry.shouldRetry() );
        }
        return -1;
    }

    @Override
    public long write( long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length, int totalAffectedPages )
            throws IOException
    {
        if ( totalAffectedPages == 0 )
        {
            return 0;
        }

        try ( Retry retry = new Retry() )
        {
            do
            {
                try
                {
                    if ( hasPositionLock )
                    {
                        return writePositionedVectoredToFileChannel( startFilePageId, bufferAddresses, bufferLengths, length );
                    }
                    return writePositionVectoredFallback( startFilePageId, bufferAddresses, bufferLengths, length );
                }
                catch ( ClosedChannelException e )
                {
                    retry.caught( e );
                }
            }
            while ( retry.shouldRetry() );
        }
        return -1;
    }

    private long writePositionedVectoredToFileChannel( long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length )
            throws IOException
    {
        long fileOffset = pageIdToPosition( startFilePageId );
        long bytesToWrite = countBuffersLengths(bufferLengths, length );
        increaseFileSizeTo( fileOffset + bytesToWrite );
        ByteBuffer[] srcs = convertToByteBuffers( bufferAddresses, bufferLengths, length );
        return lockPositionWriteVector( fileOffset, srcs, bytesToWrite );
    }

    private ByteBuffer[] convertToByteBuffers( long[] bufferAddresses, int[] bufferLengths, int length )
    {
        ByteBuffer[] buffers = new ByteBuffer[length];
        for ( int i = 0; i < length; i++ )
        {
            try
            {
                buffers[i] = UnsafeUtil.newDirectByteBuffer( bufferAddresses[i], bufferLengths[i] );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Failed to wrap pointer in ByteBuffer.", e );
            }
        }
        return buffers;
    }

    private long lockPositionWriteVector( long fileOffset, ByteBuffer[] srcs, long bytesToWrite ) throws IOException
    {
        try
        {
            long bytesWritten = 0;
            synchronized ( channel.getPositionLock() )
            {
                setPositionUnderLock( fileOffset );
                do
                {
                    bytesWritten += channel.write( srcs );
                }
                while ( bytesWritten < bytesToWrite );
                return bytesWritten;
            }
        }
        catch ( ClosedChannelException e )
        {
            tryReopen( e );
            throw new IOException( "IO failed due to interruption", e );
        }
    }

    private void setPositionUnderLock( long fileOffset ) throws IOException
    {
        try
        {
            channel.position( fileOffset );
        }
        catch ( IllegalArgumentException e )
        {
            // This is thrown if the position is negative. Turn it into an IOException,
            // which is the same exception that would be thrown in the non-vectored code path.
            throw new IOException( e );
        }
    }

    private int writePositionVectoredFallback( long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length )
            throws IOException
    {
        int bytes = 0;
        long filePageId = startFilePageId;
        for ( int i = 0; i < length; i++ )
        {
            long address = bufferAddresses[i];
            int bufferLength = bufferLengths[i];
            bytes += write( filePageId, address, bufferLength );
            filePageId += bufferLength / filePageSize;
        }
        return bytes;
    }

    @Override
    public void evicted( long filePageId )
    {
        PageEvictionCallback callback = this.onEviction;
        if ( callback != null )
        {
            callback.onEvict( filePageId );
        }
    }

    @Override
    public Path path()
    {
        return path;
    }

    private long pageIdToPosition( long pageId )
    {
        return filePageSize * pageId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        SingleFilePageSwapper that = (SingleFilePageSwapper) o;

        return path.equals( that.path );
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
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
    private synchronized void tryReopen( ClosedChannelException closedException ) throws ClosedChannelException
    {
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
            channel = createStoreChannel();
            // The closing of a FileChannel also releases all associated file locks.
            acquireLock();
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
            channel.close();
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

    @Override
    public synchronized void closeAndDelete() throws IOException
    {
        close();
        fs.deleteFile( path );
    }

    @Override
    public void force() throws IOException
    {
        try ( Retry retry = new Retry() )
        {
            do
            {
                try
                {
                    channel.force( false );
                }
                catch ( ClosedChannelException e )
                {
                    retry.caught( e );
                }
            }
            while ( retry.shouldRetry() );
        }
    }

    @Override
    public long getLastPageId()
    {
        long channelSize = getCurrentFileSize();
        if ( channelSize == 0 )
        {
            return PageCursor.UNBOUND_PAGE_ID;
        }
        long div = channelSize / filePageSize;
        long mod = channelSize % filePageSize;
        return mod == 0 ? div - 1 : div;
    }

    @Override
    public void truncate() throws IOException
    {
        setCurrentFileSize( 0 );
        try ( Retry retry = new Retry() )
        {
            do
            {
                try
                {
                    channel.truncate( 0 );
                }
                catch ( ClosedChannelException e )
                {
                    retry.caught( e );
                }
            }
            while ( retry.shouldRetry() );
        }
    }

    @Override
    public boolean canAllocate()
    {
        return PREALLOCATE_MAPPED_FILES
                && NativeAccessProvider.getNativeAccess().isAvailable()
                // this type of operation requires the underlying channel to provide a file descriptor
                && channel.getFileDescriptor() != INVALID_FILE_DESCRIPTOR;
    }

    @Override
    public void allocate( long newFileSize ) throws IOException
    {
        NativeAccess access = NativeAccessProvider.getNativeAccess();
        if ( access.isAvailable() )
        {
            NativeCallResult result = access.tryPreallocateSpace( channel.getFileDescriptor(), newFileSize );
            if ( result.isError() )
            {
                throw new IOException( result.getErrorMessage() );
            }
        }
    }

    @Override
    public String toString()
    {
        return "SingleFilePageSwapper{" +
                "filePageSize=" + filePageSize +
                ", file=" + path +
                '}';
    }

    private class Retry implements AutoCloseable
    {
        private static final int RETRIES_ON_INTERRUPTION = 10;
        private int retries = RETRIES_ON_INTERRUPTION;
        private ClosedChannelException caughtException;
        private ClosedChannelException initialException;
        private boolean wasInterrupted;

        boolean shouldRetry() throws ClosedChannelException
        {
            if ( caughtException != null && --retries >= 0 ) // we failed and have more retries to do
            {
                wasInterrupted |= Thread.interrupted();
                tryReopen( caughtException );
                caughtException = null;
                return true;
            }
            return false;
        }

        void caught( ClosedChannelException exception )
        {
            caughtException = exception;
            if ( initialException == null )
            {
                initialException = caughtException;
            }
        }

        @Override
        public void close() throws ClosedChannelException
        {
            if ( wasInterrupted )
            {
                //restore interruption flag
                Thread.currentThread().interrupt();
            }
            if ( caughtException != null )
            {
                //this means we failed on our last retry
                initialException.addSuppressed( caughtException );
                throw initialException;
            }

        }
    }
}
