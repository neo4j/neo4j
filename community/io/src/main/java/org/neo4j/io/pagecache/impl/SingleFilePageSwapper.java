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
package org.neo4j.io.pagecache.impl;

import com.sun.nio.file.ExtendedOpenOption;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Set;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

import static org.apache.commons.lang3.SystemUtils.IS_OS_LINUX;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.WRITE_OPTIONS;

/**
 * A simple PageSwapper implementation that directs all page swapping to a
 * single file on the file system.
 *
 * It additionally tracks the file size precisely, to avoid calling into the
 * file system whenever the size of the given file is queried.
 */
public class SingleFilePageSwapper implements PageSwapper
{
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
    private final File file;
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

    SingleFilePageSwapper( File file, FileSystemAbstraction fs, int filePageSize, PageEvictionCallback onEviction, boolean noChannelStriping,
            boolean useDirectIO ) throws IOException
    {
        this.fs = fs;
        this.file = file;

        var options = new ArrayList<>( WRITE_OPTIONS );
        if ( useDirectIO )
        {
            validateDirectIOPossibility( file, filePageSize );
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
        var storeChannel = fs.open( file, openOptions );
        storeChannel.tryMakeUninterruptible();
        return storeChannel;
    }

    private void validateDirectIOPossibility( File file, int filePageSize ) throws IOException
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
                throw new FileLockException( file );
            }
        }
        catch ( OverlappingFileLockException e )
        {
            throw new FileLockException( file, e );
        }
    }

    private int swapIn( long bufferAddress, long fileOffset ) throws IOException
    {
        int readTotal = 0;
        try
        {
            ByteBuffer bufferProxy = proxy( bufferAddress, filePageSize );
            int read;
            do
            {
                read = channel.read( bufferProxy, fileOffset + readTotal );
            }
            while ( read != -1 && (readTotal += read) < filePageSize );

            // Zero-fill the rest.
            int rest = filePageSize - readTotal;
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
            throw new IOException( formatSwapInErrorMessage( fileOffset, filePageSize, readTotal ), e );
        }
    }

    private static String formatSwapInErrorMessage( long fileOffset, int filePageSize, int readTotal )
    {
        return "Read failed after " + readTotal + " of " + filePageSize + " bytes from fileOffset " + fileOffset + ".";
    }

    private int swapOut( long bufferAddress, long fileOffset ) throws IOException
    {
        try
        {
            ByteBuffer bufferProxy = proxy( bufferAddress, filePageSize );
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

    private void clear( long bufferAddress, int bufferSize )
    {
        UnsafeUtil.setMemory( bufferAddress, bufferSize, MuninnPageCache.ZERO_BYTE );
    }

    @Override
    public long read( long filePageId, long bufferAddress ) throws IOException
    {
        long fileOffset = pageIdToPosition( filePageId );
        try
        {
            if ( fileOffset < getCurrentFileSize() )
            {
                return swapIn( bufferAddress, fileOffset );
            }
            else
            {
                clear( bufferAddress, filePageSize );
            }
        }
        catch ( ClosedChannelException e )
        {
            tryReopen( e );
            throw new IOException( "IO failed due to interruption", e );
        }
        return 0;
    }

    @Override
    public long read( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length ) throws IOException
    {
        if ( length == 0 )
        {
            return 0;
        }
        if ( hasPositionLock )
        {
            return readPositionedVectoredToFileChannel( startFilePageId, bufferAddresses, arrayOffset, length );
        }
        return readPositionedVectoredFallback( startFilePageId, bufferAddresses, arrayOffset, length );
    }

    private long readPositionedVectoredToFileChannel( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length ) throws IOException
    {
        long fileOffset = pageIdToPosition( startFilePageId );
        ByteBuffer[] srcs = convertToByteBuffers( bufferAddresses, arrayOffset, length );
        long bytesRead = lockPositionReadVector( fileOffset, srcs );
        if ( bytesRead == -1 )
        {
            for ( long address : bufferAddresses )
            {
                UnsafeUtil.setMemory( address, filePageSize, MuninnPageCache.ZERO_BYTE );
            }
            return 0;
        }
        else if ( bytesRead < ((long) filePageSize) * length )
        {
            int pagesRead = (int) (bytesRead / filePageSize);
            int bytesReadIntoLastReadPage = (int) (bytesRead % filePageSize);
            int pagesNeedingZeroing = length - pagesRead;
            for ( int i = 0; i < pagesNeedingZeroing; i++ )
            {
                long address = bufferAddresses[arrayOffset + pagesRead + i];
                long bytesToZero = filePageSize;
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

    private long lockPositionReadVector( long fileOffset, ByteBuffer[] srcs ) throws IOException
    {
        try
        {
            long toRead = filePageSize * (long) srcs.length;
            long read;
            long readTotal = 0;
            synchronized ( channel.getPositionLock() )
            {
                setPositionUnderLock( fileOffset );
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
            tryReopen( e );
            throw new IOException( "IO failed due to interruption", e );
        }
    }

    private int readPositionedVectoredFallback( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length ) throws IOException
    {
        int bytes = 0;
        for ( int i = 0; i < length; i++ )
        {
            long address = bufferAddresses[arrayOffset + i];
            bytes += read( startFilePageId + i, address );
        }
        return bytes;
    }

    @Override
    public long write( long filePageId, long bufferAddress ) throws IOException
    {
        long fileOffset = pageIdToPosition( filePageId );
        increaseFileSizeTo( fileOffset + filePageSize );
        try
        {
            return swapOut( bufferAddress, fileOffset );
        }
        catch ( ClosedChannelException e )
        {
            tryReopen( e );
            throw new IOException( "IO failed due to interruption", e );
        }
    }

    @Override
    public long write( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length ) throws IOException
    {
        if ( length == 0 )
        {
            return 0;
        }
        if ( hasPositionLock )
        {
            return writePositionedVectoredToFileChannel( startFilePageId, bufferAddresses, arrayOffset, length );
        }
        return writePositionVectoredFallback( startFilePageId, bufferAddresses, arrayOffset, length );
    }

    private long writePositionedVectoredToFileChannel( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length ) throws IOException
    {
        long fileOffset = pageIdToPosition( startFilePageId );
        increaseFileSizeTo( fileOffset + (((long) filePageSize) * length) );
        ByteBuffer[] srcs = convertToByteBuffers( bufferAddresses, arrayOffset, length );
        return lockPositionWriteVector( fileOffset, srcs );
    }

    private ByteBuffer[] convertToByteBuffers( long[] bufferAddresses, int arrayOffset, int length )
    {
        ByteBuffer[] buffers = new ByteBuffer[length];
        for ( int i = 0; i < length; i++ )
        {
            long address = bufferAddresses[arrayOffset + i];
            try
            {
                buffers[i] = UnsafeUtil.newDirectByteBuffer( address, filePageSize );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Failed to wrap pointer in ByteBuffer.", e );
            }
        }
        return buffers;
    }

    private long lockPositionWriteVector( long fileOffset, ByteBuffer[] srcs ) throws IOException
    {
        try
        {
            long toWrite = filePageSize * (long) srcs.length;
            long bytesWritten = 0;
            synchronized ( channel.getPositionLock() )
            {
                setPositionUnderLock( fileOffset );
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

    private int writePositionVectoredFallback( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length )
            throws IOException
    {
        int bytes = 0;
        for ( int i = 0; i < length; i++ )
        {
            long address = bufferAddresses[arrayOffset + i];
            bytes += write( startFilePageId + i, address );
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
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

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
        fs.deleteFile( file );
    }

    @Override
    public void force() throws IOException
    {
        try
        {
            channel.force( false );
        }
        catch ( ClosedChannelException e )
        {
            tryReopen( e );
            throw new IOException( "IO failed due to interruption", e );
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
        try
        {
            channel.truncate( 0 );
        }
        catch ( ClosedChannelException e )
        {
            tryReopen( e );
            throw new IOException( "IO failed due to interruption", e );
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
