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
import java.nio.channels.ClosedChannelException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

/**
 * A simple PageSwapper implementation that directs all page swapping to a
 * single file on the file system.
 *
 * It additionally tracks the file size precisely, to avoid calling into the
 * file system whenever the size of the given file is queried.
 */
public class SingleFilePageSwapper implements PageSwapper
{
    private static final long fileSizeOffset =
            UnsafeUtil.getFieldOffset( SingleFilePageSwapper.class, "fileSize" );

    private final FileSystemAbstraction fs;
    private final File file;
    private final int filePageSize;
    private PageEvictionCallback onEviction;
    private volatile StoreChannel channel;

    // Guarded by synchronized(this). See tryReopen() and close().
    private boolean closed;

    // Accessed through unsafe
    private volatile long fileSize;

    public SingleFilePageSwapper(
            File file,
            FileSystemAbstraction fs,
            int filePageSize,
            PageEvictionCallback onEviction ) throws IOException
    {
        this.fs = fs;
        this.file = file;
        this.channel = fs.open( file, "rw" );
        this.filePageSize = filePageSize;
        this.onEviction = onEviction;
        increaseFileSizeTo( channel.size() );
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

    @Override
    public int read( long filePageId, Page page ) throws IOException
    {
        long offset = pageIdToPosition( filePageId );
        try
        {
            if ( offset < getCurrentFileSize() )
            {
                return page.swapIn( channel, offset, filePageSize );
            }
        }
        catch ( ClosedChannelException e )
        {
            // AsynchronousCloseException is a subclass of
            // ClosedChannelException, and ClosedByInterruptException is in
            // turn a subclass of AsynchronousCloseException.
            tryReopen( e );
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
        long offset = pageIdToPosition( filePageId );
        increaseFileSizeTo( offset + filePageSize );
        try
        {
            page.swapOut( channel, offset, filePageSize );
            return filePageSize;
        }
        catch ( ClosedChannelException e )
        {
            // AsynchronousCloseException is a subclass of
            // ClosedChannelException, and ClosedByInterruptException is in
            // turn a subclass of AsynchronousCloseException.
            tryReopen( e );
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
        if ( onEviction != null )
        {
            onEviction.onEvict( filePageId, page );
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

        return !(channel != null ? !channel.equals( that.channel ) : that.channel != null);
    }

    @Override
    public int hashCode()
    {
        return channel != null ? channel.hashCode() : 0;
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
            channel = fs.open( file, "rw" );
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
        channel.close();

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
        try
        {
            channel.force( false );
        }
        catch ( ClosedChannelException e )
        {
            // AsynchronousCloseException is a subclass of
            // ClosedChannelException, and ClosedByInterruptException is in
            // turn a subclass of AsynchronousCloseException.
            tryReopen( e );
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
    public String toString()
    {
        return "SingleFilePageSwapper{" +
                "filePageSize=" + filePageSize +
                ", file=" + file +
                '}';
    }
}
