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
package org.neo4j.io.pagecache.impl;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.impl.muninn.UnsafeUtil;

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

    private final File file;
    private final StoreChannel channel;
    private final int filePageSize;
    private final PageEvictionCallback onEviction;

    // Accessed through unsafe
    private volatile long fileSize;

    public SingleFilePageSwapper(
            File file,
            FileSystemAbstraction fs,
            int filePageSize,
            PageEvictionCallback onEviction ) throws IOException
    {
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
    public void read( long filePageId, Page page ) throws IOException
    {
        long offset = pageIdToPosition( filePageId );
        if ( offset < getCurrentFileSize() )
        {
            page.swapIn( channel, offset, filePageSize );
        }
    }

    @Override
    public void write( long filePageId, Page page ) throws IOException
    {
        long offset = pageIdToPosition( filePageId );
        increaseFileSizeTo( offset + filePageSize );
        page.swapOut( channel, offset, filePageSize );
    }

    @Override
    public void evicted( long filePageId, Page page )
    {
        onEviction.onEvict( filePageId, page );
    }

    @Override
    public String fileName()
    {
        return file.getName();
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

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public void force() throws IOException
    {
        channel.force( false );
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
