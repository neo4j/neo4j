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
package org.neo4j.io.pagecache.impl.common;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;

public class SingleFilePageSwapper implements PageSwapper
{
    private final File file;
    private final StoreChannel channel;
    private final int filePageSize;
    private final PageEvictionCallback onEviction;

    public SingleFilePageSwapper(
            File file,
            StoreChannel channel,
            int filePageSize,
            PageEvictionCallback onEviction )
    {
        this.file = file;
        this.channel = channel;
        this.filePageSize = filePageSize;
        this.onEviction = onEviction;
    }

    @Override
    public void read( long filePageId, Page page ) throws IOException
    {
        long offset = pageIdToPosition( filePageId );
        if ( offset < channel.size() )
        {
            page.swapIn( channel, offset, filePageSize );
        }
    }

    @Override
    public void write( long filePageId, Page page ) throws IOException
    {
        long offset = pageIdToPosition( filePageId );
        page.swapOut( channel, offset, filePageSize );
    }

    @Override
    public void evicted( long filePageId )
    {
        onEviction.onEvict( filePageId );
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
        long channelSize = channel.size();
        if ( channelSize == 0 )
        {
            return PageCursor.UNBOUND_PAGE_ID;
        }
        long div = channelSize / filePageSize;
        long mod = channelSize % filePageSize;
        return mod == 0? div - 1 : div;
    }
}
