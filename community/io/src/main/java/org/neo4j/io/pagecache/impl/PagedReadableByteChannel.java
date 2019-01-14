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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * Presents a {@link ReadableByteChannel} view of a {@link PagedFile}.
 * <p>
 * The byte channel will read the whole file sequentially from the beginning till the end.
 */
public final class PagedReadableByteChannel implements ReadableByteChannel
{
    private final PageCursor cursor;
    private boolean open = true;
    private int bytesLeftInCurrentPage;

    public PagedReadableByteChannel( PagedFile pagedFile ) throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK | PagedFile.PF_READ_AHEAD );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( !open )
        {
            throw new ClosedChannelException();
        }
        if ( bytesLeftInCurrentPage == 0 )
        {
            if ( cursor.next() )
            {
                bytesLeftInCurrentPage = cursor.getCurrentPageSize();
            }
            else
            {
                return -1;
            }
        }
        int position = dst.position();
        int remaining = Math.min( dst.remaining(), bytesLeftInCurrentPage );
        int offset = cursor.getOffset();
        do
        {
            dst.position( position );
            cursor.setOffset( offset );
            for ( int i = 0; i < remaining; i++ )
            {
                dst.put( cursor.getByte() );
            }
        }
        while ( cursor.shouldRetry() );
        bytesLeftInCurrentPage -= remaining;
        return remaining;
    }

    @Override
    public boolean isOpen()
    {
        return open;
    }

    @Override
    public void close()
    {
        open = false;
        cursor.close();
    }
}
