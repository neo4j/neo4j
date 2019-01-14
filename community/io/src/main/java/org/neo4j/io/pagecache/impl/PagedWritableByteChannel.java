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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * Presents a {@link WritableByteChannel} view of the {@link PagedFile}.
 * <p>
 * The paged file will be overwritten sequentially, from the beginning till the end.
 * <p>
 * If the file already contains data, and the channel is not given enough data to overwrite the file completely,
 * then the data at the end of the file will be left untouched.
 * <p>
 * If this is undesired, then the file can be mapped with {@link java.nio.file.StandardOpenOption#TRUNCATE_EXISTING}
 * to remove the existing data before writing to the file.
 * @see org.neo4j.io.pagecache.PageCache#map(File, int, OpenOption...)
 */
public final class PagedWritableByteChannel implements WritableByteChannel
{
    private final PageCursor cursor;
    private boolean open = true;
    private int bytesLeftInCurrentPage;

    public PagedWritableByteChannel( PagedFile pagedFile ) throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        if ( !open )
        {
            throw new ClosedChannelException();
        }
        if ( bytesLeftInCurrentPage == 0 )
        {
            if ( !cursor.next() )
            {
                throw new IOException( "Could not advance write cursor" );
            }
            bytesLeftInCurrentPage = cursor.getCurrentPageSize();
        }
        int remaining = Math.min( src.remaining(), bytesLeftInCurrentPage );
        for ( int i = 0; i < remaining; i++ )
        {
            cursor.putByte( src.get() );
        }
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
