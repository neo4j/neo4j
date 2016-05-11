/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class PagedWritableByteChannel implements WritableByteChannel
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
    public void close() throws IOException
    {
        open = false;
        cursor.close();
    }
}
