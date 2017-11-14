/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.pagecache;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class StubPagedFile implements PagedFile
{
    private final int pageSize;
    public final int exposedPageSize;
    public long lastPageId = 1;

    public StubPagedFile( int pageSize )
    {
        this.pageSize = pageSize;
        this.exposedPageSize = pageSize;
    }

    @Override
    public PageCursor io( long pageId, int pf_flags ) throws IOException
    {
        StubPageCursor cursor = new StubPageCursor( pageId, pageSize );
        prepareCursor( cursor );
        return cursor;
    }

    @Override
    public int prefetch( long startPageId ) throws IOException
    {
        return 0;
    }

    protected void prepareCursor( StubPageCursor cursor )
    {
    }

    @Override
    public int pageSize()
    {
        return exposedPageSize;
    }

    @Override
    public long fileSize() throws IOException
    {
        final long lastPageId = getLastPageId();
        if ( lastPageId < 0 )
        {
            return 0L;
        }
        return (lastPageId + 1) * pageSize();
    }

    @Override
    public void flushAndForce() throws IOException
    {
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
    }

    @Override
    public long getLastPageId() throws IOException
    {
        return lastPageId;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public ReadableByteChannel openReadableByteChannel()
    {
        throw new UnsupportedOperationException( "Not implemented for StubPagedFile" );
    }

    @Override
    public WritableByteChannel openWritableByteChannel()
    {
        throw new UnsupportedOperationException( "Not implemented for StubPagedFile" );
    }
}
