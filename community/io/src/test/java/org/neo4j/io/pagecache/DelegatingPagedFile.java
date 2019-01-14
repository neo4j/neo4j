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
package org.neo4j.io.pagecache;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class DelegatingPagedFile implements PagedFile
{
    private final PagedFile delegate;

    public DelegatingPagedFile( PagedFile delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public PageCursor io( long pageId, int pf_flags ) throws IOException
    {
        return delegate.io( pageId, pf_flags );
    }

    @Override
    public void flushAndForce() throws IOException
    {
        delegate.flushAndForce();
    }

    @Override
    public long getLastPageId() throws IOException
    {
        return delegate.getLastPageId();
    }

    @Override
    public int pageSize()
    {
        return delegate.pageSize();
    }

    @Override
    public long fileSize() throws IOException
    {
        return delegate.fileSize();
    }

    @Override
    public File file()
    {
        return delegate.file();
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }

    @Override
    public ReadableByteChannel openReadableByteChannel() throws IOException
    {
        return delegate.openReadableByteChannel();
    }

    @Override
    public WritableByteChannel openWritableByteChannel() throws IOException
    {
        return delegate.openWritableByteChannel();
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        delegate.flushAndForce( limiter );
    }
}
