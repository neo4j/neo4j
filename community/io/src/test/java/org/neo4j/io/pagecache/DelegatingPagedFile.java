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
package org.neo4j.io.pagecache;

import java.io.IOException;

public class DelegatingPagedFile implements PagedFile
{
    private final PagedFile delegate;

    public DelegatingPagedFile( PagedFile delegate )
    {
        this.delegate = delegate;
    }

    public PageCursor io( long pageId, int pf_flags ) throws IOException
    {
        return delegate.io( pageId, pf_flags );
    }

    public void flushAndForce() throws IOException
    {
        delegate.flushAndForce();
    }

    public long getLastPageId() throws IOException
    {
        return delegate.getLastPageId();
    }

    public int pageSize()
    {
        return delegate.pageSize();
    }

    public void close() throws IOException
    {
        delegate.close();
    }

    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        delegate.flushAndForce( limiter );
    }
}
