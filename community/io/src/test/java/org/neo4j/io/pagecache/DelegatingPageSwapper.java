/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;

/**
 * A PageSwapper that delegates all calls to a wrapped PageSwapper instance.
 *
 * Useful for overriding specific functionality in a sub-class.
 */
public class DelegatingPageSwapper implements PageSwapper
{
    private final PageSwapper delegate;

    public DelegatingPageSwapper( PageSwapper delegate )
    {
        this.delegate = delegate;
    }

    public long read( long filePageId, Page page ) throws IOException
    {
        return delegate.read( filePageId, page );
    }

    public void close() throws IOException
    {
        delegate.close();
    }

    public void evicted( long pageId, Page page )
    {
        delegate.evicted( pageId, page );
    }

    public void force() throws IOException
    {
        delegate.force();
    }

    public File file()
    {
        return delegate.file();
    }

    public long write( long filePageId, Page page ) throws IOException
    {
        return delegate.write( filePageId, page );
    }

    public long getLastPageId() throws IOException
    {
        return delegate.getLastPageId();
    }

    public void truncate() throws IOException
    {
        delegate.truncate();
    }

    public long read( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException
    {
        return delegate.read( startFilePageId, pages, arrayOffset, length );
    }

    public long write( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException
    {
        return delegate.write( startFilePageId, pages, arrayOffset, length );
    }
}
