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

    @Override
    public long read( long filePageId, long bufferAddress, int bufferSize ) throws IOException
    {
        return delegate.read( filePageId, bufferAddress, bufferSize );
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }

    @Override
    public void evicted( long filePageId )
    {
        delegate.evicted( filePageId );
    }

    @Override
    public void force() throws IOException
    {
        delegate.force();
    }

    @Override
    public File file()
    {
        return delegate.file();
    }

    @Override
    public long write( long filePageId, long bufferAddress ) throws IOException
    {
        return delegate.write( filePageId, bufferAddress );
    }

    @Override
    public long getLastPageId() throws IOException
    {
        return delegate.getLastPageId();
    }

    @Override
    public void truncate() throws IOException
    {
        delegate.truncate();
    }

    @Override
    public void closeAndDelete() throws IOException
    {
        delegate.closeAndDelete();
    }

    @Override
    public long read( long startFilePageId, long[] bufferAddresses, int bufferSize, int arrayOffset, int length ) throws IOException
    {
        return delegate.read( startFilePageId, bufferAddresses, bufferSize, arrayOffset, length );
    }

    @Override
    public long write( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length ) throws IOException
    {
        return delegate.write( startFilePageId, bufferAddresses, arrayOffset, length );
    }
}
