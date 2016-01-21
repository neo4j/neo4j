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
package org.neo4j.io.pagecache.tracing;

import java.io.File;

import org.neo4j.io.pagecache.PageSwapper;

/**
 * A PageCacheTracer that delegates all calls to a wrapped instance.
 *
 * Useful for overriding specific functionality in a sub-class.
 */
public class DelegatingPageCacheTracer implements PageCacheTracer
{
    private final PageCacheTracer delegate;

    public DelegatingPageCacheTracer( PageCacheTracer delegate )
    {
        this.delegate = delegate;
    }

    public void mappedFile( File file )
    {
        delegate.mappedFile( file );
    }

    public long bytesRead()
    {
        return delegate.bytesRead();
    }

    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        return delegate.beginFileFlush( swapper );
    }

    public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
    {
        return delegate.beginPageEvictions( pageCountToEvict );
    }

    public long unpins()
    {
        return delegate.unpins();
    }

    public MajorFlushEvent beginCacheFlush()
    {
        return delegate.beginCacheFlush();
    }

    public long bytesWritten()
    {
        return delegate.bytesWritten();
    }

    public long pins()
    {
        return delegate.pins();
    }

    public long filesUnmapped()
    {
        return delegate.filesUnmapped();
    }

    public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
    {
        return delegate.beginPin( writeLock, filePageId, swapper );
    }

    public void unmappedFile( File file )
    {
        delegate.unmappedFile( file );
    }

    public long evictionExceptions()
    {
        return delegate.evictionExceptions();
    }

    public long filesMapped()
    {
        return delegate.filesMapped();
    }

    public long flushes()
    {
        return delegate.flushes();
    }

    public long faults()
    {
        return delegate.faults();
    }

    public long evictions()
    {
        return delegate.evictions();
    }
}
