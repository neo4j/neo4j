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

    @Override
    public void mappedFile( File file )
    {
        delegate.mappedFile( file );
    }

    @Override
    public long bytesRead()
    {
        return delegate.bytesRead();
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        return delegate.beginFileFlush( swapper );
    }

    @Override
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
    {
        return delegate.beginPageEvictions( pageCountToEvict );
    }

    @Override
    public long unpins()
    {
        return delegate.unpins();
    }

    @Override
    public long hits()
    {
        return delegate.hits();
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        return delegate.beginCacheFlush();
    }

    @Override
    public long bytesWritten()
    {
        return delegate.bytesWritten();
    }

    @Override
    public long pins()
    {
        return delegate.pins();
    }

    @Override
    public long filesUnmapped()
    {
        return delegate.filesUnmapped();
    }

    @Override
    public void unmappedFile( File file )
    {
        delegate.unmappedFile( file );
    }

    @Override
    public long evictionExceptions()
    {
        return delegate.evictionExceptions();
    }

    @Override
    public double hitRatio()
    {
        return delegate.hitRatio();
    }

    @Override
    public double usageRatio()
    {
        return delegate.usageRatio();
    }

    @Override
    public void pins( long pins )
    {
        delegate.pins( pins );
    }

    @Override
    public void unpins( long unpins )
    {
        delegate.unpins( unpins );
    }

    @Override
    public void hits( long hits )
    {
        delegate.hits( hits );
    }

    @Override
    public void faults( long faults )
    {
        delegate.faults( faults );
    }

    @Override
    public void bytesRead( long bytesRead )
    {
        delegate.bytesRead( bytesRead );
    }

    @Override
    public void evictions( long evictions )
    {
        delegate.evictions( evictions );
    }

    @Override
    public void evictionExceptions( long evictionExceptions )
    {
        delegate.evictionExceptions( evictionExceptions );
    }

    @Override
    public void bytesWritten( long bytesWritten )
    {
        delegate.bytesWritten( bytesWritten );
    }

    @Override
    public void flushes( long flushes )
    {
        delegate.flushes( flushes );
    }

    @Override
    public void maxPages( long maxPages )
    {
        delegate.maxPages( maxPages );
    }

    @Override
    public long filesMapped()
    {
        return delegate.filesMapped();
    }

    @Override
    public long flushes()
    {
        return delegate.flushes();
    }

    @Override
    public long faults()
    {
        return delegate.faults();
    }

    @Override
    public long evictions()
    {
        return delegate.evictions();
    }
}
