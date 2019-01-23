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
package org.neo4j.io.pagecache.tracing.linear;

import java.io.File;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static org.neo4j.io.pagecache.tracing.linear.HEvents.EvictionRunHEvent;
import static org.neo4j.io.pagecache.tracing.linear.HEvents.MajorFlushHEvent;
import static org.neo4j.io.pagecache.tracing.linear.HEvents.MappedFileHEvent;
import static org.neo4j.io.pagecache.tracing.linear.HEvents.UnmappedFileHEvent;

/**
 * Tracer for global page cache events that add all of them to event history tracer that can build proper linear
 * history across all tracers.
 * Only use this for debugging internal data race bugs and the like, in the page cache.
 * @see HEvents
 * @see LinearHistoryPageCursorTracer
 */
public final class LinearHistoryPageCacheTracer implements PageCacheTracer
{

    private LinearHistoryTracer tracer;

    LinearHistoryPageCacheTracer( LinearHistoryTracer tracer )
    {
        this.tracer = tracer;
    }

    @Override
    public void mappedFile( File file )
    {
        tracer.add( new MappedFileHEvent( file ) );
    }

    @Override
    public void unmappedFile( File file )
    {
        tracer.add( new UnmappedFileHEvent( file ) );
    }

    @Override
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
    {
        return tracer.add( new EvictionRunHEvent( tracer, pageCountToEvict ) );
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        return tracer.add( new MajorFlushHEvent( tracer, swapper.file() ) );
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        return tracer.add( new MajorFlushHEvent( tracer, null ) );
    }

    @Override
    public long faults()
    {
        return 0;
    }

    @Override
    public long evictions()
    {
        return 0;
    }

    @Override
    public long pins()
    {
        return 0;
    }

    @Override
    public long unpins()
    {
        return 0;
    }

    @Override
    public long hits()
    {
        return 0;
    }

    @Override
    public long flushes()
    {
        return 0;
    }

    @Override
    public long bytesRead()
    {
        return 0;
    }

    @Override
    public long bytesWritten()
    {
        return 0;
    }

    @Override
    public long filesMapped()
    {
        return 0;
    }

    @Override
    public long filesUnmapped()
    {
        return 0;
    }

    @Override
    public long evictionExceptions()
    {
        return 0;
    }

    @Override
    public double hitRatio()
    {
        return 0d;
    }

    @Override
    public double usageRatio()
    {
        return 0d;
    }

    @Override
    public void pins( long pins )
    {
    }

    @Override
    public void unpins( long unpins )
    {
    }

    @Override
    public void hits( long hits )
    {
    }

    @Override
    public void faults( long faults )
    {
    }

    @Override
    public void bytesRead( long bytesRead )
    {
    }

    @Override
    public void evictions( long evictions )
    {
    }

    @Override
    public void evictionExceptions( long evictionExceptions )
    {
    }

    @Override
    public void bytesWritten( long bytesWritten )
    {
    }

    @Override
    public void flushes( long flushes )
    {
    }

    @Override
    public void maxPages( long maxPages )
    {
    }
}
