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

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * Tracer for page cache cursor events that add all of them to event history tracer that can build proper linear
 * history across all tracers.
 * Only use this for debugging internal data race bugs and the like, in the page cache.
 *
 * @see HEvents
 * @see LinearHistoryPageCacheTracer
 */
public class LinearHistoryPageCursorTracer implements PageCursorTracer
{
    private LinearHistoryTracer tracer;

    LinearHistoryPageCursorTracer( LinearHistoryTracer tracer )
    {
        this.tracer = tracer;
    }

    @Override
    public long faults()
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
    public long bytesRead()
    {
        return 0;
    }

    @Override
    public long evictions()
    {
        return 0;
    }

    @Override
    public long evictionExceptions()
    {
        return 0;
    }

    @Override
    public long bytesWritten()
    {
        return 0;
    }

    @Override
    public long flushes()
    {
        return 0;
    }

    @Override
    public double hitRatio()
    {
        return 0d;
    }

    @Override
    public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
    {
        return tracer.add( new HEvents.PinHEvent( tracer, writeLock, filePageId, swapper ) );
    }

    @Override
    public void init( PageCacheTracer tracer )
    {
        // nothing to do
    }

    @Override
    public void reportEvents()
    {
        // nothing to do
    }

    @Override
    public long accumulatedHits()
    {
        return 0;
    }

    @Override
    public long accumulatedFaults()
    {
        return 0;
    }
}
