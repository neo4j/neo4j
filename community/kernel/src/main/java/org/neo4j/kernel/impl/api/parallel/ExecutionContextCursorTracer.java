/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api.parallel;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;

public class ExecutionContextCursorTracer extends DefaultPageCursorTracer
{
    private long pins;
    private long unpins;
    private long hits;
    private long faults;
    private long bytesRead;
    private long bytesWritten;
    private long evictions;
    private long evictionExceptions;
    private long flushes;
    private long merges;
    private volatile boolean completed;

    ExecutionContextCursorTracer( PageCacheTracer pageCacheTracer, String tag )
    {
        super( pageCacheTracer, tag );
    }

    // We override report events here since we want to capture all the events accumulated in the tracer and another thread and make
    // then available to consumer thread. That in ensued by waiting for completed flag by consumer thread.
    @Override
    public void reportEvents()
    {
        pins = super.pins();
        unpins = super.unpins();
        hits = super.hits();
        faults = super.faults();
        bytesRead = super.bytesRead();
        bytesWritten = super.bytesWritten();
        evictions = super.evictions();
        evictionExceptions = super.evictionExceptions();
        flushes = super.flushes();
        merges = super.merges();
        completed = true;
    }

    public boolean isCompleted()
    {
        return completed;
    }

    @Override
    public long faults()
    {
        return faults;
    }

    @Override
    public long pins()
    {
        return pins;
    }

    @Override
    public long unpins()
    {
        return unpins;
    }

    @Override
    public long hits()
    {
        return hits;
    }

    @Override
    public long bytesRead()
    {
        return bytesRead;
    }

    @Override
    public long evictions()
    {
        return evictions;
    }

    @Override
    public long evictionExceptions()
    {
        return evictionExceptions;
    }

    @Override
    public long bytesWritten()
    {
        return bytesWritten;
    }

    @Override
    public long flushes()
    {
        return flushes;
    }

    @Override
    public long merges()
    {
        return merges;
    }
}
