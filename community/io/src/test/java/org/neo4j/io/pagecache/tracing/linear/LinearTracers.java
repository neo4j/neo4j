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
package org.neo4j.io.pagecache.tracing.linear;

import java.io.PrintStream;
import java.util.function.Consumer;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;

public class LinearTracers
{
    private final LinearHistoryPageCacheTracer pageCacheTracer;
    private final PageCursorTracerSupplier cursorTracerSupplier;
    private final LinearHistoryTracer tracer;

    LinearTracers( LinearHistoryPageCacheTracer pageCacheTracer, PageCursorTracerSupplier cursorTracerSupplier,
            LinearHistoryTracer tracer )
    {
        this.pageCacheTracer = pageCacheTracer;
        this.cursorTracerSupplier = cursorTracerSupplier;
        this.tracer = tracer;
    }

    public LinearHistoryPageCacheTracer getPageCacheTracer()
    {
        return pageCacheTracer;
    }

    public PageCursorTracerSupplier getCursorTracerSupplier()
    {
        return cursorTracerSupplier;
    }

    public void printHistory( PrintStream err )
    {
        tracer.printHistory( err );
    }

    public void processHistory( Consumer<HEvents.HEvent> processor )
    {
        tracer.processHistory( processor );
    }
}
