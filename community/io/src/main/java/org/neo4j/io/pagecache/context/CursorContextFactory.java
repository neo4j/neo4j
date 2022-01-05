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
package org.neo4j.io.pagecache.context;

import java.util.function.LongSupplier;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public class CursorContextFactory
{
    private final PageCacheTracer cacheTracer;
    private final VersionContextSupplier versionContextSupplier;

    public CursorContextFactory( PageCacheTracer cacheTracer, VersionContextSupplier versionContextSupplier )
    {
        this.cacheTracer = cacheTracer;
        this.versionContextSupplier = versionContextSupplier;
    }

    public CursorContext create( String tag )
    {
        return new CursorContext( cacheTracer.createPageCursorTracer( tag ), versionContextSupplier.createVersionContext() );
    }

    public CursorContext create( PageCursorTracer cursorTracer )
    {
        return new CursorContext( cursorTracer, versionContextSupplier.createVersionContext() );
    }

    public void init( LongSupplier lastClosedTransactionIdSupplier )
    {
        versionContextSupplier.init( lastClosedTransactionIdSupplier );
    }
}
