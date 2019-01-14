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
package org.neo4j.io.pagecache.impl.muninn;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;

final class CursorFactory
{
    private final MuninnPagedFile pagedFile;
    private final long victimPage;
    private final PageCursorTracerSupplier pageCursorTracerSupplier;
    private final PageCacheTracer pageCacheTracer;
    private final VersionContextSupplier versionContextSupplier;

    /**
     * Cursor factory construction
     * @param pagedFile paged file for which cursor is created
     * @param pageCursorTracerSupplier supplier of thread local (transaction local) page cursor tracers that will
     * provide thread local page cache statistics
     * @param pageCacheTracer global page cache tracer
     * @param versionContextSupplier version context supplier
     */
    CursorFactory( MuninnPagedFile pagedFile, PageCursorTracerSupplier pageCursorTracerSupplier,
            PageCacheTracer pageCacheTracer, VersionContextSupplier versionContextSupplier )
    {
        this.pagedFile = pagedFile;
        this.victimPage = pagedFile.pageCache.victimPage;
        this.pageCursorTracerSupplier = pageCursorTracerSupplier;
        this.pageCacheTracer = pageCacheTracer;
        this.versionContextSupplier = versionContextSupplier;
    }

    MuninnReadPageCursor takeReadCursor( long pageId, int pf_flags )
    {
        MuninnReadPageCursor cursor = new MuninnReadPageCursor( victimPage, getPageCursorTracer(), versionContextSupplier );
        cursor.initialise( pagedFile, pageId, pf_flags );
        return cursor;
    }

    MuninnWritePageCursor takeWriteCursor( long pageId, int pf_flags )
    {
        MuninnWritePageCursor cursor = new MuninnWritePageCursor( victimPage, getPageCursorTracer(), versionContextSupplier );
        cursor.initialise( pagedFile, pageId, pf_flags );
        return cursor;
    }

    private PageCursorTracer getPageCursorTracer()
    {
        PageCursorTracer pageCursorTracer = pageCursorTracerSupplier.get();
        pageCursorTracer.init( pageCacheTracer );
        return pageCursorTracer;
    }
}
