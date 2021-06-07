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
package org.neo4j.kernel.impl.store.cursor;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.EmptyVersionContext;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.cursor.CursorTypes;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.context.EmptyVersionContext.EMPTY;

@DbmsExtension
class CachedStoreCursorsIT
{
    @Inject
    private RecordStorageEngine storageEngine;

    @Test
    void cacheRequestedCursors()
    {
        DefaultPageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
        PageCursorTracer cursorTracer = pageCacheTracer.createPageCursorTracer( "cacheRequestedCursors" );
        try ( var cursorContext = new CursorContext( cursorTracer, EMPTY );
              var storageCursors = storageEngine.createStorageCursors( cursorContext ) )
        {
            Object[] cursors = new Object[CursorTypes.MAX_TYPE + 1];
            for ( short i = 0; i <= CursorTypes.MAX_TYPE; i++ )
            {
                cursors[i] = storageCursors.pageCursor( i );
            }

            for ( int i = 0; i < 10; i++ )
            {
                for ( short j = 0; j <= CursorTypes.MAX_TYPE; j++ )
                {
                    assertEquals( cursors[j], storageCursors.pageCursor( j ) );
                }
            }
        }
    }
}
