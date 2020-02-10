/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checking.index;

import java.util.Iterator;

import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexAccessor;

public class IndexIterator implements BoundedIterable<Long>
{
    private static final String CONSISTENCY_INDEX_ITERATOR_TAG = "consistencyIndexIterator";
    private static final String CONSISTENCY_INDEX_COUNTER_TAG = "consistencyIndexCounter";
    private final IndexAccessor indexAccessor;
    private final PageCacheTracer pageCacheTracer;
    private BoundedIterable<Long> indexReader;
    private PageCursorTracer readerTracer;

    public IndexIterator( IndexAccessor indexAccessor, PageCacheTracer pageCacheTracer )
    {
        this.indexAccessor = indexAccessor;
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    public long maxCount()
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( CONSISTENCY_INDEX_COUNTER_TAG );
              var reader = indexAccessor.newAllEntriesReader( cursorTracer ) )
        {
            return reader.maxCount();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void close() throws Exception
    {
        if ( indexReader != null )
        {
            indexReader.close();
            readerTracer.close();
        }
    }

    @Override
    public Iterator<Long> iterator()
    {
        if ( indexReader == null )
        {
            readerTracer = pageCacheTracer.createPageCursorTracer( CONSISTENCY_INDEX_ITERATOR_TAG );
            indexReader = indexAccessor.newAllEntriesReader( readerTracer );
        }

        return indexReader.iterator();
    }
}
