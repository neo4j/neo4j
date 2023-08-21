/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport.cache;

import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;

public class PageCacheLongArray extends PageCacheNumberArray<LongArray> implements LongArray {
    private static final String PAGE_CACHE_LONG_ARRAY_WORKER_TAG = "pageCacheLongArrayWorker";

    PageCacheLongArray(
            PagedFile pagedFile, CursorContextFactory contextFactory, long length, long defaultValue, long base)
            throws IOException {
        super(pagedFile, contextFactory, Long.BYTES, length, defaultValue, base);
    }

    @Override
    public long get(long index) {
        long pageId = pageId(index);
        int offset = offset(index);
        try (CursorContext cursorContext = contextFactory.create(PAGE_CACHE_LONG_ARRAY_WORKER_TAG);
                PageCursor cursor = pagedFile.io(pageId, PF_SHARED_READ_LOCK, cursorContext)) {
            cursor.next();
            long result;
            do {
                result = cursor.getLong(offset);
            } while (cursor.shouldRetry());
            checkBounds(cursor);
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void set(long index, long value) {
        long pageId = pageId(index);
        int offset = offset(index);
        try (CursorContext cursorContext = contextFactory.create(PAGE_CACHE_LONG_ARRAY_WORKER_TAG);
                PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW, cursorContext)) {
            cursor.next();
            cursor.putLong(offset, value);
            checkBounds(cursor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
