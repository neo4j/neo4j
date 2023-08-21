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

import static java.lang.Math.toIntExact;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;

/**
 * Abstraction over page cache backed number arrays.
 *
 * @see PageCachedNumberArrayFactory
 */
public abstract class PageCacheNumberArray<N extends NumberArray<N>> implements NumberArray<N> {
    private static final String PAGE_CACHE_WORKER_TAG = "pageCacheNumberArrayWorker";
    protected final PagedFile pagedFile;
    protected final int entriesPerPage;
    protected final int entrySize;
    protected final CursorContextFactory contextFactory;
    private final long length;
    private final long defaultValue;
    private final long base;
    private boolean closed;

    PageCacheNumberArray(
            PagedFile pagedFile, CursorContextFactory contextFactory, int entrySize, long length, long base)
            throws IOException {
        this(pagedFile, contextFactory, entrySize, length, 0, base);
    }

    PageCacheNumberArray(
            PagedFile pagedFile,
            CursorContextFactory contextFactory,
            int entrySize,
            long length,
            long defaultValue,
            long base)
            throws IOException {
        this.pagedFile = pagedFile;
        this.contextFactory = contextFactory;
        this.entrySize = entrySize;
        this.entriesPerPage = pagedFile.payloadSize() / entrySize;
        this.length = length;
        this.defaultValue = defaultValue;
        this.base = base;

        try (CursorContext cursorContext = contextFactory.create(PAGE_CACHE_WORKER_TAG);
                PageCursor cursorToSetLength = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
            setLength(cursorToSetLength, length);
        }

        if (defaultValue != 0) {
            setDefaultValue(defaultValue);
        }
    }

    private void setLength(PageCursor cursor, long length) throws IOException {
        if (!cursor.next((length - 1) / entriesPerPage)) {
            throw new IllegalStateException(
                    String.format("Unable to extend the backing file %s to desired size %d.", pagedFile, length));
        }
    }

    protected long pageId(long index) {
        return rebase(index) / entriesPerPage;
    }

    protected int offset(long index) {
        return toIntExact(rebase(index) % entriesPerPage * entrySize);
    }

    private long rebase(long index) {
        return index - base;
    }

    protected void setDefaultValue(long defaultValue) throws IOException {
        try (CursorContext cursorContext = contextFactory.create(PAGE_CACHE_WORKER_TAG);
                PageCursor writeCursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, cursorContext)) {
            writeCursor.next();
            int payloadSize = pagedFile.payloadSize();
            fillPageWithDefaultValue(writeCursor, defaultValue, payloadSize);
            if (pageId(length - 1) > 0) {
                try (PageCursor cursor = pagedFile.io(1, PF_NO_GROW | PF_SHARED_WRITE_LOCK, cursorContext)) {
                    while (cursor.next()) {
                        writeCursor.copyTo(0, cursor, 0, payloadSize);
                        checkBounds(writeCursor);
                    }
                }
            }
        }
    }

    protected void fillPageWithDefaultValue(PageCursor writeCursor, long defaultValue, int payloadSize) {
        int longsInPage = payloadSize / Long.BYTES;
        for (int i = 0; i < longsInPage; i++) {
            writeCursor.putLong(defaultValue);
        }
        checkBounds(writeCursor);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public void clear() {
        try {
            setDefaultValue(defaultValue);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        pagedFile.close();
        closed = true;
    }

    @Override
    public N at(long index) {
        return (N) this;
    }

    @Override
    public void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor) {
        visitor.offHeapUsage(length() * entrySize);
    }

    protected static void checkBounds(PageCursor cursor) {
        if (cursor.checkAndClearBoundsFlag()) {
            throw new IllegalStateException(String.format(
                    "Cursor %s access out of bounds, page id %d, offset %d",
                    cursor, cursor.getCurrentPageId(), cursor.getOffset()));
        }
    }
}
