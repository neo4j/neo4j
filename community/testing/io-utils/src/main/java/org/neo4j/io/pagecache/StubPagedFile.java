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
package org.neo4j.io.pagecache;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.monitoring.PageFileCounters;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageFileSwapperTracer;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;

public class StubPagedFile implements PagedFile {
    private final int pageSize;
    public final int exposedPageSize;
    private final int reservedBytes;
    public long lastPageId = 1;

    public StubPagedFile(int pageSize, int reservedBytes) {
        this.pageSize = pageSize;
        this.exposedPageSize = pageSize;
        this.reservedBytes = reservedBytes;
    }

    @Override
    public PageCursor io(long pageId, int pf_flags, CursorContext context) throws IOException {
        StubPageCursor cursor = new StubPageCursor(
                pageId, ByteBuffers.allocate(pageSize, ByteOrder.LITTLE_ENDIAN, INSTANCE), reservedBytes);
        prepareCursor(cursor);
        return cursor;
    }

    protected void prepareCursor(StubPageCursor cursor) {}

    @Override
    public int pageSize() {
        return exposedPageSize;
    }

    @Override
    public int payloadSize() {
        return pageSize - reservedBytes;
    }

    @Override
    public int pageReservedBytes() {
        return reservedBytes;
    }

    @Override
    public long fileSize() {
        if (lastPageId < 0) {
            return 0L;
        }
        return (lastPageId + 1) * pageSize();
    }

    @Override
    public Path path() {
        return Path.of("stub");
    }

    @Override
    public void flushAndForce(FileFlushEvent flushEvent) {}

    @Override
    public long getLastPageId() {
        return lastPageId;
    }

    @Override
    public void increaseLastPageIdTo(long newLastPageId) {}

    @Override
    public void close() {}

    @Override
    public void setDeleteOnClose(boolean deleteOnClose) {}

    @Override
    public boolean isDeleteOnClose() {
        return false;
    }

    @Override
    public String getDatabaseName() {
        return "stub";
    }

    @Override
    public PageFileCounters pageFileCounters() {
        return PageFileSwapperTracer.NULL;
    }

    @Override
    public boolean isMultiVersioned() {
        return false;
    }

    @Override
    public void truncate(long pagesToKeep, FileTruncateEvent truncateEvent) throws IOException {}

    @Override
    public int touch(long pageId, int count, CursorContext cursorContext) {
        return 0;
    }

    @Override
    public boolean preAllocateSupported() {
        return false;
    }

    @Override
    public void preAllocate(long newFileSizeInPages) {}
}
