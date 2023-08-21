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

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.monitoring.PageFileCounters;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;

public class DelegatingPagedFile implements PagedFile {
    protected final PagedFile delegate;

    public DelegatingPagedFile(PagedFile delegate) {
        this.delegate = delegate;
    }

    @Override
    public PageCursor io(long pageId, int pf_flags, CursorContext context) throws IOException {
        return delegate.io(pageId, pf_flags, context);
    }

    @Override
    public void flushAndForce(FileFlushEvent flushEvent) throws IOException {
        delegate.flushAndForce(flushEvent);
    }

    @Override
    public long getLastPageId() throws IOException {
        return delegate.getLastPageId();
    }

    @Override
    public void increaseLastPageIdTo(long newLastPageId) {
        delegate.increaseLastPageIdTo(newLastPageId);
    }

    @Override
    public int pageSize() {
        return delegate.pageSize();
    }

    @Override
    public int payloadSize() {
        return delegate.payloadSize();
    }

    @Override
    public int pageReservedBytes() {
        return delegate.pageReservedBytes();
    }

    @Override
    public long fileSize() throws IOException {
        return delegate.fileSize();
    }

    @Override
    public Path path() {
        return delegate.path();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void setDeleteOnClose(boolean deleteOnClose) {
        delegate.setDeleteOnClose(deleteOnClose);
    }

    @Override
    public boolean isDeleteOnClose() {
        return delegate.isDeleteOnClose();
    }

    @Override
    public String getDatabaseName() {
        return delegate.getDatabaseName();
    }

    @Override
    public PageFileCounters pageFileCounters() {
        return delegate.pageFileCounters();
    }

    @Override
    public boolean isMultiVersioned() {
        return delegate.isMultiVersioned();
    }

    @Override
    public void truncate(long pagesToKeep, FileTruncateEvent truncateEvent) throws IOException {
        delegate.truncate(pagesToKeep, truncateEvent);
    }

    @Override
    public int touch(long pageId, int count, CursorContext cursorContext) throws IOException {
        return delegate.touch(pageId, count, cursorContext);
    }

    @Override
    public boolean preAllocateSupported() {
        return delegate.preAllocateSupported();
    }

    @Override
    public void preAllocate(long newFileSizeInPages) throws IOException {
        delegate.preAllocate(newFileSizeInPages);
    }
}
