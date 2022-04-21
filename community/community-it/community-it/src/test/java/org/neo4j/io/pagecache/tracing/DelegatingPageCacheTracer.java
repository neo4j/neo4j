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
package org.neo4j.io.pagecache.tracing;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * A PageCacheTracer that delegates all calls to a wrapped instance.
 *
 * Useful for overriding specific functionality in a sub-class.
 */
public class DelegatingPageCacheTracer implements PageCacheTracer {
    private final PageCacheTracer delegate;

    public DelegatingPageCacheTracer(PageCacheTracer delegate) {
        this.delegate = delegate;
    }

    @Override
    public PageFileSwapperTracer createFileSwapperTracer() {
        return delegate.createFileSwapperTracer();
    }

    @Override
    public PageCursorTracer createPageCursorTracer(String tag) {
        return delegate.createPageCursorTracer(tag);
    }

    @Override
    public void mappedFile(int swapperId, PagedFile pagedFile) {
        delegate.mappedFile(swapperId, pagedFile);
    }

    @Override
    public long bytesRead() {
        return delegate.bytesRead();
    }

    @Override
    public MajorFlushEvent beginFileFlush(PageSwapper swapper) {
        return delegate.beginFileFlush(swapper);
    }

    @Override
    public EvictionRunEvent beginPageEvictions(int pageCountToEvict) {
        return delegate.beginPageEvictions(pageCountToEvict);
    }

    @Override
    public EvictionRunEvent beginEviction() {
        return delegate.beginEviction();
    }

    @Override
    public long unpins() {
        return delegate.unpins();
    }

    @Override
    public long hits() {
        return delegate.hits();
    }

    @Override
    public MajorFlushEvent beginCacheFlush() {
        return delegate.beginCacheFlush();
    }

    @Override
    public long bytesWritten() {
        return delegate.bytesWritten();
    }

    @Override
    public long pins() {
        return delegate.pins();
    }

    @Override
    public long filesUnmapped() {
        return delegate.filesUnmapped();
    }

    @Override
    public void unmappedFile(int swapperId, PagedFile pagedFile) {
        delegate.unmappedFile(swapperId, pagedFile);
    }

    @Override
    public long evictionExceptions() {
        return delegate.evictionExceptions();
    }

    @Override
    public double hitRatio() {
        return delegate.hitRatio();
    }

    @Override
    public double usageRatio() {
        return delegate.usageRatio();
    }

    @Override
    public long iopqPerformed() {
        return delegate.iopqPerformed();
    }

    @Override
    public long ioLimitedTimes() {
        return delegate.ioLimitedTimes();
    }

    @Override
    public long ioLimitedMillis() {
        return delegate.ioLimitedMillis();
    }

    @Override
    public long openedCursors() {
        return delegate.openedCursors();
    }

    @Override
    public long closedCursors() {
        return delegate.closedCursors();
    }

    @Override
    public void pins(long pins) {
        delegate.pins(pins);
    }

    @Override
    public void unpins(long unpins) {
        delegate.unpins(unpins);
    }

    @Override
    public void hits(long hits) {
        delegate.hits(hits);
    }

    @Override
    public void faults(long faults) {
        delegate.faults(faults);
    }

    @Override
    public void noFaults(long noFaults) {
        delegate.noFaults(noFaults);
    }

    @Override
    public void failedFaults(long failedFaults) {
        delegate.failedFaults(failedFaults);
    }

    @Override
    public void bytesRead(long bytesRead) {
        delegate.bytesRead(bytesRead);
    }

    @Override
    public void evictions(long evictions) {
        delegate.evictions(evictions);
    }

    @Override
    public void cooperativeEvictions(long evictions) {
        delegate.cooperativeEvictions(evictions);
    }

    @Override
    public void evictionExceptions(long evictionExceptions) {
        delegate.evictionExceptions(evictionExceptions);
    }

    @Override
    public void bytesWritten(long bytesWritten) {
        delegate.bytesWritten(bytesWritten);
    }

    @Override
    public void flushes(long flushes) {
        delegate.flushes(flushes);
    }

    @Override
    public void merges(long merges) {
        delegate.merges(merges);
    }

    @Override
    public void maxPages(long maxPages, long pageSize) {
        delegate.maxPages(maxPages, pageSize);
    }

    @Override
    public void iopq(long iopq) {
        delegate.iopq(iopq);
    }

    @Override
    public void limitIO(long millis) {
        delegate.limitIO(millis);
    }

    @Override
    public void closeCursor() {
        delegate.closeCursor();
    }

    @Override
    public void openCursor() {
        delegate.openCursor();
    }

    @Override
    public long filesMapped() {
        return delegate.filesMapped();
    }

    @Override
    public long flushes() {
        return delegate.flushes();
    }

    @Override
    public long merges() {
        return delegate.merges();
    }

    @Override
    public long faults() {
        return delegate.faults();
    }

    @Override
    public long failedFaults() {
        return delegate.failedFaults();
    }

    @Override
    public long noFaults() {
        return delegate.noFaults();
    }

    @Override
    public long evictions() {
        return delegate.evictions();
    }

    @Override
    public long cooperativeEvictions() {
        return delegate.cooperativeEvictions();
    }
}
