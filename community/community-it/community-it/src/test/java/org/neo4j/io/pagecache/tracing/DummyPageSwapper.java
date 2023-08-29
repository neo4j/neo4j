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
package org.neo4j.io.pagecache.tracing;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.pagecache.PageSwapper;

public class DummyPageSwapper implements PageSwapper {
    protected final String filename;
    protected final int filePageSize;
    protected final PageFileSwapperTracer tracer;

    public DummyPageSwapper(String filename, int filePageSize) {
        this(filename, filePageSize, PageFileSwapperTracer.NULL);
    }

    public DummyPageSwapper(String filename, int filePageSize, PageFileSwapperTracer tracer) {
        this.filename = filename;
        this.filePageSize = filePageSize;
        this.tracer = tracer;
    }

    @Override
    public long read(long filePageId, long bufferAddress) throws IOException {
        return filePageSize;
    }

    @Override
    public long read(long filePageId, long bufferAddress, int bufferLength) throws IOException {
        return bufferLength;
    }

    @Override
    public long write(long filePageId, long bufferAddress) throws IOException {
        return filePageSize;
    }

    @Override
    public long write(long filePageId, long bufferAddress, int bufferLength) {
        return bufferAddress;
    }

    @Override
    public void evicted(long filePageId) {}

    @Override
    public Path path() {
        return Path.of(filename);
    }

    @Override
    public void close() {}

    @Override
    public void force() {}

    @Override
    public long getLastPageId() {
        return 0;
    }

    @Override
    public void truncate() {}

    @Override
    public void truncate(long size) {}

    @Override
    public boolean canAllocate() {
        return false;
    }

    @Override
    public void allocate(long newFileSize) {}

    @Override
    public int swapperId() {
        return 42;
    }

    @Override
    public PageFileSwapperTracer fileSwapperTracer() {
        return tracer;
    }

    @Override
    public boolean isPageFlushable(long pageRef) {
        return true;
    }

    @Override
    public void closeAndDelete() {}

    @Override
    public long read(long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length) {
        return 0;
    }

    @Override
    public long write(
            long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length, int totalAffectedPages) {
        return 0;
    }
}
