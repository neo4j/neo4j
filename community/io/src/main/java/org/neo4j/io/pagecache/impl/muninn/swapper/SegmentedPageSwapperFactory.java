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
package org.neo4j.io.pagecache.impl.muninn.swapper;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.impl.muninn.EvictionBouncer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

public class SegmentedPageSwapperFactory implements PageSwapperFactory {
    private final PageSwapperFactory delegate;
    private final FileSystemAbstraction fs;
    private final PageCacheTracer pageCacheTracer;

    public SegmentedPageSwapperFactory(
            PageSwapperFactory delegate, FileSystemAbstraction fs, PageCacheTracer pageCacheTracer) {
        this.delegate = delegate;
        this.fs = fs;
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    public PageSwapper createPageSwapper(
            Path path,
            int filePageSize,
            PageEvictionCallback onEviction,
            boolean createIfNotExist,
            boolean useDirectIO,
            long pagesPerSegment,
            IOController ioController,
            EvictionBouncer evictionBouncer,
            SwapperIdProvider swapperIdProvider)
            throws IOException {
        if (pagesPerSegment == 0) {
            return delegate.createPageSwapper(
                    path,
                    filePageSize,
                    onEviction,
                    createIfNotExist,
                    useDirectIO,
                    pagesPerSegment,
                    ioController,
                    evictionBouncer,
                    swapperIdProvider);
        }
        return new SegmentedPageSwapper(
                path,
                filePageSize,
                pagesPerSegment,
                onEviction,
                createIfNotExist,
                useDirectIO,
                ioController,
                evictionBouncer,
                swapperIdProvider,
                delegate,
                fs,
                pageCacheTracer.createFileSwapperTracer(),
                pageCacheTracer);
    }
}
