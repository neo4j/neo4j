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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.neo4j.internal.nativeimpl.NativeAccessFactory;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.impl.muninn.EvictionBouncer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * A factory for SingleFilePageSwapper instances.
 *
 * @see SingleFilePageSwapper
 */
public class SingleFilePageSwapperFactory implements PageSwapperFactory {
    private final FileSystemAbstraction fs;
    private final PageCacheTracer pageCacheTracer;
    private final BlockSwapper blockSwapper;
    private final boolean doForceOperations;

    public SingleFilePageSwapperFactory(
            FileSystemAbstraction fs, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker) {
        this(fs, pageCacheTracer, memoryTracker, true);
    }

    public SingleFilePageSwapperFactory(
            FileSystemAbstraction fs,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker,
            boolean doForceOperations) {
        this.fs = fs;
        this.pageCacheTracer = pageCacheTracer;
        this.blockSwapper = createBlockSwapper(memoryTracker);
        this.doForceOperations = doForceOperations;
    }

    @Override
    public PageSwapper createPageSwapper(
            Path file,
            int filePageSize,
            PageEvictionCallback onEviction,
            boolean createIfNotExist,
            boolean useDirectIO,
            long pagesPerSegment,
            IOController ioController,
            EvictionBouncer evictionBouncer,
            SwapperIdProvider swapperIdProvider)
            throws IOException {
        if (!createIfNotExist && !fs.fileExists(file)) {
            throw new NoSuchFileException(file.toString(), null, "Cannot map non-existing file");
        }
        return new SingleFilePageSwapper(
                file,
                fs,
                filePageSize,
                onEviction,
                useDirectIO,
                ioController,
                swapperIdProvider,
                pageCacheTracer.createFileSwapperTracer(),
                blockSwapper,
                nativeAccessFactory(),
                evictionBouncer,
                doForceOperations);
    }

    private static BlockSwapper createBlockSwapper(MemoryTracker memoryTracker) {
        if (UnsafeUtil.unsafeByteBufferAccessAvailable()) {
            return new UnsafeBlockSwapper();
        }
        return new FallbackBlockSwapper(memoryTracker);
    }

    @VisibleForTesting
    protected NativeAccessFactory nativeAccessFactory() {
        return file -> NativeAccessProvider.getNativeAccess();
    }
}
