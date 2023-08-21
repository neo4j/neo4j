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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.util.Objects.requireNonNull;
import static org.eclipse.collections.impl.factory.Sets.immutable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;

/**
 * Factory of page cache backed number arrays.
 * @see NumberArrayFactory
 */
public class PageCachedNumberArrayFactory extends NumberArrayFactory.Adapter {
    private final PageCache pageCache;
    private final Path storeDir;
    private final String databaseName;
    private final CursorContextFactory contextFactory;
    private final InternalLog log;

    public PageCachedNumberArrayFactory(
            PageCache pageCache,
            CursorContextFactory contextFactory,
            Path storeDir,
            InternalLog log,
            String databaseName) {
        this.pageCache = requireNonNull(pageCache);
        this.log = log;
        this.contextFactory = requireNonNull(contextFactory);
        this.storeDir = requireNonNull(storeDir);
        this.databaseName = databaseName;
    }

    @Override
    public IntArray newIntArray(long length, int defaultValue, long base, MemoryTracker memoryTracker) {
        try {
            Path tempFile = Files.createTempFile(storeDir, "intArray", ".tmp");
            PagedFile pagedFile =
                    pageCache.map(tempFile, pageCache.pageSize(), databaseName, immutable.of(DELETE_ON_CLOSE, CREATE));
            log.info("Using page-cache backed caching, this may affect performance negatively. IntArray length:"
                    + length);
            return new PageCacheIntArray(pagedFile, contextFactory, length, defaultValue, base);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public LongArray newLongArray(long length, long defaultValue, long base, MemoryTracker memoryTracker) {
        try {
            Path tempFile = Files.createTempFile(storeDir, "longArray", ".tmp");
            PagedFile pagedFile =
                    pageCache.map(tempFile, pageCache.pageSize(), databaseName, immutable.of(DELETE_ON_CLOSE, CREATE));
            log.info("Using page-cache backed caching, this may affect performance negatively. LongArray length:"
                    + length);
            return new PageCacheLongArray(pagedFile, contextFactory, length, defaultValue, base);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ByteArray newByteArray(long length, byte[] defaultValue, long base, MemoryTracker memoryTracker) {
        try {
            Path tempFile = Files.createTempFile(storeDir, "byteArray", ".tmp");
            PagedFile pagedFile =
                    pageCache.map(tempFile, pageCache.pageSize(), databaseName, immutable.of(DELETE_ON_CLOSE, CREATE));
            log.info("Using page-cache backed caching, this may affect performance negatively. ByteArray length:"
                    + length);
            return new PageCacheByteArray(pagedFile, contextFactory, length, defaultValue, base);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
