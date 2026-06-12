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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageSwapperTest;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;

public class SegmentedPageSwapperTest extends PageSwapperTest {
    private static final long PAGES_PER_SEGMENT = 16L;

    @Inject
    private FileSystemAbstraction fs;

    @Override
    protected PageSwapperFactory swapperFactory(FileSystemAbstraction fileSystem) {
        return new SegmentedPageSwapperFactory(
                new SingleFilePageSwapperFactory(fileSystem, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE),
                fileSystem,
                PageCacheTracer.NULL);
    }

    @Override
    protected FileSystemAbstraction getFs() {
        return fs;
    }

    @Override
    protected long pagesPerSegment() {
        return PAGES_PER_SEGMENT;
    }

    @Test
    void segmentForPage() throws IOException {
        Path file = file("a");
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        try (var swapper = (SegmentedPageSwapper) createSwapperAndFile(swapperFactory, file)) {
            assertEquals(0, swapper.segmentIndexFor(0));
            assertEquals(0, swapper.segmentIndexFor(1));
            assertEquals(0, swapper.segmentIndexFor(15));
            assertEquals(1, swapper.segmentIndexFor(16));
            assertEquals(1, swapper.segmentIndexFor(30));
            assertEquals(1, swapper.segmentIndexFor(31));
            assertEquals(2, swapper.segmentIndexFor(32));
            assertEquals(10, swapper.segmentIndexFor(160));
        }
    }

    @Test
    void pageWithinSegment() throws IOException {
        Path file = file("a");
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        try (var swapper = (SegmentedPageSwapper) createSwapperAndFile(swapperFactory, file)) {
            assertEquals(0, swapper.pageWithinSegment(0));
            assertEquals(1, swapper.pageWithinSegment(1));
            assertEquals(15, swapper.pageWithinSegment(15));
            assertEquals(0, swapper.pageWithinSegment(16));
            assertEquals(14, swapper.pageWithinSegment(30));
            assertEquals(15, swapper.pageWithinSegment(31));
            assertEquals(0, swapper.pageWithinSegment(32));
            assertEquals(1, swapper.pageWithinSegment(161));
        }
    }

    @Test
    void bytesLeftInSegment() throws IOException {
        Path file = file("a");
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        try (var swapper = (SegmentedPageSwapper) createSwapperAndFile(swapperFactory, file)) {
            assertEquals(16L * cachePageSize, swapper.bytesLeftInSegment(0));
            assertEquals(15L * cachePageSize, swapper.bytesLeftInSegment(1));
            assertEquals(cachePageSize, swapper.bytesLeftInSegment(15));
            assertEquals(16L * cachePageSize, swapper.bytesLeftInSegment(16));
            assertEquals(2L * cachePageSize, swapper.bytesLeftInSegment(30));
            assertEquals(cachePageSize, swapper.bytesLeftInSegment(31));
            assertEquals(16L * cachePageSize, swapper.bytesLeftInSegment(32));
            assertEquals(15L * cachePageSize, swapper.bytesLeftInSegment(161));
        }
    }
}
