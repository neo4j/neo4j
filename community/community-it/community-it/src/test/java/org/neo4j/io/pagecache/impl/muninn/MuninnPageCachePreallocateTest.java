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
package org.neo4j.io.pagecache.impl.muninn;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@Neo4jLayoutExtension
class MuninnPageCacheExplicitPreallocateTest {

    private static final int PAGE_CACHE_CHUNK_SIZE = 4 * 1024;
    private JobScheduler jobScheduler;
    private final PageSwapper swapper = mock(PageSwapper.class);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private Neo4jLayout neo4jLayout;

    @BeforeEach
    void beforeEach() {
        jobScheduler = new ThreadPoolJobScheduler();
        when(swapper.canAllocate()).thenReturn(true);
    }

    @AfterEach
    void afterEach() throws Exception {
        jobScheduler.close();
    }

    // This test checks that explicit pre-allocation is not influenced by automatic pre-allocation configuration
    // and depends only on underlying swapper capabilities.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPreAllocationSupport(boolean automaticPreAllocation) throws IOException {
        try (var pageCache = createPageCache(automaticPreAllocation)) {
            try (var pageFile = mapFile(pageCache)) {
                assertTrue(pageFile.preAllocateSupported());

                when(swapper.canAllocate()).thenReturn(false);

                assertFalse(pageFile.preAllocateSupported());
            }
        }
    }

    // The first 32MBs of a file are not a subject of automatic pre-allocation, so pre-allocation in that range
    // should work the same regardless if automatic pre-allocation is enabled or not.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSmallPreAllocation(boolean automaticPreAllocation) throws IOException {
        try (var pageCache = createPageCache(automaticPreAllocation)) {
            try (var pageFile = mapFile(pageCache)) {
                pageFile.preAllocate(2);
                pageFile.preAllocate(3);
                pageFile.preAllocate(7);

                verify(swapper).allocate(pageFile.pageSize() * 2L);
                verify(swapper).allocate(pageFile.pageSize() * 3L);
                verify(swapper).allocate(pageFile.pageSize() * 7L);
            }
        }
    }

    @Test
    void testLargeExplicitPreAllocationWithAutomaticPreAllocation() throws IOException {
        try (var pageCache = createPageCache(true)) {
            try (var pageFile = mapFile(pageCache)) {
                // Let's aim for the second chunk
                pageFile.preAllocate(PAGE_CACHE_CHUNK_SIZE + 123);
                // Targeting already pre-allocated chunk should do nothing
                pageFile.preAllocate(PAGE_CACHE_CHUNK_SIZE + 1234);
                // Let's poke the third chunk
                pageFile.preAllocate(2 * PAGE_CACHE_CHUNK_SIZE + 7);

                verify(swapper).allocate(pageFile.pageSize() * (2 * PAGE_CACHE_CHUNK_SIZE));
                verify(swapper).allocate(pageFile.pageSize() * (3 * PAGE_CACHE_CHUNK_SIZE));
            }
        }
    }

    @Test
    void testLargeExplicitPreAllocationWithoutAutomaticPreAllocation() throws IOException {
        try (var pageCache = createPageCache(false)) {
            try (var pageFile = mapFile(pageCache)) {
                pageFile.preAllocate(PAGE_CACHE_CHUNK_SIZE + 123);
                pageFile.preAllocate(PAGE_CACHE_CHUNK_SIZE + 1234);

                verify(swapper).allocate(pageFile.pageSize() * (PAGE_CACHE_CHUNK_SIZE + 123L));
                verify(swapper).allocate(pageFile.pageSize() * (PAGE_CACHE_CHUNK_SIZE + 1234L));
            }
        }
    }

    private PagedFile mapFile(PageCache pageCache) throws IOException {
        return pageCache.map(
                neo4jLayout.databasesDirectory().resolve("a"), pageCache.pageSize(), DEFAULT_DATABASE_NAME);
    }

    private MuninnPageCache createPageCache(boolean automaticPreAllocation) {
        long memory = MuninnPageCache.memoryRequiredForPages(1024);
        var memoryTracker = new LocalMemoryTracker();
        var allocator = MemoryAllocator.createAllocator(memory, memoryTracker);
        MuninnPageCache.Configuration configuration =
                MuninnPageCache.config(allocator).preallocateStoreFiles(automaticPreAllocation);
        PageSwapperFactory pageSwapperFactory =
                (path,
                        filePageSize,
                        onEviction,
                        createIfNotExist,
                        useDirectIO,
                        ioController,
                        evictionGuard,
                        swappers) -> {
                    when(swapper.swapperId()).thenReturn(swappers.allocate(swapper));
                    when(swapper.path()).thenReturn(path);
                    return swapper;
                };
        return new MuninnPageCache(pageSwapperFactory, jobScheduler, configuration);
    }
}
