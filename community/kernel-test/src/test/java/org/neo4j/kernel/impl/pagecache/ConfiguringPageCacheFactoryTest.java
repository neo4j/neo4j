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
package org.neo4j.kernel.impl.pagecache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_store_files;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;

@EphemeralTestDirectoryExtension
class ConfiguringPageCacheFactoryTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    private JobScheduler jobScheduler;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void tearDown() throws Exception {
        jobScheduler.close();
    }

    @Test
    void shouldFitAsManyPagesAsItCan() {
        // Given
        long pageCount = 60;
        long memory = MuninnPageCache.memoryRequiredForPages(pageCount);
        Config config = Config.defaults(pagecache_memory, memory);

        // When
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());

        // Then
        try (PageCache cache = factory.getOrCreatePageCache()) {
            assertThat(cache.pageSize()).isEqualTo(PAGE_SIZE);
            assertThat(cache.maxCachedPages()).isEqualTo(pageCount);
        }
    }

    @Test
    void createPageCacheWithoutPreallocationEnabled() throws IOException {
        Config config = Config.defaults(preallocate_store_files, false);

        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());

        Path testFile = testDirectory.createFile("a");
        try (var cache = factory.getOrCreatePageCache();
                var file = cache.map(testFile, PAGE_SIZE, "foo");
                var io = file.io(1024, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            int bigPageToExpand = 20021;
            assertDoesNotThrow(() -> io.next(bigPageToExpand));
            assertEquals(bigPageToExpand, file.getLastPageId());
        }
    }

    @Test
    void shouldDumpConfigurationWithUnspecifiedPageCacheMemorySetting() {
        // givben
        Config config = Config.defaults();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                logProvider.getLog(ConfiguringPageCacheFactory.class),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());

        // when
        factory.dumpConfiguration();

        // then
        LogAssertions.assertThat(logProvider).containsMessages("Page cache: <not specified>");
    }
}
