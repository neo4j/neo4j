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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class ConfigurableStandalonePageCacheFactoryTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void mustAutomaticallyStartEvictionThread() throws Exception {
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                JobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
            Path file = testDirectory.homePath().resolve("a").normalize();
            fs.write(file).close();

            try (PageCache cache = ConfigurableStandalonePageCacheFactory.createPageCache(
                            fs, jobScheduler, PageCacheTracer.NULL);
                    PagedFile pf = cache.map(file, 4096, DEFAULT_DATABASE_NAME);
                    PageCursor cursor = pf.io(0, PagedFile.PF_SHARED_WRITE_LOCK, CursorContext.NULL_CONTEXT)) {
                // The default size is currently 8MBs.
                // It should be possible to write more than that.
                // If the eviction thread has not been started, then this test will block forever.
                for (int i = 0; i < 10_000; i++) {
                    assertTrue(cursor.next());
                    cursor.putInt(42);
                }
            }
        }
    }
}
