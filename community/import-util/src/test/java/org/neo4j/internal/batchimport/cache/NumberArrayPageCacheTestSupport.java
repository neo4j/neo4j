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

import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

public class NumberArrayPageCacheTestSupport {
    static Fixture prepareDirectoryAndPageCache(Class<?> testClass) throws IOException {
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        TestDirectory testDirectory = TestDirectory.testDirectory(testClass, fileSystem);
        Path dir = testDirectory.prepareDirectoryForTest("test");
        ThreadPoolJobScheduler scheduler = new ThreadPoolJobScheduler();
        var contextFactory = new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);
        PageCache pageCache =
                StandalonePageCacheFactory.createPageCache(fileSystem, scheduler, PageCacheTracer.NULL, config(1024));
        return new Fixture(pageCache, fileSystem, dir, scheduler, contextFactory);
    }

    public static class Fixture implements AutoCloseable {
        public final PageCache pageCache;
        public final FileSystemAbstraction fileSystem;
        public final CursorContextFactory contextFactory;
        public final Path directory;
        private final ThreadPoolJobScheduler scheduler;

        private Fixture(
                PageCache pageCache,
                FileSystemAbstraction fileSystem,
                Path directory,
                ThreadPoolJobScheduler scheduler,
                CursorContextFactory contextFactory) {
            this.pageCache = pageCache;
            this.fileSystem = fileSystem;
            this.directory = directory;
            this.scheduler = scheduler;
            this.contextFactory = contextFactory;
        }

        @Override
        public void close() throws Exception {
            pageCache.close();
            scheduler.close();
            fileSystem.close();
        }
    }
}
