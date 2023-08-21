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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

class MunninPageCacheCloseIT {
    private abstract static class TestBase {
        @Inject
        private PageCache pageCache;

        @Inject
        private FileSystemAbstraction fs;

        @Inject
        private TestDirectory directory;

        @Test
        void shouldBeAbleToShutDownWhenInterrupted() throws Exception {
            Path file = directory.file("file");
            try (StoreChannel channel = fs.write(file)) {
                channel.writeAll(ByteBuffer.wrap(new byte[100]));
            }

            AtomicBoolean success = new AtomicBoolean(false);
            Thread thread = new Thread(
                    () -> {
                        try (PagedFile pagedFile = pageCache.map(file, 10, DEFAULT_DATABASE_NAME)) {
                            // Write something
                            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                                if (cursor.next()) {
                                    cursor.putByte((byte) 6);
                                }
                            }
                            Thread.currentThread().interrupt(); // simulate an unexpected interruption
                            // then try to close the page
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }

                        success.set(true);
                    },
                    "MunninPageCacheCloseIT writeInterruptionThread");

            try {
                thread.start();
                thread.join(MINUTES.toMillis(1));
                assertTrue(
                        success.get()); // if this fails, then the thread is still alive, trying to close the paged file
            } finally {
                if (thread.isAlive()) {
                    // There is not much we can do here really except wait abit longer and hope it terminates.
                    thread.join(MINUTES.toMillis(1));
                    // if still alive, the thread will leak.
                }
            }
        }
    }

    @EphemeralPageCacheExtension
    @Nested
    class MunninPageCacheCloseWithEphemeralFileSystemIT extends TestBase {}

    @PageCacheExtension
    @Nested
    class MunninPageCacheCloseWithRealFileSystemIT extends TestBase {}
}
