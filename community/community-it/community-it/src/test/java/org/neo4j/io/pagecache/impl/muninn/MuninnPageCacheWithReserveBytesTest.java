/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.impl.muninn;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class MuninnPageCacheWithReserveBytesTest extends MuninnPageCacheTest {
    @Override
    protected Fixture<MuninnPageCache> createFixture() {
        return super.createFixture().withReservedBytes(Long.BYTES * 3);
    }

    @Test
    void allowOpeningMultipleReadAndSingleWriteCursorsPerThread() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            Path fileA = existingFile("a");
            Path fileB = existingFile("b");

            generateFileWithRecords(fileA, 1, 16, recordsPerFilePage, reservedBytes, filePageSize);
            generateFileWithRecords(fileB, 1, 16, recordsPerFilePage, reservedBytes, filePageSize);

            try (PagedFile pfA = map(fileA, filePageSize);
                    PagedFile pfB = map(fileB, filePageSize);
                    PageCursor a = pfA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor b = pfA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor writerMain = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor e = pfB.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor f = pfB.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(a.next());
                assertTrue(b.next());
                assertTrue(writerMain.next());
                assertTrue(e.next());
                assertTrue(f.next());

                var anotherWriterLockedPage = new AtomicBoolean();
                var anotherThreadWriter = executor.submit(() -> {
                    try (PageCursor lockerWriter = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        lockerWriter.next();
                        anotherWriterLockedPage.set(true);
                    } catch (IOException e1) {
                        throw new UncheckedIOException(e1);
                    }
                });

                MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(1000));
                assertFalse(anotherWriterLockedPage.get());
                writerMain.close();

                anotherThreadWriter.get();
                assertTrue(anotherWriterLockedPage.get());
            }
        });
    }
}
