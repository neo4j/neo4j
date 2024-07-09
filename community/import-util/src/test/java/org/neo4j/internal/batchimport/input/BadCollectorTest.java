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
package org.neo4j.internal.batchimport.input;

import static java.io.OutputStream.nullOutputStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.batchimport.input.BadCollector.COLLECT_ALL;
import static org.neo4j.internal.batchimport.input.BadCollector.UNLIMITED_TOLERANCE;
import static org.neo4j.test.OtherThreadExecutor.command;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

@ExtendWith(EphemeralFileSystemExtension.class)
class BadCollectorTest {
    @Inject
    private FileSystemAbstraction fs;

    private final Groups groups = new Groups();
    private final Group group = groups.getOrCreate(null);

    @Test
    void shouldCollectBadRelationshipsEvenIfThresholdNeverReached() throws IOException {
        // given
        int tolerance = 5;

        var groupA = groups.getOrCreate("a");
        var groupB = groups.getOrCreate("b");
        try (BadCollector badCollector = new BadCollector(badOutputFile(), tolerance, COLLECT_ALL)) {
            // when
            badCollector.collectBadRelationship("1", groupA, "T", "2", groupB, "1");

            // then
            assertEquals(1, badCollector.badEntries());
        }
    }

    @Test
    void shouldThrowExceptionIfDuplicateNodeTipsUsOverTheToleranceEdge() throws IOException {
        // given
        int tolerance = 1;

        try (BadCollector badCollector = new BadCollector(badOutputFile(), tolerance, COLLECT_ALL)) {
            // when
            collectBadRelationship(badCollector, group);
            assertThrows(InputException.class, () -> badCollector.collectDuplicateNode(1, 1, group));
        }
    }

    @Test
    void shouldThrowExceptionIfBadRelationshipsTipsUsOverTheToleranceEdge() throws IOException {
        // given
        int tolerance = 1;

        try (BadCollector badCollector = new BadCollector(badOutputFile(), tolerance, COLLECT_ALL)) {
            // when
            badCollector.collectDuplicateNode(1, 1, group);
            assertThrows(InputException.class, () -> collectBadRelationship(badCollector, group));
        }
    }

    @Test
    void shouldNotCollectBadRelationshipsIfWeShouldOnlyBeCollectingNodes() throws IOException {
        // given
        int tolerance = 1;

        try (BadCollector badCollector = new BadCollector(badOutputFile(), tolerance, BadCollector.DUPLICATE_NODES)) {
            // when
            badCollector.collectDuplicateNode(1, 1, group);
            assertThrows(InputException.class, () -> collectBadRelationship(badCollector, group));
            assertEquals(1 /* only duplicate node collected */, badCollector.badEntries());
        }
    }

    @Test
    void shouldNotCollectBadNodesIfWeShouldOnlyBeCollectingRelationships() throws IOException {
        // given
        int tolerance = 1;

        try (BadCollector badCollector = new BadCollector(badOutputFile(), tolerance, BadCollector.BAD_RELATIONSHIPS)) {
            // when
            collectBadRelationship(badCollector, group);
            assertThrows(InputException.class, () -> badCollector.collectDuplicateNode(1, 1, group));
            assertEquals(1 /* only duplicate rel collected */, badCollector.badEntries());
        }
    }

    @Test
    void shouldCollectUnlimitedNumberOfBadEntriesIfToldTo() {
        // GIVEN
        try (BadCollector collector = new BadCollector(nullOutputStream(), UNLIMITED_TOLERANCE, COLLECT_ALL)) {
            // WHEN
            int count = 10_000;
            for (int i = 0; i < count; i++) {
                collector.collectDuplicateNode(i, i, group);
            }

            // THEN
            assertEquals(count, collector.badEntries());
        }
    }

    @Test
    void skipBadEntriesLogging() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (BadCollector badCollector =
                new BadCollector(outputStream, 100, COLLECT_ALL, 10, true, BadCollector.NO_MONITOR)) {
            collectBadRelationship(badCollector, group);
            for (int i = 0; i < 2; i++) {
                badCollector.collectDuplicateNode(i, i, group);
            }
            collectBadRelationship(badCollector, group);
            badCollector.collectExtraColumns("a,b,c", 1, "a");
            assertEquals(0, outputStream.size(), "Output stream should not have any reported entries");
        }
    }

    @Test
    void shouldApplyBackPressure() throws Exception {
        // given
        int backPressureThreshold = 10;
        BlockableMonitor monitor = new BlockableMonitor();
        try (OtherThreadExecutor t2 = new OtherThreadExecutor("T2");
                BadCollector badCollector = new BadCollector(
                        nullOutputStream(), UNLIMITED_TOLERANCE, COLLECT_ALL, backPressureThreshold, false, monitor)) {
            try (monitor) {
                for (int i = 0; i < backPressureThreshold; i++) {
                    badCollector.collectDuplicateNode(i, i, group);
                }

                // when
                Future<Object> enqueue =
                        t2.executeDontWait(command(() -> badCollector.collectDuplicateNode(999, 999, group)));
                t2.waitUntilWaiting(waitDetails -> waitDetails.isAt(BadCollector.class, "collect"));
                monitor.unblock();

                // then
                enqueue.get();
            }
        }
    }

    private static void collectBadRelationship(Collector collector, Group group) {
        collector.collectBadRelationship("A", group, "TYPE", "B", group, "A");
    }

    private OutputStream badOutputFile() throws IOException {
        Path badDataPath = Path.of("/tmp/foo2").toAbsolutePath();
        Path badDataFile = badDataFile(fs, badDataPath);
        return fs.openAsOutputStream(badDataFile, true);
    }

    private static Path badDataFile(FileSystemAbstraction fileSystem, Path badDataPath) throws IOException {
        fileSystem.mkdir(badDataPath.getParent());
        fileSystem.write(badDataPath);
        return badDataPath;
    }

    private static class BlockableMonitor implements BadCollector.Monitor, AutoCloseable {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void beforeProcessEvent() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void unblock() {
            latch.countDown();
        }

        @Override
        public void close() {
            unblock();
        }
    }
}
