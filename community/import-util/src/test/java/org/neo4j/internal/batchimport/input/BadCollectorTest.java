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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.internal.batchimport.input.BadCollector.COLLECT_ALL;
import static org.neo4j.internal.batchimport.input.BadCollector.DEFAULT_BACK_PRESSURE_THRESHOLD;
import static org.neo4j.internal.batchimport.input.BadCollector.UNLIMITED_TOLERANCE;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.APPEND_OPTIONS;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.TRUNCATE_OPTIONS;
import static org.neo4j.test.OtherThreadExecutor.command;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
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
        try (var badCollector = BadCollector.create(badOutputFile(), tolerance)) {
            // when
            badCollector.collectBadRelationship("1", groupA, "T", "2", groupB, "1", "source", 8L);

            // then
            assertThat(badCollector.badEntries()).isOne();
        }
    }

    @Test
    void shouldThrowExceptionIfDuplicateNodeTipsUsOverTheToleranceEdge() throws IOException {
        // given
        int tolerance = 1;

        try (var badCollector = BadCollector.create(badOutputFile(), tolerance)) {
            // when
            collectBadRelationship(badCollector, group);
            assertThatExceptionOfType(InputException.class)
                    .isThrownBy(() -> badCollector.collectDuplicateNode(1, 1, group, "source", 8L));
        }
    }

    @Test
    void shouldThrowExceptionIfBadRelationshipsTipsUsOverTheToleranceEdge() throws IOException {
        // given
        int tolerance = 1;

        try (var badCollector = BadCollector.create(badOutputFile(), tolerance)) {
            // when
            badCollector.collectDuplicateNode(1, 1, group, "source", 8L);
            assertThatExceptionOfType(InputException.class)
                    .isThrownBy(() -> collectBadRelationship(badCollector, group));
        }
    }

    @Test
    void shouldNotCollectBadRelationshipsIfWeShouldOnlyBeCollectingNodes() throws IOException {
        // given
        int tolerance = 1;

        try (var badCollector = BadCollector.create(badOutputFile(), tolerance, BadCollector.DUPLICATE_NODES)) {
            // when
            badCollector.collectDuplicateNode(1, 1, group, "source", 8L);
            assertThatExceptionOfType(InputException.class)
                    .isThrownBy(() -> collectBadRelationship(badCollector, group));
            assertThat(badCollector.badEntries()).isOne();
        }
    }

    @Test
    void shouldNotCollectBadNodesIfWeShouldOnlyBeCollectingRelationships() throws IOException {
        // given
        int tolerance = 1;

        try (var badCollector = BadCollector.create(badOutputFile(), tolerance, BadCollector.BAD_RELATIONSHIPS)) {
            // when
            collectBadRelationship(badCollector, group);
            assertThatExceptionOfType(InputException.class)
                    .isThrownBy(() -> badCollector.collectDuplicateNode(1, 1, group, "source", 8L));
            assertThat(badCollector.badEntries()).isOne();
        }
    }

    @Test
    void shouldCollectUnlimitedNumberOfBadEntriesIfToldTo() throws IOException {
        // GIVEN
        try (var collector = BadCollector.create(nullOutputStream(), UNLIMITED_TOLERANCE, COLLECT_ALL)) {
            // WHEN
            int count = 10_000;
            for (int i = 0; i < count; i++) {
                collector.collectDuplicateNode(i, i, group, "source", 8L);
            }

            // THEN
            assertThat(collector.badEntries()).isEqualTo(count);
        }
    }

    @Test
    void skipBadEntriesLogging() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (BadCollector badCollector =
                new BadCollector(outputStream, 100, COLLECT_ALL, 10, true, BadCollector.NO_MONITOR)) {
            collectBadRelationship(badCollector, group);
            for (int i = 0; i < 2; i++) {
                badCollector.collectDuplicateNode(i, i, group, "source", 8L);
            }
            collectBadRelationship(badCollector, group);
            badCollector.collectExtraColumns("a,b,c", 1, "a");
            assertThat(outputStream.size())
                    .as("Output stream should not have any reported entries")
                    .isZero();
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
                    badCollector.collectDuplicateNode(i, i, group, "source", 8L);
                }

                // when
                Future<Object> enqueue = t2.executeDontWait(
                        command(() -> badCollector.collectDuplicateNode(999, 999, group, "source", 8L)));
                t2.waitUntilWaiting(waitDetails -> waitDetails.isAt(BadCollector.class, "collect"));
                monitor.unblock();

                // then
                enqueue.get();
            }
        }
    }

    @Test
    void shouldCheckpointBadEntriesCountAndReportPosition() throws IOException {
        // given
        int count = 5;
        Path report = Path.of("checkpointed-report.json.log").toAbsolutePath();

        // when
        var checkpoint = new ByteArrayOutputStream();
        try (var badCollector = collectorReportingTo(fs.open(report, TRUNCATE_OPTIONS))) {
            for (int i = 0; i < count; i++) {
                badCollector.collectDuplicateNode(i, i, group, "source", 8L + i);
            }
            badCollector.checkpoint(new DataOutputStream(checkpoint));
        }

        // then
        try (var reader = new DataInputStream(new ByteArrayInputStream(checkpoint.toByteArray()))) {
            assertThat(reader.readLong()).as("bad entries").isEqualTo(count);
            assertThat(reader.readLong()).as("report position").isEqualTo(fs.getFileSize(report));
            assertThat(reader.available()).isZero();
        }
    }

    @Test
    void shouldResumeBadEntriesCountFromCheckpoint() throws IOException {
        // given
        int count = 5;
        Path report = Path.of("resumed-count.json.log").toAbsolutePath();
        var checkpoint = checkpointAfterCollecting(report, count, UNLIMITED_TOLERANCE);

        // when
        try (var badCollector = collectorReportingTo(fs.open(report, APPEND_OPTIONS))) {
            badCollector.resumeFromCheckpoint(checkpoint);

            // then
            assertThat(badCollector.badEntries()).isEqualTo(count);
        }
    }

    @Test
    void shouldCountToleranceFromResumedBadEntriesCount() throws IOException {
        // given
        int tolerance = 5;
        Path report = Path.of("resumed-tolerance.json.log").toAbsolutePath();
        var checkpoint = checkpointAfterCollecting(report, tolerance, tolerance);

        try (var badCollector = collectorReportingTo(fs.open(report, APPEND_OPTIONS), tolerance)) {
            badCollector.resumeFromCheckpoint(checkpoint);

            // when/then
            assertThatExceptionOfType(InputException.class)
                    .isThrownBy(() -> badCollector.collectDuplicateNode(1, 1, group, "source", 8L));
        }
    }

    @Test
    void shouldWaitForQueueToBeDrainedBeforeCompletingCheckpoint() throws Exception {
        // given
        BlockableMonitor monitor = new BlockableMonitor();
        try (OtherThreadExecutor t2 = new OtherThreadExecutor("T2");
                BadCollector badCollector = new BadCollector(
                        nullOutputStream(),
                        UNLIMITED_TOLERANCE,
                        COLLECT_ALL,
                        DEFAULT_BACK_PRESSURE_THRESHOLD,
                        false,
                        monitor)) {
            try (monitor) {
                badCollector.collectDuplicateNode(1, 1, group, "source", 8L);

                // when
                Future<Object> checkpoint = t2.executeDontWait(
                        command(() -> badCollector.checkpoint(new DataOutputStream(nullOutputStream()))));
                t2.waitUntilWaiting(waitDetails -> waitDetails.isAt(BadCollector.class, "checkpoint"));
                monitor.unblock();

                // then
                checkpoint.get();
            }
        }
    }

    @Test
    void shouldFlushAndForceReportedEntriesOnCheckpoint() throws IOException {
        // given
        int count = 5;
        Path report = Path.of("report.json.log").toAbsolutePath();
        var channel = new ForceCountingChannel(fs.open(report, TRUNCATE_OPTIONS));
        try (var badCollector = BadCollector.create(
                ProblemReporters.jsonOutputProblemHandler(channel), UNLIMITED_TOLERANCE, COLLECT_ALL, false)) {
            for (int i = 0; i < count; i++) {
                badCollector.collectDuplicateNode(i, i, group, "source", 8L + i);
            }

            // when
            badCollector.checkpoint(new DataOutputStream(nullOutputStream()));

            // then everything reported so far is in the report file, durably so
            SoftAssertions.assertSoftly(softly -> {
                try {
                    softly.assertThat(contents(report)).hasLineCount(count).endsWith("\n");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                softly.assertThat(channel.forceCount()).isPositive();
            });
        }
    }

    @Test
    void shouldDiscardEntriesReportedAfterTheCheckpointOnResume() throws IOException {
        // given a report checkpointed after 5 entries, with 3 more reported before the crash
        int checkpointed = 5;
        Path report = Path.of("resumed-report.json.log").toAbsolutePath();
        var checkpoint = new ByteArrayOutputStream();
        try (var badCollector = collectorReportingTo(fs.open(report, TRUNCATE_OPTIONS))) {
            for (int i = 0; i < checkpointed; i++) {
                badCollector.collectDuplicateNode(i, i, group, "source", 8L + i);
            }
            badCollector.checkpoint(new DataOutputStream(checkpoint));
            for (int i = checkpointed; i < checkpointed + 3; i++) {
                badCollector.collectDuplicateNode(i, i, group, "source", 8L + checkpointed + i);
            }
        }
        assertThat(contents(report)).hasLineCount(checkpointed + 3);

        // when resuming from that checkpoint
        try (var badCollector = collectorReportingTo(fs.open(report, APPEND_OPTIONS))) {
            badCollector.resumeFromCheckpoint(new DataInputStream(new ByteArrayInputStream(checkpoint.toByteArray())));

            // then the entries reported after it are gone, and the count matches what the report holds
            assertThat(contents(report)).hasLineCount(checkpointed).endsWith("\n");
            assertThat(badCollector.badEntries()).isEqualTo(checkpointed);

            // and re-reporting them as the input is revisited appends rather than overwrites
            badCollector.collectDuplicateNode(checkpointed, checkpointed, group, "source", 8L + checkpointed);
            badCollector.checkpoint(new DataOutputStream(nullOutputStream()));
            assertThat(contents(report)).hasLineCount(checkpointed + 1);
        }
    }

    @Test
    @Timeout(60)
    void shouldFailCheckpointWhenEntriesCanNoLongerBeReported() throws IOException {
        // given a handler that takes down the event processor, leaving the collected entry unreportable
        var failure = new UncheckedIOException(new IOException("No space left on device"));
        try (var badCollector =
                BadCollector.create(new FailingProblemHandler(failure), UNLIMITED_TOLERANCE, COLLECT_ALL, false)) {
            badCollector.collectDuplicateNode(1, 1, group, "source", 8L);

            // when/then
            assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> badCollector.checkpoint(new DataOutputStream(nullOutputStream())))
                    .withMessageContaining("left unreported")
                    .withCause(failure);
        }
    }

    @Test
    void shouldFailCheckpointWhenTheReportCouldNotBeWritten() throws IOException {
        // given a report on a channel that rejects every write, the way a full disk does
        int count = 5;
        Path report = Path.of("unwritable-report.json.log").toAbsolutePath();
        try (var badCollector = collectorReportingTo(new FullDiskChannel(fs.open(report, TRUNCATE_OPTIONS)))) {
            for (int i = 0; i < count; i++) {
                badCollector.collectDuplicateNode(i, i, group, "source", i);
            }
            // when/then a checkpoint cannot report entries as durable when none of them reached the report
            assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> badCollector.checkpoint(new DataOutputStream(nullOutputStream())));
        }
        assertThat(fs.getFileSize(report)).isZero();
    }

    @Test
    void shouldFailCollectingWhenEntriesCanNoLongerBeReported() throws Exception {
        // given a handler that takes down the event processor, so nothing will drain the queue again
        int backPressureThreshold = 10;
        var failure = new UncheckedIOException(new IOException("No space left on device"));
        var outcome = new AtomicReference<Throwable>();
        try (var badCollector = new BadCollector(
                new FailingProblemHandler(failure),
                UNLIMITED_TOLERANCE,
                COLLECT_ALL,
                backPressureThreshold,
                false,
                BadCollector.NO_MONITOR)) {
            Thread collecting = new Thread(() -> {
                try {
                    for (int i = 0; i <= backPressureThreshold; i++) {
                        badCollector.collectDuplicateNode(i, i, group, "source", i);
                    }
                } catch (Throwable t) {
                    outcome.set(t);
                }
            });
            collecting.setDaemon(true);
            // when collecting past the point where back pressure kicks in
            collecting.start();
            collecting.join(TimeUnit.SECONDS.toMillis(10));
            // then it fails rather than waiting on back pressure that nothing can relieve
            assertThat(collecting.isAlive())
                    .as("collecting parked on back pressure that nothing can relieve")
                    .isFalse();
            assertThat(outcome.get())
                    .as("the reporting failure should reach the collecting thread")
                    .hasRootCauseInstanceOf(IOException.class);
        }
    }

    private Collector collectorReportingTo(StoreChannel channel) {
        return collectorReportingTo(channel, UNLIMITED_TOLERANCE);
    }

    private Collector collectorReportingTo(StoreChannel channel, long tolerance) {
        return BadCollector.create(ProblemReporters.jsonOutputProblemHandler(channel), tolerance, COLLECT_ALL, false);
    }

    private DataInputStream checkpointAfterCollecting(Path report, int count, long tolerance) throws IOException {
        var checkpoint = new ByteArrayOutputStream();
        try (var badCollector = collectorReportingTo(fs.open(report, TRUNCATE_OPTIONS), tolerance)) {
            for (int i = 0; i < count; i++) {
                badCollector.collectDuplicateNode(i, i, group, "source", 8L + i);
            }
            badCollector.checkpoint(new DataOutputStream(checkpoint));
        }
        return new DataInputStream(new ByteArrayInputStream(checkpoint.toByteArray()));
    }

    private String contents(Path report) throws IOException {
        try (var input = fs.openAsInputStream(report)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private record FailingProblemHandler(RuntimeException failure) implements BadCollector.ProblemHandler {
        @Override
        public void handle(BadCollector.ProblemReporter reporter) {
            throw failure;
        }

        @Override
        public long checkpoint() {
            return 0;
        }

        @Override
        public void resumeFromCheckpoint(long position) {}

        @Override
        public void close() {}
    }

    private static class ForceCountingChannel extends DelegatingStoreChannel<StoreChannel> {
        private final AtomicInteger forceCount = new AtomicInteger();

        ForceCountingChannel(StoreChannel delegate) {
            super(delegate);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            forceCount.incrementAndGet();
            super.force(metaData);
        }

        int forceCount() {
            return forceCount.get();
        }
    }

    private static class FullDiskChannel extends DelegatingStoreChannel<StoreChannel> {
        FullDiskChannel(StoreChannel delegate) {
            super(delegate);
        }

        @Override
        public void writeAll(ByteBuffer src) throws IOException {
            throw new IOException("No space left on device");
        }
    }

    private static void collectBadRelationship(Collector collector, Group group) {
        collector.collectBadRelationship("A", group, "TYPE", "B", group, "A", "source", 8L);
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
