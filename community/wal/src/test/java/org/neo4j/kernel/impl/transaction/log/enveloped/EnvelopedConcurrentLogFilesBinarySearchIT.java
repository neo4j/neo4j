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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * Stress test that exercises {@link EnvelopedLogFiles#storeChannels} concurrently with appends. The
 * log files are sized so that they rotate frequently and the index range spans multiple files, which
 * means the binary search in {@link LogBinarySearch} will run at the same time as new log files are
 * being created and old ones pruned. Any safety issue with binary search for log files during file
 * rotation should surface here as an unexpected exception in one of the reader tasks.
 */
@TestDirectoryExtension
@RandomSupportExtension
public class EnvelopedConcurrentLogFilesBinarySearchIT {

    private static final int SEGMENT_BLOCK_SIZE = 1024;
    private static final int TOTAL_SEGMENTS = 4;
    private static final int WRITE_BUFFERED_BLOCKS = 2;
    private static final int N_READERS = 4;
    private static final int RUN_SECONDS = 5;
    private static final long MIN_ENTRIES_TO_KEEP = 10;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private RandomSupport randomSupport;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private EnvelopedLogFiles envelopedLogFiles;

    @BeforeEach
    void setUp() throws IOException {
        envelopedLogFiles = createEnvelopedLogFiles();
        envelopedLogFiles.initialise();
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(1, MINUTES)).isTrue();
        envelopedLogFiles.close();
    }

    @Test
    void storeChannelsShouldBeSafeDuringFileRotation() throws Throwable {
        var writerCompleted = new AtomicBoolean();
        // Highest index that has been written and flushed to disk.
        var lastFlushedIndex = new AtomicLong(-1);

        var readerFutures = new ArrayList<Future<Long>>();
        for (var i = 0; i < N_READERS; i++) {
            readerFutures.add(executor.submit(new ReaderTask(lastFlushedIndex, writerCompleted)));
        }

        var writerFuture = executor.submit(
                new WriterTask(lastFlushedIndex, MIN_ENTRIES_TO_KEEP * 2, Duration.ofSeconds(RUN_SECONDS)));
        long writtenCount = writerFuture.get();
        writerCompleted.set(true);

        assertThat(writtenCount).as("writer should have appended entries").isGreaterThan(0);

        long totalReads = 0;
        for (var f : readerFutures) {
            totalReads += f.get(); // main purpose is to rethrow any exception from the reader
        }
        assertThat(totalReads)
                .as("readers should have called storeChannels at least once")
                .isGreaterThan(0);
    }

    private class WriterTask implements Callable<Long> {
        private final AtomicLong lastFlushedIndex;
        private final long minWrites;
        private final Duration duration;

        WriterTask(AtomicLong lastFlushedIndex, long minWrites, Duration duration) {
            this.lastFlushedIndex = lastFlushedIndex;
            this.minWrites = minWrites;
            this.duration = duration;
        }

        @Override
        public Long call() throws IOException {
            var writer = envelopedLogFiles.currentWriteChannel();
            var endTime = System.currentTimeMillis() + duration.toMillis();
            long count = 0;
            long term = 1;
            while (count < minWrites || endTime > System.currentTimeMillis()) {
                // Vary payload sizes so entries don't always rotate at the same boundary.
                // Max payload kept well below maxFileSize (= SEGMENT_BLOCK_SIZE * TOTAL_SEGMENTS = 4096)
                // so a single entry never spans more than a couple of segments.
                int size = randomSupport.nextInt(64, 600);
                var data = randomSupport.nextBytes(size);
                writeEntry(writer, data, term);
                count++;
                if (randomSupport.nextInt(100) == 0) {
                    term++;
                }
                if (count > 2 && randomSupport.nextInt(10) == 0) {
                    envelopedLogFiles.prune(randomSupport.nextLong(1, count - 1));
                }
                // Flush every few entries; the flush publishes the new index to readers via
                // lastFlushedIndex. Without the flush, readers could legitimately fail to locate
                // the entry because it hasn't been written to disk yet.
                if (count % 3 == 0) {
                    writer.prepareForFlush().flush();
                    lastFlushedIndex.set(writer.currentIndex());
                }
            }
            writer.prepareForFlush().flush();
            lastFlushedIndex.set(writer.currentIndex());
            return count;
        }
    }

    private class ReaderTask implements Callable<Long> {
        private final AtomicLong lastFlushedIndex;
        private final AtomicBoolean writerCompleted;

        ReaderTask(AtomicLong lastFlushedIndex, AtomicBoolean writerCompleted) {
            this.lastFlushedIndex = lastFlushedIndex;
            this.writerCompleted = writerCompleted;
        }

        @Override
        public Long call() throws IOException, InterruptedException {
            long calls = 0;
            EnvelopeLogFilesRangeReader rangeReader =
                    new EnvelopeLogFilesRangeReader(envelopedLogFiles, NullLogProvider.getInstance());
            while (!writerCompleted.get()) {
                long toIndex = lastFlushedIndex.get();
                if (toIndex < 0) {
                    // Nothing has been flushed yet, give the writer a chance to make progress.
                    Thread.sleep(10);
                    continue;
                }
                long fromIndex = randomSupport.nextLong(0, toIndex + 1);
                try {
                    var channels = rangeReader.storeChannels(fromIndex, toIndex);
                    IOUtils.closeAllSilently(channels.storeChannels());
                } catch (NoSuchFileException e) {
                    // Ignore this - it's still possible for a log version to get pruned after we've decided
                    // to read it, but before we've finished with it. In production this would get returned to
                    // the client as a failure to pull log entries.
                }
                calls++;
            }
            return calls;
        }
    }

    private static void writeEntry(EnvelopeWriteChannel writeChannel, byte[] data, long term) throws IOException {
        writeChannel.beginChecksumForWriting();
        writeChannel.putTerm(term);
        writeChannel.putVersion(KernelVersion.GLORIOUS_FUTURE.version());
        writeChannel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writeChannel.put(data, data.length);
        writeChannel.endCurrentEntry();
    }

    private EnvelopedLogFiles createEnvelopedLogFiles() {
        var baseFolder = testDirectory.directory("logsFolder");
        var filesHelper = new SequentialFileNameHelper(baseFolder, "raft.log");
        return new EnvelopedLogFiles(
                new LogsRepository(fs, filesHelper),
                (fileVersion, preFileIndex, preFileChecksum, segmentSize, lastTerm) -> LogFormat.fromKernelVersion(
                                KernelVersion.GLORIOUS_FUTURE)
                        .newHeader(
                                fileVersion,
                                preFileIndex,
                                lastTerm,
                                StoreIdentifier.newStoreIdentifier(12345),
                                segmentSize,
                                preFileChecksum,
                                KernelVersion.GLORIOUS_FUTURE,
                                UNSPECIFIED_CREATION_TIME),
                SEGMENT_BLOCK_SIZE,
                WRITE_BUFFERED_BLOCKS,
                TOTAL_SEGMENTS,
                EmptyMemoryTracker.INSTANCE,
                ThresholdFactory.fromConfigValue(
                        fs, NullLogProvider.getInstance(), Clock.systemUTC(), MIN_ENTRIES_TO_KEEP + " entries"),
                new StoreChannelNativeAccessor(
                        fs, NativeAccessProvider.getNativeAccess(), NullLogProvider.getInstance(), s -> {}),
                NullLogProvider.getInstance());
    }
}
