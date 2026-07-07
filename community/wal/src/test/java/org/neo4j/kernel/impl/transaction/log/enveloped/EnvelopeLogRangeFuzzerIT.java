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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class EnvelopeLogRangeFuzzerIT {

    private static final int SEGMENT_BLOCK_SIZE = 512;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    TestDirectory testDirectory;

    @Inject
    RandomSupport randomSupport;

    private EnvelopedLogFiles envelopedLogFiles;
    private EnvelopeLogFilesRangeReader envelopeLogFilesRangeReader;

    @BeforeEach
    void setUp() throws IOException {
        envelopedLogFiles = envelopedLogFiles();
        envelopedLogFiles.initialise();
        envelopeLogFilesRangeReader = new EnvelopeLogFilesRangeReader(envelopedLogFiles, NullLogProvider.getInstance());
    }

    @AfterEach
    void tearDown() throws IOException {
        envelopedLogFiles.close();
    }

    @RepeatedTest(10)
    void even() throws IOException {
        getRangeAndCompare(new int[] {2, 16, 256, 1024});
    }

    @RepeatedTest(10)
    void uneven() throws IOException {
        getRangeAndCompare(new int[] {3, 13, 307, 1111});
    }

    void getRangeAndCompare(int[] dataSizes) throws IOException {
        writeData(envelopedLogFiles, randomSupport, dataSizes);
        var lowestIndex = getLowestIndex();
        var highestIndex = envelopedLogFiles.currentWriteChannel().currentIndex();

        var from = randomSupport.nextLong(lowestIndex, highestIndex);
        var to = randomSupport.nextLong(from, highestIndex);
        var storeChannels = envelopeLogFilesRangeReader.storeChannels(from, to);
        var storeChannelsQueue = new LinkedList<>(storeChannels.storeChannels());
        var storeChannel = storeChannelsQueue.poll();
        var versionedChannel = createChannel(storeChannel);
        try (var originalReadChannel = envelopedLogFiles.openReadChannel(storeChannels.fromIndex());
                var readChannel = new EnvelopeReadChannel(
                        versionedChannel,
                        SEGMENT_BLOCK_SIZE,
                        (LogVersionBridge) (channel, raw) -> {
                            var poll = storeChannelsQueue.poll();
                            if (poll == null) {
                                return channel;
                            }
                            channel.close();
                            return createChannel(poll);
                        },
                        EmptyMemoryTracker.INSTANCE,
                        false)) {
            originalReadChannel.goToEntry(storeChannels.fromIndex());
            readChannel.goToNextEnvelope(); // positioning does not pre-read next envelope so we need to do this
            while (!storeChannelsQueue.isEmpty() || readChannel.position() < storeChannels.toPosition()) {
                assertThat(readChannel.position()).isEqualTo(originalReadChannel.position());
                assertThat(readChannel.entryIndex()).isEqualTo(originalReadChannel.entryIndex());
                assertThat(readChannel.currentTerm()).isEqualTo(originalReadChannel.currentTerm());
                assertThat(readChannel.getChecksum()).isEqualTo(originalReadChannel.getChecksum());
                try {
                    readChannel.goToNextEntry();
                } catch (ReadPastEndException ignore) {
                    if (readChannel.currentIndex != storeChannels.toIndex()) {
                        throw new IllegalStateException("Read past end of log range prematurely");
                    }
                }
                originalReadChannel.goToNextEntry();
            }
        }
    }

    private LogVersionedStoreChannel createChannel(StoreChannel storeChannel) throws IOException {
        var position = storeChannel.position();
        storeChannel.position(0);
        var logHeader = LogHeaderReader.readLogHeader(storeChannel, true, null, EmptyMemoryTracker.INSTANCE);
        storeChannel.position(position);
        return new PhysicalLogVersionedStoreChannel(
                storeChannel, logHeader, null, ChannelNativeAccessor.EMPTY_ACCESSOR, LogTracers.NULL);
    }

    private long getLowestIndex() throws IOException {
        try (var envelopeLog = envelopedLogFiles.openReadChannel()) {
            envelopeLog.alignWithStartEntry();
            return envelopeLog.entryIndex();
        }
    }

    private static void writeData(EnvelopedLogFiles envelopedLogFiles, RandomSupport randomSupport, int[] dataSizes)
            throws IOException {
        var shouldTruncate = randomSupport.nextBoolean();
        var shouldPrune = randomSupport.nextBoolean();
        var leftToWrite = randomSupport.nextInt((int) ByteUnit.Byte.toBytes(4500), (int) ByteUnit.KibiByte.toBytes(12));

        long currentTerm = 1;

        var logOperation = new ArrayList<ThrowingConsumer<EnvelopedLogFiles, IOException>>();

        while (leftToWrite > 0) {
            var data = randomSupport.nextBytes(randomSupport.among(dataSizes));
            if (randomSupport.nextBoolean()) {
                currentTerm++;
            }
            long termToWrite = currentTerm;
            logOperation.add(e -> writeData(envelopedLogFiles.currentWriteChannel(), data, termToWrite));
            leftToWrite -= data.length;
        }

        var currentEndIndex = logOperation.size() - 1;
        var pruneIndex = randomSupport.nextLong(currentEndIndex - 3);
        var truncateIndex = randomSupport.nextLong(pruneIndex + 1, currentEndIndex - 2);
        if (shouldPrune) {
            logOperation.add(randomSupport.nextInt((int) (pruneIndex + 1), currentEndIndex), e -> {
                e.currentWriteChannel().prepareForFlush().flush();
                e.prune(pruneIndex);
            });
        }
        if (shouldTruncate) {
            logOperation.add(randomSupport.nextInt((int) (truncateIndex + 2), currentEndIndex), e -> {
                e.currentWriteChannel().prepareForFlush().flush();
                e.truncate(truncateIndex);
            });
        }
        for (var op : logOperation) {
            op.accept(envelopedLogFiles);
        }
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();
    }

    private static void writeData(EnvelopeWriteChannel writeChannel, byte[] data, long term) throws IOException {
        writeChannel.beginChecksumForWriting();
        if (term >= 0) {
            writeChannel.putTerm(term);
        }
        writeChannel.putVersion(KernelVersion.GLORIOUS_FUTURE.version());
        writeChannel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writeChannel.put(data, data.length);
        writeChannel.endCurrentEntry();
    }

    EnvelopedLogFiles envelopedLogFiles() {
        var baseFileName = "raft.log";
        var baseFolder = testDirectory.directory("logsFolder");
        return new EnvelopedLogFiles(
                new LogsRepository(fs, new SequentialFileNameHelper(baseFolder, baseFileName)),
                (fileVersion, preFileIndex, preFileChecksum, segmentSize, lastTerm) -> LogFormat.fromKernelVersion(
                                KernelVersion.GLORIOUS_FUTURE)
                        .newHeader(
                                fileVersion,
                                preFileIndex,
                                lastTerm,
                                StoreIdentifier.newStoreIdentifier(
                                        ThreadLocalRandom.current().nextLong()),
                                segmentSize,
                                preFileChecksum,
                                KernelVersion.GLORIOUS_FUTURE,
                                UNSPECIFIED_CREATION_TIME),
                SEGMENT_BLOCK_SIZE,
                4,
                8,
                EmptyMemoryTracker.INSTANCE,
                ThresholdFactory.PRUNE_ALL,
                new StoreChannelNativeAccessor(
                        fs, NativeAccessProvider.getNativeAccess(), NullLogProvider.getInstance(), s -> {}),
                NullLogProvider.getInstance());
    }
}
