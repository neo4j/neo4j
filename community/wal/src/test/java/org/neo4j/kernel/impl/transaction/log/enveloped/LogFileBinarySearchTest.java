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
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneThreshold;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class LogFileBinarySearchTest {

    private static final String EIGHT_BYTES_MESSAGE = "message!";
    public static final int writeBufferedBlocks = 2;
    private final int segmentBlockSize = 256;
    private final int totalSegments = 3;
    private final LogPruneThreshold pruneStrategy = ThresholdFactory.PRUNE_ALL;

    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private EnvelopedLogFiles envelopedLogFiles;

    private static void writeData(EnvelopeWriteChannel writeChannel, byte[] data) throws IOException {
        writeData(writeChannel, data, -1L);
    }

    private static void writeData(EnvelopeWriteChannel writeChannel, byte[] data, int index) throws IOException {
        data[0] = (byte) index;
        writeData(writeChannel, data, -1L);
    }

    private static void writeData(EnvelopeWriteChannel writeChannel, byte[] data, long term) throws IOException {
        writeChannel.beginChecksumForWriting();
        if (term >= 0) {
            writeChannel.putTerm(term);
        }
        writeChannel.putVersion(KernelVersion.V2026_01.version());
        writeChannel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writeChannel.put(data, data.length);
        writeChannel.endCurrentEntry();
    }

    @BeforeEach
    void setUp() {
        recreateEnvelopedLogFiles();
    }

    private void recreateEnvelopedLogFiles() {
        var baseFileName = "raft.log";
        var baseFolder = testDirectory.directory("logsFolder");
        envelopedLogFiles = new EnvelopedLogFiles(
                new LogsRepository(fs, new SequentialFileNameHelper(baseFolder, baseFileName)),
                (fileVersion, preFileIndex, preFileChecksum, segmentSize, lastTerm) -> LogFormat.fromKernelVersion(
                                KernelVersion.GLORIOUS_FUTURE)
                        .newHeader(
                                fileVersion,
                                preFileIndex,
                                lastTerm,
                                StoreIdentifier.newStoreIdentifier(0),
                                segmentSize,
                                preFileChecksum,
                                KernelVersion.GLORIOUS_FUTURE,
                                UNSPECIFIED_CREATION_TIME),
                segmentBlockSize,
                writeBufferedBlocks,
                totalSegments,
                EmptyMemoryTracker.INSTANCE,
                pruneStrategy,
                new StoreChannelNativeAccessor(
                        fs, NativeAccessProvider.getNativeAccess(), NullLogProvider.getInstance(), s -> {}),
                NullLogProvider.getInstance());
    }

    /**
     * Structure of files - one file per line below:
     * | LOG-HEADER[li: -1]
     */
    @Test
    void shouldFindFirstFileIfJustInitialized() throws IOException {
        envelopedLogFiles.initialise();
        try (var readChannel = envelopedLogFiles.openReadChannel(0)) {
            var currentLogHeader = readChannel.logHeader();
            assertThat(currentLogHeader.getLogVersion()).isZero();
            assertThat(currentLogHeader.getLastAppendIndex()).isEqualTo(-1);
        }
    }

    /**
     * Structure of files - one file per line below:
     * | LOG-HEADER[li: -1] | ENVELOPE[i:0 t:FULL l:225]  | ENVELOPE[i:1 t:FULL l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:2 t:FULL l:225]  | ENVELOPE[i:3 t:FULL l:225]
     */
    @Test
    void shouldFindCorrectFileWithFullEnvelopes() throws IOException {
        var baseData = new byte[segmentBlockSize - HEADER_SIZE];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        for (int i = 0; i < 4; i++) {
            writeData(writeChannel, baseData, i);
        }
        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 4; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var currentLogHeader = readChannel.logHeader();
                var readData = new byte[baseData.length];
                while (readChannel.entryIndex() != i) {
                    readChannel.goToNextEntry();
                }
                readChannel.get(readData, readData.length);
                // assert it did not switch file
                assertThat(currentLogHeader).isEqualTo(readChannel.logHeader());
                // and that we found the expected entry
                assertThat(readData[0]).isEqualTo((byte) i);
            }
        }
    }

    /**
     * Structure of files - one file per line below:
     * | LOG-HEADER[li: -1] | ENVELOPE[i:0 t:FULL l:85] ENVELOPE[i:1 t:BEGIN l:109]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:END l:77]
     */
    @Test
    void shouldFindCorrectFileWhenThereSmallAndMiddleEnvelopes() throws IOException {
        var smallBaseData = new byte[segmentBlockSize / 3];
        var largeBaseData = new byte[segmentBlockSize * 7];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallBaseData, 0);
        writeData(writeChannel, largeBaseData, 1);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 2; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var currentLogHeader = readChannel.logHeader();
                var readData = new byte[8];
                while (readChannel.entryIndex() != i) {
                    readChannel.goToNextEntry();
                }
                readChannel.get(readData, readData.length);
                // assert it did not switch file
                assertThat(currentLogHeader).isEqualTo(readChannel.logHeader());
                // and that we found the expected entry
                assertThat(readData[0]).isEqualTo((byte) i);
            }
        }
    }

    /**
     * Structure of files - one file per line below:
     * | LOG-HEADER[li: -1] | ENVELOPE[i:0 t:BEGIN l:225]  | ENVELOPE[i:0 t:MIDDLE l:225]
     * | LOG-HEADER[li: 0] | ENVELOPE[i:0 t:MIDDLE l:225]  | ENVELOPE[i:0 t:MIDDLE l:225]
     * | LOG-HEADER[li: 0] | ENVELOPE[i:0 t:MIDDLE l:225]  | ENVELOPE[i:0 t:MIDDLE l:225]
     * | LOG-HEADER[li: 0] | ENVELOPE[i:0 t:MIDDLE l:225]  | ENVELOPE[i:0 t:END l:217]
     * | LOG-HEADER[li: 0] | ENVELOPE[i:1 t:FULL l:85]
     */
    @Test
    void shouldFindCorrectFileWhenThereMiddleEnvelopesAndThenSmall() throws IOException {
        var smallBaseData = new byte[segmentBlockSize / 3];
        var largeBaseData = new byte[segmentBlockSize * 7];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, largeBaseData, 0);
        writeData(writeChannel, smallBaseData, 1);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 2; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var currentLogHeader = readChannel.logHeader();
                var readData = new byte[8];
                while (readChannel.entryIndex() != i) {
                    readChannel.goToNextEntry();
                }
                readChannel.get(readData, readData.length);
                // assert it did not switch file
                assertThat(currentLogHeader).isEqualTo(readChannel.logHeader());
                // and that we found the expected entry
                assertThat(readData[0]).isEqualTo((byte) i);
            }
        }
    }

    /**
     * Structure of files - one file per line below:
     * | LOG-HEADER[li: -1] | ENVELOPE[i:0 t:BEGIN l:225]  | ENVELOPE[i:0 t:MIDDLE l:225]
     * | LOG-HEADER[li: 0] | ENVELOPE[i:0 t:MIDDLE l:225]  | ENVELOPE[i:0 t:MIDDLE l:225]
     * | LOG-HEADER[li: 0] | ENVELOPE[i:0 t:MIDDLE l:225]  | ENVELOPE[i:0 t:END l:155] ENVELOPE[i:1 t:BEGIN l:39]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:END l:116]
     */
    @Test
    void shouldFindCorrectFileWhenThereMiddleEnvelopes() throws IOException {
        var largeBaseData = new byte[segmentBlockSize * 5];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, largeBaseData, 0);
        writeData(writeChannel, largeBaseData, 1);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 2; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var currentLogHeader = readChannel.logHeader();
                var readData = new byte[8];
                while (readChannel.entryIndex() != i) {
                    readChannel.goToNextEntry();
                }
                readChannel.get(readData, readData.length);
                // assert it did not switch file
                assertThat(currentLogHeader).isEqualTo(readChannel.logHeader());
                // and that we found the expected entry
                assertThat(readData[0]).isEqualTo((byte) i);
            }
        }
    }

    /**
     * Structure of files - one file per line below:
     * | LOG-HEADER[li: -1] | ENVELOPE[i:0 t:FULL l:85] ENVELOPE[i:1 t:BEGIN l:109]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:MIDDLE l:225]  | ENVELOPE[i:1 t:MIDDLE l:225]
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:END l:46] ENVELOPE[i:2 t:BEGIN l:148]  | ENVELOPE[i:2 t:END l:108] ENVELOPE[i:3 t:BEGIN l:86]
     * | LOG-HEADER[li: 3] | ENVELOPE[i:3 t:MIDDLE l:225]  | ENVELOPE[i:3 t:MIDDLE l:225]
     * | LOG-HEADER[li: 3] | ENVELOPE[i:3 t:MIDDLE l:225]  | ENVELOPE[i:3 t:END l:7] ENVELOPE[i:4 t:FULL l:85]
     */
    @Test
    void shouldFindCorrectFileWhenThereAreMixOfEnvelopeSizes() throws IOException {
        var smallBaseData = new byte[segmentBlockSize / 3];
        var fullBaseData = new byte[segmentBlockSize];
        var largeBaseData = new byte[segmentBlockSize * 3];
        var verLargeBaseData = new byte[segmentBlockSize * 5];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallBaseData, 0);
        writeData(writeChannel, verLargeBaseData, 1);
        writeData(writeChannel, fullBaseData, 2);
        writeData(writeChannel, largeBaseData, 3);
        writeData(writeChannel, smallBaseData, 4);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 5; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var currentLogHeader = readChannel.logHeader();
                var readData = new byte[8];
                while (readChannel.entryIndex() != i) {
                    readChannel.goToNextEntry();
                }
                readChannel.get(readData, readData.length);
                // assert it did not switch file
                assertThat(currentLogHeader).isEqualTo(readChannel.logHeader());
                // and that we found the expected entry
                assertThat(readData[0]).isEqualTo((byte) i);
            }
        }
    }

    /**
     * Structure of files - one file per line below after pruning:
     * | LOG-HEADER[li: 17] | ENVELOPE[i:18 t:FULL l:128] ENVELOPE[i:19 t:BEGIN l:66]  | ENVELOPE[i:19 t:END l:62] ENVELOPE[i:20 t:FULL l:128]
     * | LOG-HEADER[li: 20] | ENVELOPE[i:21 t:FULL l:128] ENVELOPE[i:22 t:BEGIN l:66]  | ENVELOPE[i:22 t:END l:62] ENVELOPE[i:23 t:FULL l:128]
     * | LOG-HEADER[li: 23] | ENVELOPE[i:24 t:FULL l:128] ENVELOPE[i:25 t:BEGIN l:66]  | ENVELOPE[i:25 t:END l:62] ENVELOPE[i:26 t:FULL l:128]  ......
     */
    @Test
    void shouldFindFileCorrectlyEvenAfterPrune() throws IOException {
        var baseData = new byte[segmentBlockSize / 2];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        for (int i = 0; i < 40; i++) {
            baseData[0] = (byte) i; // set unique data index
            writeData(writeChannel, baseData);
        }
        writeChannel.prepareForFlush().flush();

        long prunedIndex = envelopedLogFiles.prune(20);

        for (int i = (int) (prunedIndex + 1); i < 40; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var currentLogHeader = readChannel.logHeader();
                var readData = new byte[8];
                while (readChannel.entryIndex() != i) {
                    readChannel.goToNextEntry();
                }
                readChannel.get(readData, readData.length);
                // assert it did not switch file
                assertThat(currentLogHeader).isEqualTo(readChannel.logHeader());
                // and that we found the expected entry
                assertThat(readData[0]).isEqualTo((byte) i);
            }
        }
    }

    /**
     * Structure of files - one file per line below after pruning:
     * | LOG-HEADER[li: 1] | ENVELOPE[i:1 t:END l:93] ENVELOPE[i:2 t:FULL l:8] ENVELOPE[i:3 t:FULL l:8]
     */
    @Test
    void shouldReturnNullIfIndexHasPruned() throws IOException {
        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        var totalFileDataSize = segmentBlockSize * (totalSegments - 1);
        var largeData = new byte[totalFileDataSize / 2];
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        writeData(envelopedLogFiles.currentWriteChannel(), largeData); // spills over to next file
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        // 1 is completed in first file, so we expect it to be removed
        assertThat(envelopedLogFiles.prune(2)).isOne();

        // 0 has been pruned entirley
        assertThat(envelopedLogFiles.openReadChannel(0)).isNull();
        // 1 spills over but should still be considered pruned
        assertThat(envelopedLogFiles.openReadChannel(1)).isNull();
        // 2 has not been pruned and should open a file
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel(2)) {
            envelopeReadChannel.alignWithStartEntry();
            assertThat(envelopeReadChannel.entryIndex()).isEqualTo(2);
        }
    }
}
