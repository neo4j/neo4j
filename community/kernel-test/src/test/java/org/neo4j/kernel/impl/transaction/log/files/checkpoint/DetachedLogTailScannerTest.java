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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata.EMPTY_APPEND_BATCH_INFO;
import static org.neo4j.kernel.impl.transaction.log.files.checkpoint.DetachedLogTailScanner.NO_TRANSACTION_ID;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LogIndexEncoding;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;

@EphemeralNeo4jLayoutExtension
class DetachedLogTailScannerTest {
    @Inject
    protected FileSystemAbstraction fs;

    @Inject
    protected DatabaseLayout databaseLayout;

    protected LogFiles logFiles;
    protected AssertableLogProvider logProvider;
    protected LogVersionRepository logVersionRepository;
    private SimpleAppendIndexProvider appendIndexProvider;

    private static Stream<Arguments> params() {
        return Stream.of(arguments(1, 2), arguments(42, 43));
    }

    @BeforeEach
    void setUp() throws IOException {
        logVersionRepository = new SimpleLogVersionRepository();
        appendIndexProvider = new SimpleAppendIndexProvider();
        logProvider = new AssertableLogProvider();
        logFiles = createLogFiles();
    }

    @Test
    void includeWrongPositionInException() throws Exception {
        long txId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();
        setupLogFiles(
                10,
                logFile(start(txId - 1), commit(txId - 1), position),
                logFile(checkPoint(position)),
                logFile(start(txId), commit(txId)));

        // remove all tx log files
        Path[] matchedFiles = logFiles.getLogFile().getMatchedFiles();
        for (Path matchedFile : matchedFiles) {
            fs.delete(matchedFile);
        }

        // Rebuild to trigger logtail reading
        var e = assertThrows(
                RuntimeException.class,
                () -> LogFilesBuilder.writeableBuilder(
                                databaseLayout,
                                fs,
                                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                                LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                        .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                        .build());
        assertThat(e)
                .rootCause()
                .hasMessageContaining("LogPosition{logVersion=8,")
                .hasMessageContaining("checkpoint does not point to a valid location in transaction logs.");
    }

    @Test
    void notFoundFilesEmptyLogTailLastBatch() {
        LogTailMetadata tailMetadata = logFiles.getTailMetadata();
        assertTrue(tailMetadata.logsMissing());
        assertEquals(EMPTY_APPEND_BATCH_INFO, tailMetadata.lastBatch());
    }

    @ParameterizedTest
    @MethodSource("params")
    void emptyLogFilesButHasHeaderLastBatch(int startLogVersion, int endLogVersion) throws Exception {
        setupLogFiles(endLogVersion, logFile());

        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(endLogVersion, lastBatch.logPositionAfter().getLogVersion());
        assertEquals(
                logFiles.getLogFile()
                        .extractHeader(logFiles.getLogFile().getCurrentLogVersion())
                        .getLastAppendIndex(),
                lastBatch.appendIndex());
    }

    @ParameterizedTest
    @MethodSource("params")
    void emptyLogFilesWithoutHeaderLastBatch(int startLogVersion, int endLogVersion) throws Exception {
        setupLogFiles(endLogVersion, logFile());
        fs.truncate(
                logFiles.getLogFile().getLogFileForVersion(logFiles.getLogFile().getCurrentLogVersion()), 0);

        // Recreate the logfiles since the previous header has been cached
        LogFiles logFiles = createLogFiles();
        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(LogPosition.UNSPECIFIED, lastBatch.logPositionAfter());
        assertEquals(UNKNOWN_APPEND_INDEX, lastBatch.appendIndex());
    }

    @Test
    void lastBatchOfFilesWithoutCheckpoint() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        setupLogFiles(10, logFile(start(appendIndex), commit(appendIndex - 5)));

        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter().getByteOffset()).isGreaterThan(LogFormat.BIGGEST_HEADER);
    }

    @Test
    void lastBatchOfFilesWithoutCheckpointAndSeveralEntries() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        PositionEntry position = position();
        Map<Entry, LogPosition> positions = setupLogFiles(
                10,
                logFile(
                        start(appendIndex),
                        commit(10),
                        start(appendIndex + 1),
                        commit(11),
                        start(appendIndex + 2), // last full batch
                        commit(12),
                        position,
                        start(appendIndex + 3) // incomplete
                        ));

        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex + 2, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter()).isEqualTo(positions.get(position));
    }

    @Test
    void lastBatchOfFilesWithoutCheckpointAndMaxPosition() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        PositionEntry endPosition = position();
        Map<Entry, LogPosition> positions = setupLogFiles(
                10,
                logFile(
                        start(appendIndex),
                        commit(10),
                        start(appendIndex + 1),
                        commit(11),
                        start(appendIndex + 2), // last in range batch
                        commit(12),
                        endPosition,
                        start(appendIndex + 3),
                        commit(13),
                        start(appendIndex + 4),
                        commit(14)));

        this.logFiles = createLogFiles(positions.get(endPosition));
        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex + 2, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter()).isEqualTo(positions.get(endPosition));
    }

    @Test
    void lastBatchOfFilesWithoutCheckpointAndMaxPositionNotInLastFile() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        PositionEntry endPosition = position();
        Map<Entry, LogPosition> positions = setupLogFiles(
                11,
                logFile(
                        start(appendIndex),
                        commit(10),
                        start(appendIndex + 1), // last in range batch
                        commit(11),
                        endPosition,
                        start(appendIndex + 2),
                        commit(12)),
                logFile(start(appendIndex + 3), commit(13), start(appendIndex + 4), commit(14)));

        this.logFiles = createLogFiles(positions.get(endPosition));
        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex + 1, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter()).isEqualTo(positions.get(endPosition));
    }

    @Test
    void lastBatchOfFilesWithMaxPositionInMiddleOfEntry() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        PositionEntry position = position();
        PositionEntry endPosition = position();
        Map<Entry, LogPosition> positions = setupLogFiles(
                11,
                logFile(
                        start(appendIndex),
                        commit(10),
                        start(appendIndex + 1), // last complete in range batch
                        commit(11),
                        position),
                logFile(
                        start(appendIndex + 2),
                        endPosition, // prevent us reading a full entry
                        commit(12),
                        start(appendIndex + 3),
                        commit(13)));

        this.logFiles = createLogFiles(positions.get(endPosition));
        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex + 1, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter()).isEqualTo(positions.get(position));
    }

    @Test
    void lastBatchOfFilesWithCheckpoint() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        setupLogFiles(
                10,
                logFile(start(appendIndex), commit(appendIndex - 5)),
                logFile(checkPoint()),
                logFile(start(appendIndex + 1), commit(appendIndex - 4)));

        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex + 1, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter().getByteOffset()).isGreaterThan(LogFormat.BIGGEST_HEADER);
    }

    @Test
    void lastBatchOfFilesWithIncompleteChunkAtTheEnd() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        setupLogFiles(
                10,
                logFile(start(appendIndex), commit(appendIndex - 5)),
                logFile(checkPoint()),
                logFile(start(appendIndex + 1), commit(appendIndex - 4), start(appendIndex + 2)));

        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertThat(lastBatch.appendIndex()).isEqualTo(appendIndex + 1);
        assertThat(lastBatch.logPositionAfter().getLogVersion()).isEqualTo(10);
        assertThat(lastBatch.logPositionAfter().getByteOffset()).isGreaterThan(LogFormat.BIGGEST_HEADER);
    }

    @Test
    void lastBatchOfFilesWithCheckpointAndMaxPosition() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        PositionEntry endPosition = position();
        Map<Entry, LogPosition> positions = setupLogFiles(
                10,
                logFile(
                        start(appendIndex),
                        commit(10),
                        checkPoint(),
                        start(appendIndex + 1),
                        commit(11),
                        start(appendIndex + 2), // last in range batch
                        commit(12),
                        endPosition,
                        start(appendIndex + 3),
                        commit(13),
                        start(appendIndex + 4),
                        commit(14)));

        this.logFiles = createLogFiles(positions.get(endPosition));
        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex + 2, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter()).isEqualTo(positions.get(endPosition));
    }

    @Test
    void lastBatchOfWithCheckpointAndLastBatchNotInFinalFile() throws Exception {
        long appendIndex = BASE_APPEND_INDEX + 1;
        PositionEntry position = position();
        PositionEntry endPosition = position();
        Map<Entry, LogPosition> positions = setupLogFiles(
                11,
                logFile(
                        start(appendIndex),
                        commit(10),
                        checkPoint(),
                        start(appendIndex + 1), // last complete in range batch
                        commit(11),
                        position),
                logFile(
                        start(appendIndex + 2),
                        endPosition, // prevent us reading a full entry
                        commit(12),
                        start(appendIndex + 3),
                        commit(13)));

        this.logFiles = createLogFiles(positions.get(endPosition));
        LogTailMetadata logTailInformation = logFiles.getTailMetadata();
        AppendBatchInfo lastBatch = logTailInformation.lastBatch();
        assertEquals(appendIndex + 1, lastBatch.appendIndex());
        assertEquals(10, lastBatch.logPositionAfter().getLogVersion());
        assertThat(lastBatch.logPositionAfter()).isEqualTo(positions.get(position));
    }

    @Test
    void detectMissingLogFiles() {
        LogTailMetadata tailInformation = logFiles.getTailMetadata();
        assertTrue(tailInformation.logsMissing());
        assertTrue(tailInformation.isRecoveryRequired());
    }

    @ParameterizedTest
    @MethodSource("params")
    void noLogFilesFound(int startLogVersion, int endLogVersion) throws Exception {
        // given no files
        setupLogFiles(endLogVersion);

        // when
        LogTailMetadata logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, false, UNKNOWN_APPEND_INDEX, true, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void oneLogFileNoCheckPoints(int startLogVersion, int endLogVersion) throws Exception {
        // given
        setupLogFiles(endLogVersion, logFile());

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, false, UNKNOWN_APPEND_INDEX, false, logTailInformation);
        assertFalse(logTailInformation.logsMissing());
    }

    @Test
    void tailShouldContainStoreIdOnNoCheckpointIfFilesExist() throws Exception {
        // given
        setupLogFiles(2, logFile());

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, false, UNKNOWN_APPEND_INDEX, false, logTailInformation);
        assertFalse(logTailInformation.logsMissing());
        assertThat(logTailInformation.getStoreIdentifier()).isPresent();
    }

    @ParameterizedTest
    @MethodSource("params")
    void oneLogFileNoCheckPointsOneStart(int startLogVersion, int endLogVersion) throws Exception {
        // given
        long appendIndexAndCommit = BASE_APPEND_INDEX + 1;
        setupLogFiles(endLogVersion, logFile(start(appendIndexAndCommit), commit(appendIndexAndCommit)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, true, appendIndexAndCommit, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesNoCheckPoints(int startLogVersion, int endLogVersion) throws Exception {
        // given
        setupLogFiles(endLogVersion, logFile(), logFile());

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, false, UNKNOWN_APPEND_INDEX, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesNoCheckPointsOneStart(int startLogVersion, int endLogVersion) throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 1;
        setupLogFiles(endLogVersion, logFile(), logFile(start(txId), commit(txId)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesNoCheckPointsOneStartWithoutCommit(int startLogVersion, int endLogVersion) throws Exception {
        // given
        setupLogFiles(endLogVersion, logFile(), logFile(start(BASE_APPEND_INDEX + 1), pseudoEndSegment()));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, true, BASE_APPEND_INDEX + 1, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesNoCheckPointsTwoCommits(int startLogVersion, int endLogVersion) throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 1;
        setupLogFiles(endLogVersion, logFile(), logFile(start(txId), commit(txId), start(txId + 1), commit(txId + 1)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesCheckPointTargetsPrevious(int startLogVersion, int endLogVersion) throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();
        setupLogFiles(
                endLogVersion,
                logFile(start(txId - 1), commit(txId - 1), position),
                logFile(start(txId), commit(txId)),
                logFile(checkPoint(position)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesCheckPointTargetsPreviousMaxPositionAtCheckpoint(int startLogVersion, int endLogVersion)
            throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();

        Map<Entry, LogPosition> positions = new HashMap<>();
        logFile(start(txId - 1), commit(txId - 1), position).create(startLogVersion, positions);
        logFile(start(txId), commit(txId)).create(startLogVersion + 1, positions);
        logFile(checkPoint(position)).create(startLogVersion + 1, positions);

        this.logFiles = createLogFiles(positions.get(position));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, false, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesCheckPointTargetsPreviousMaxPositionAfterCheckpoint(int startLogVersion, int endLogVersion)
            throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();
        PositionEntry position2 = position();

        Map<Entry, LogPosition> positions = new HashMap<>();
        logFile(start(txId - 1), commit(txId - 1), position).create(startLogVersion, positions);
        logFile(checkPoint(position)).create(startLogVersion + 1, positions);
        logFile(start(txId), commit(txId), position2, start(txId + 1), commit(txId + 1))
                .create(startLogVersion + 1, positions);

        this.logFiles = createLogFiles(positions.get(position2));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesNoCheckPointMaxPosition(int startLogVersion, int endLogVersion) throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();

        Map<Entry, LogPosition> positions = new HashMap<>();
        logFile(start(txId - 1), commit(txId - 1), position).create(startLogVersion, positions);
        logFile(start(txId), commit(txId)).create(startLogVersion + 1, positions);

        this.logFiles = createLogFiles(positions.get(position));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, true, txId - 1, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesNoCheckPointMaxPositionBeforeFirstAvailablePos(int startLogVersion, int endLogVersion)
            throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();

        Map<Entry, LogPosition> positions = new HashMap<>();
        logFile(start(txId - 1), commit(txId - 1), position).create(startLogVersion, positions);
        logFile(start(txId), commit(txId)).create(startLogVersion + 1, positions);

        // then
        assertThatThrownBy(() -> createLogFiles(new LogPosition(0, 10)))
                .hasMessageContaining("earlier than start position");
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesStartAndCommitInDifferentFiles(int startLogVersion, int endLogVersion) throws Exception {
        // Hard to fake the required rotation in enveloped logs, but this case is supported there too (much more likely
        // to have a split in the middle of an entry though)
        assumeTrue(LATEST_KERNEL_VERSION.isLessThan(KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED));
        // given
        long txId = BASE_APPEND_INDEX + 1;
        setupLogFiles(endLogVersion, logFile(start(txId)), logFile(commit(txId)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(false, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void latestLogFileContainingACheckPointOnly(int startLogVersion, int endLogVersion) throws Exception {
        // given
        setupLogFiles(endLogVersion, logFile(checkPoint()));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, false, NO_TRANSACTION_ID, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void latestLogFileContainingACheckPointAndAStartBefore(int startLogVersion, int endLogVersion) throws Exception {
        // given
        setupLogFiles(endLogVersion, logFile(start(2), commit(2), checkPoint()));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, false, NO_TRANSACTION_ID, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesSecondIsCorruptedBeforeCommit(int startLogVersion, int endLogVersion) throws Exception {
        setupLogFiles(endLogVersion, logFile(checkPoint()), logFile(start(2), pseudoEndSegment(), start(3)));

        Path highestLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
        fs.truncate(highestLogFile, fs.getFileSize(highestLogFile) - 1);

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        // Enveloped logs are less forgiving to corruption and will "not find" the append index
        long expectedFirstAppendIndexAfterCheckpoint =
                LATEST_KERNEL_VERSION.isAtLeast(KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED) ? 0 : 2;
        assertLatestCheckPoint(true, true, expectedFirstAppendIndexAfterCheckpoint, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void twoLogFilesSecondIsCorruptedBeforeAfterCommit(int startLogVersion, int endLogVersion) throws Exception {
        int firstTxId = 2;
        setupLogFiles(
                endLogVersion,
                logFile(checkPoint()),
                logFile(start(firstTxId), commit(firstTxId), start(3), commit(3)));

        Path highestLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
        fs.truncate(highestLogFile, fs.getFileSize(highestLogFile) - 3);

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, firstTxId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void latestLogFileContainingACheckPointAndAStartAfter(int startLogVersion, int endLogVersion) throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 1;
        StartEntry start = start(txId);
        setupLogFiles(endLogVersion, logFile(start, commit(txId), checkPoint(start)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void latestLogFileContainingMultipleCheckPointsOneStartInBetween(int startLogVersion, int endLogVersion)
            throws Exception {
        // given
        setupLogFiles(endLogVersion, logFile(checkPoint(), start(2), commit(2), checkPoint()));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, false, NO_TRANSACTION_ID, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void latestLogFileContainingMultipleCheckPointsOneStartAfterBoth(int startLogVersion, int endLogVersion)
            throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 1;
        setupLogFiles(endLogVersion, logFile(checkPoint(), checkPoint(), start(txId), commit(txId)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void olderLogFileContainingACheckPointAndNewerFileContainingAStart(int startLogVersion, int endLogVersion)
            throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 1;
        StartEntry start = start(txId);
        setupLogFiles(endLogVersion, logFile(checkPoint()), logFile(start, commit(txId)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void olderLogFileContainingACheckPointAndNewerFileIsEmpty(int startLogVersion, int endLogVersion) throws Exception {
        // given
        StartEntry start = start(2);
        setupLogFiles(endLogVersion, logFile(start, commit(2), checkPoint()), logFile());

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, false, NO_TRANSACTION_ID, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToAPreviousPositionThanStart(
            int startLogVersion, int endLogVersion) throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 1;
        StartEntry start = start(txId);
        setupLogFiles(endLogVersion, logFile(start, commit(txId)), logFile(checkPoint(start)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToALaterPositionThanStart(
            int startLogVersion, int endLogVersion) throws Exception {
        // given
        PositionEntry position = position();
        setupLogFiles(endLogVersion, logFile(start(2), commit(2), position), logFile(checkPoint(position)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, false, NO_TRANSACTION_ID, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void latestLogEmptyStartEntryBeforeAndAfterCheckPointInTheLastButOneLog(int startLogVersion, int endLogVersion)
            throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 2;
        setupLogFiles(endLogVersion, logFile(start(2), commit(2), checkPoint(), start(txId), commit(txId)), logFile());

        // when
        var logTailInformation = logFiles.getTailMetadata();

        // then
        assertLatestCheckPoint(true, true, txId, false, logTailInformation);
    }

    @ParameterizedTest
    @MethodSource("params")
    void printProgress(long startLogVersion, long endLogVersion) throws Exception {
        // given
        long txId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();
        setupLogFiles(
                endLogVersion,
                logFile(start(txId - 1), commit(txId - 1), position),
                logFile(checkPoint(position)),
                logFile(start(txId), commit(txId)));

        // when
        logFiles.getTailMetadata();

        // then
        String message = "Scanning log file with version %d for checkpoint entries";
        assertThat(logProvider).forLevel(INFO).containsMessageWithArguments(message, endLogVersion);
        assertThat(logProvider).forLevel(INFO).containsMessageWithArguments(message, startLogVersion);
    }

    @Test
    void extractTxIdFromFirstChunkEndOnEmptyLogs() throws Exception {
        long chunkTxId = BASE_APPEND_INDEX + 1;
        setupLogFiles(10, logFile(start(chunkTxId), chunkEnd(chunkTxId)), logFile());

        LogTailMetadata tailMetadata = logFiles.getTailMetadata();
        assertEquals(chunkTxId, ((LogTailInformation) tailMetadata).firstAppendIndexAfterLastCheckPoint);
    }

    @Test
    void extractTxIdFromFirstChunkEndOnNotEmptyLogs() throws Exception {
        long chunkTxId = BASE_APPEND_INDEX + 2;
        PositionEntry position = position();
        setupLogFiles(
                11,
                logFile(start(chunkTxId - 1), commit(chunkTxId - 1), position),
                logFile(checkPoint(position)),
                logFile(start(chunkTxId), chunkEnd(chunkTxId)));

        LogTailMetadata tailMetadata = logFiles.getTailMetadata();
        assertEquals(chunkTxId, ((LogTailInformation) tailMetadata).firstAppendIndexAfterLastCheckPoint);
    }

    @Test
    void parseConsensusIndexFromCheckpoint() throws Exception {
        // given
        long transactionId = 4;
        long consensusIndex = 999;
        logFiles = createLogFiles(KernelVersion.V5_7);
        setupLogFiles(
                13,
                logFile(
                        KernelVersion.V5_7,
                        start(transactionId - 2),
                        commit(transactionId - 2),
                        start(transactionId - 1),
                        commit(transactionId - 1),
                        start(transactionId),
                        commit(transactionId),
                        checkPoint(new TransactionId(
                                transactionId,
                                transactionId,
                                LATEST_KERNEL_VERSION,
                                BASE_TX_CHECKSUM,
                                BASE_TX_COMMIT_TIMESTAMP,
                                consensusIndex))));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        assertThat(logTailInformation
                        .getLastCheckPoint()
                        .orElseThrow()
                        .transactionId()
                        .consensusIndex())
                .isEqualTo(consensusIndex);
    }

    @Test
    void doNotParseConsensusIndexFromTransactionHeaderFor57PlusCheckpoint() throws Exception {
        // given
        long transactionId = 4;
        logFiles = createLogFiles(KernelVersion.V5_7);
        setupLogFiles(
                13,
                logFile(
                        KernelVersion.V5_7,
                        start(666),
                        commit(transactionId - 2),
                        start(666),
                        commit(transactionId - 1),
                        start(999),
                        commit(transactionId),
                        checkPoint(new TransactionId(
                                transactionId,
                                transactionId,
                                LATEST_KERNEL_VERSION,
                                BASE_TX_CHECKSUM,
                                BASE_TX_COMMIT_TIMESTAMP,
                                UNKNOWN_CONSENSUS_INDEX))));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        assertThat(logTailInformation
                        .getLastCheckPoint()
                        .orElseThrow()
                        .transactionId()
                        .consensusIndex())
                .isEqualTo(UNKNOWN_CONSENSUS_INDEX);
    }

    @Test
    void parseConsensusIndexFromTransactionHeaderFor50Checkpoint() throws Exception {
        // given
        long transactionId = 4;
        long consensusIndex = 999;
        logFiles = createLogFiles(KernelVersion.V5_0);
        setupLogFiles(
                13,
                logFile(
                        KernelVersion.V5_0,
                        start(transactionId - 2, 666),
                        commit(transactionId - 2),
                        start(transactionId - 1, 666),
                        commit(transactionId - 1),
                        start(transactionId, consensusIndex),
                        commit(transactionId),
                        checkPoint(new TransactionId(
                                transactionId,
                                transactionId,
                                LATEST_KERNEL_VERSION,
                                BASE_TX_CHECKSUM,
                                BASE_TX_COMMIT_TIMESTAMP,
                                UNKNOWN_CONSENSUS_INDEX))));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        assertThat(logTailInformation
                        .getLastCheckPoint()
                        .orElseThrow()
                        .transactionId()
                        .consensusIndex())
                .isEqualTo(consensusIndex);
    }

    @Test
    void parseConsensusIndexFromTransactionHeaderWhenInPreviousLog() throws Exception {
        // given
        long transactionId = 7;
        long consensusIndex = 999;
        logFiles = createLogFiles(KernelVersion.V5_0);
        setupLogFiles(
                13,
                logFile(
                        KernelVersion.V5_0,
                        start(transactionId - 1, 555),
                        commit(transactionId - 1),
                        start(transactionId, consensusIndex),
                        commit(transactionId)),
                logFile(
                        KernelVersion.V5_0,
                        checkPoint(new TransactionId(
                                transactionId,
                                transactionId,
                                LATEST_KERNEL_VERSION,
                                BASE_TX_CHECKSUM,
                                BASE_TX_COMMIT_TIMESTAMP,
                                UNKNOWN_CONSENSUS_INDEX))));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        assertThat(logTailInformation
                        .getLastCheckPoint()
                        .orElseThrow()
                        .transactionId()
                        .consensusIndex())
                .isEqualTo(consensusIndex);
    }

    @Test
    void parseConsensusIndexFromTransactionHeaderWhenHasMoreTransactionsAfterCheckpoint() throws Exception {
        // given
        long transactionId = 1;
        long consensusIndex = 999;
        logFiles = createLogFiles(KernelVersion.V5_0);
        setupLogFiles(
                13,
                logFile(
                        KernelVersion.V5_0,
                        start(transactionId, consensusIndex),
                        commit(transactionId),
                        checkPoint(new TransactionId(
                                transactionId,
                                transactionId,
                                LATEST_KERNEL_VERSION,
                                BASE_TX_CHECKSUM,
                                BASE_TX_COMMIT_TIMESTAMP,
                                UNKNOWN_CONSENSUS_INDEX)),
                        start(1001),
                        commit(transactionId + 1)));

        // when
        var logTailInformation = logFiles.getTailMetadata();

        assertThat(logTailInformation
                        .getLastCheckPoint()
                        .orElseThrow()
                        .transactionId()
                        .consensusIndex())
                .isEqualTo(consensusIndex);
    }

    // === Below is code for helping the tests above ===

    Map<Entry, LogPosition> setupLogFiles(long endLogVersion, LogCreator... logFiles) throws Exception {
        Map<Entry, LogPosition> positions = new HashMap<>();
        long version = endLogVersion - logFiles.length;
        for (LogCreator logFile : logFiles) {
            logFile.create(++version, positions);
        }

        this.logFiles = createLogFiles();
        return positions;
    }

    LogCreator logFile(Entry... entries) {
        return logFile(LATEST_KERNEL_VERSION, entries);
    }

    LogCreator logFile(KernelVersion kernelVersion, Entry... entries) {
        return (logVersion, positions) -> {
            try {
                AtomicLong lastTxId = new AtomicLong();
                logVersionRepository.setCurrentLogVersion(logVersion);
                logVersionRepository.setCheckpointLogVersion(logVersion);
                LifeSupport logFileLife = new LifeSupport();
                logFileLife.start();
                logFileLife.add(logFiles);
                LogFile logFile = logFiles.getLogFile();
                var checkpointFile = logFiles.getCheckpointFile();
                int previousChecksum = BASE_TX_CHECKSUM;
                try {
                    TransactionLogWriter logWriter = logFile.getTransactionLogWriter();
                    LogEntryWriter<?> writer = logWriter.getWriter();
                    LogPosition lastCommitEntryPosition = logWriter.getCurrentPosition();
                    for (Entry entry : entries) {
                        positions.put(entry, lastCommitEntryPosition);
                        switch (entry) {
                            case StartEntry startEntry -> {
                                writer.writeStartEntry(
                                        kernelVersion,
                                        0,
                                        0,
                                        startEntry.appendIndex(),
                                        UNKNOWN_TX_SEQUENCE_NUMBER,
                                        previousChecksum,
                                        startEntry.additionalHeader());
                                appendIndexProvider.setAppendIndex(startEntry.appendIndex());
                            }
                            case CommitEntry commitEntry -> {
                                previousChecksum = writer.writeCommitEntry(kernelVersion, commitEntry.txId, 0);
                                lastCommitEntryPosition = logWriter.getCurrentPosition();
                                lastTxId.set(commitEntry.txId);
                            }
                            case ChunkEndEntry chunkEntry ->
                                previousChecksum = writer.writeChunkEndEntry(kernelVersion, chunkEntry.txId, 1);
                            case CheckPointEntry checkPointEntry -> {
                                Entry target = checkPointEntry.withPositionOfEntry;
                                LogPosition logPosition =
                                        target != null ? positions.get(target) : lastCommitEntryPosition;
                                assert logPosition != null : "No registered log position for " + target;
                                writeCheckpoint(
                                        checkpointFile, checkPointEntry.transactionId(), logPosition, kernelVersion);
                            }
                            case PositionEntry ignore -> {
                                // we don't write anything for registering positions for other CheckPointEntry
                            }
                            case PseudoEndSegmentEntry ignored ->
                                writer.getChannel().putChecksum();
                        }
                    }
                } finally {
                    logFileLife.shutdown();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    LogFiles createLogFiles() throws IOException {
        return createLogFiles(LATEST_KERNEL_VERSION);
    }

    LogFiles createLogFiles(LogPosition maxPosition) throws IOException {
        return createLogFiles(LATEST_KERNEL_VERSION, maxPosition);
    }

    LogFiles createLogFiles(KernelVersion kernelVersion) throws IOException {
        return createLogFiles(kernelVersion, LogPosition.UNSPECIFIED);
    }

    LogFiles createLogFiles(KernelVersion kernelVersion, LogPosition maxPosition) throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout, fs, fixed(kernelVersion), () -> LogFormat.fromKernelVersion(kernelVersion))
                .withLogVersionRepository(logVersionRepository)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withLogProvider(logProvider)
                .withConfig(Config.defaults(fail_on_corrupted_log_files, false))
                .withTailReadingMaxPosition(maxPosition)
                .build();
    }

    void writeCheckpoint(
            CheckpointFile separateCheckpointFile,
            TransactionId transactionId,
            LogPosition logPosition,
            KernelVersion kernelVersion)
            throws IOException {
        separateCheckpointFile
                .getCheckpointAppender()
                .checkPoint(
                        LogCheckPointEvent.NULL,
                        transactionId,
                        transactionId.appendIndex(),
                        kernelVersion,
                        logPosition,
                        logPosition,
                        Instant.now(),
                        "test");
    }

    @FunctionalInterface
    interface LogCreator {
        void create(long version, Map<Entry, LogPosition> positions);
    }

    // Marker interface, helping compilation/test creation
    sealed interface Entry {}

    private record StartEntry(long appendIndex, byte[] additionalHeader) implements Entry {}

    private record CommitEntry(long txId) implements Entry {}

    private record ChunkEndEntry(long txId) implements Entry {}

    private record CheckPointEntry(Entry withPositionOfEntry, TransactionId transactionId) implements Entry {}

    // Use class to ensure reference equality when we record several positions
    private static final class PositionEntry implements Entry {}

    private record PseudoEndSegmentEntry() implements Entry {}

    private static StartEntry start(long appendIndex) {
        return new StartEntry(appendIndex, EMPTY_BYTE_ARRAY);
    }

    private static StartEntry start(long appendIndex, long consensusIndex) {
        return new StartEntry(appendIndex, LogIndexEncoding.encodeLogIndex(consensusIndex));
    }

    private static CommitEntry commit(long txId) {
        return new CommitEntry(txId);
    }

    private static ChunkEndEntry chunkEnd(long txId) {
        return new ChunkEndEntry(txId);
    }

    private static CheckPointEntry checkPoint() {
        return checkPoint(null /*means self-position*/, UNKNOWN_TRANSACTION_ID);
    }

    private static CheckPointEntry checkPoint(Entry forEntry) {
        return checkPoint(forEntry, UNKNOWN_TRANSACTION_ID);
    }

    private static CheckPointEntry checkPoint(TransactionId transactionId) {
        return checkPoint(null, transactionId);
    }

    private static CheckPointEntry checkPoint(Entry forEntry, TransactionId transactionId) {
        return new CheckPointEntry(forEntry, transactionId);
    }

    private static PositionEntry position() {
        return new PositionEntry();
    }

    /**
     * Force flushing of enveloped data.
     */
    static PseudoEndSegmentEntry pseudoEndSegment() {
        return new PseudoEndSegmentEntry();
    }

    private static void assertLatestCheckPoint(
            boolean hasCheckPointEntry,
            boolean commitsAfterLastCheckPoint,
            long firstAppendIndexAfterLastCheckPoint,
            boolean filesNotFound,
            LogTailMetadata logTailInformation) {
        var tail = (LogTailInformation) logTailInformation;
        assertEquals(hasCheckPointEntry, tail.lastCheckPoint != null);
        assertEquals(commitsAfterLastCheckPoint, tail.hasRecordsToRecover());
        if (commitsAfterLastCheckPoint) {
            assertEquals(firstAppendIndexAfterLastCheckPoint, tail.firstAppendIndexAfterLastCheckPoint);
        }
        assertEquals(filesNotFound, tail.filesNotFound);
    }
}
