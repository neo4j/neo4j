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
package org.neo4j.kernel.impl.transaction.log.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.kernel.KernelVersion.DEFAULT_BOOTSTRAP_VERSION;
import static org.neo4j.kernel.KernelVersion.GLORIOUS_FUTURE;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.mockito.stubbing.Answer;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.internal.nativeimpl.ErrorTranslator;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.AppendedChunkLogVersionLocator;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.arguments.KernelVersionSource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.Futures;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class TransactionLogFileTest {

    private static final StoreId STORE_ID = new StoreId(1, 2, "engine-1", "format-1", 3, 4);

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    private CapturingChannelFileSystem wrappingFileSystem;

    private LogFileVersionTracker logFileVersionTracker;

    private final long rotationThreshold = ByteUnit.mebiBytes(1);
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository(1L);
    private final SimpleAppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore(
            2L, 3L, DEFAULT_BOOTSTRAP_VERSION, 0, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX, 0, 0);

    @BeforeEach
    void setUp() {
        wrappingFileSystem = new CapturingChannelFileSystem(fileSystem);
        appendIndexProvider.setAppendIndex(2L);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    @EnabledOnOs(OS.LINUX)
    void truncateCurrentLogFile(KernelVersion kernelVersion) throws IOException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.add(logFiles);
        life.start();

        LogFile logFile = logFiles.getLogFile();
        long sizeBefore = fileSystem.getFileSize(logFile.getLogFileForVersion(logFile.getCurrentLogVersion()));

        logFile.truncate();

        long sizeAfter = fileSystem.getFileSize(logFile.getLogFileForVersion(logFile.getCurrentLogVersion()));

        assertThat(sizeBefore)
                .describedAs("Truncation should truncate any preallocated space.")
                .isGreaterThan(sizeAfter);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void skipLogFileWithoutHeader(KernelVersion kernelVersion) throws IOException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.add(logFiles);
        life.start();

        // set append index to be the same as for our test transaction
        appendIndexProvider.setAppendIndex(6L);
        // simulate new file without header presence
        logVersionRepository.incrementAndGetVersion();
        fileSystem
                .write(logFiles.getLogFile().getLogFileForVersion(logVersionRepository.getCurrentLogVersion()))
                .close();
        transactionIdStore.transactionCommitted(5L, 6L, DEFAULT_BOOTSTRAP_VERSION, 5, 5L, 6L);

        var versionLocator = new AppendedChunkLogVersionLocator(4L);
        logFiles.getLogFile().accept(versionLocator);

        LogPosition logPosition = versionLocator.getLogPositionOrThrow();
        assertEquals(1, logPosition.getLogVersion());
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void preAllocateOnStartAndEvictOnShutdownNewLogFile(KernelVersion kernelVersion) throws IOException {
        final CapturingNativeAccess capturingNativeAccess = new CapturingNativeAccess();
        LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        fixed(kernelVersion),
                        () -> LogFormat.fromKernelVersion(kernelVersion))
                .withTransactionIdStore(transactionIdStore)
                .withLogVersionRepository(logVersionRepository)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(STORE_ID)
                .withDependencies(dependenciesOf(capturingNativeAccess))
                .build();

        startStop(capturingNativeAccess, life, kernelVersion);

        assertEquals(2, capturingNativeAccess.getPreallocateCounter());
        assertEquals(2, capturingNativeAccess.getEvictionCounter());
        assertEquals(0, capturingNativeAccess.getAdviseCounter());
        assertEquals(0, capturingNativeAccess.getKeepCounter());
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void adviseOnStartAndEvictOnShutdownExistingLogFile(KernelVersion kernelVersion) throws IOException {
        var capturingNativeAccess = new CapturingNativeAccess();

        startStop(capturingNativeAccess, life, kernelVersion);
        capturingNativeAccess.reset();

        startStop(capturingNativeAccess, new LifeSupport(), kernelVersion);

        assertEquals(0, capturingNativeAccess.getPreallocateCounter());
        assertEquals(6, capturingNativeAccess.getEvictionCounter());
        assertEquals(6, capturingNativeAccess.getAdviseCounter());
        assertEquals(6, capturingNativeAccess.getKeepCounter());
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldOpenInFreshDirectoryAndFinallyAddHeader(KernelVersion kernelVersion) throws Exception {
        // GIVEN
        LogFiles logFiles = buildLogFiles(kernelVersion);

        // WHEN
        life.start();
        life.add(logFiles);
        life.shutdown();

        // THEN
        Path file = LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout.getTransactionLogsDirectory(), fileSystem)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .build()
                .getLogFile()
                .getLogFileForVersion(1L);
        LogHeader header = readLogHeader(fileSystem, file, INSTANCE);
        assertEquals(1L, header.getLogVersion());
        assertEquals(2L, header.getLastAppendIndex());
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldWriteSomeDataIntoTheLog(KernelVersion kernelVersion) throws Exception {
        // GIVEN
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        // WHEN
        LogFile logFile = logFiles.getLogFile();
        TransactionLogWriter transactionLogWriter = logFile.getTransactionLogWriter();
        var channel = transactionLogWriter.getChannel();
        LogPosition currentPosition = transactionLogWriter.getCurrentPosition();
        int intValue = 45;
        long longValue = 4854587;
        channel.beginChecksumForWriting();
        channel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        channel.putVersion(kernelVersion.version());
        channel.putInt(intValue);
        channel.putLong(longValue);
        channel.putChecksum();
        logFile.flush();

        // THEN
        try (ReadableChannel reader = logFile.getReader(currentPosition)) {
            assertEquals(kernelVersion.version(), reader.getVersion());
            assertEquals(intValue, reader.getInt());
            assertEquals(longValue, reader.getLong());
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldReadOlderLogs(KernelVersion kernelVersion) throws Exception {
        // GIVEN
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        // WHEN
        LogFile logFile = logFiles.getLogFile();
        TransactionLogWriter logWriter = logFile.getTransactionLogWriter();
        var writer = logWriter.getChannel();
        LogPosition position1 = logWriter.getCurrentPosition();
        int intValue = 45;
        long longValue = 4854587;
        byte[] someBytes1 = someBytes(42);
        byte[] someBytes2 = someBytes(69);
        writer.beginChecksumForWriting();
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.putVersion(kernelVersion.version());
        writer.putInt(intValue);
        writer.putLong(longValue);
        writer.put(someBytes1, someBytes1.length);
        int checksum1 = writer.putChecksum();
        logFile.flush();
        LogPosition position2 = logWriter.getCurrentPosition();
        writer.beginChecksumForWriting();
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.putVersion(kernelVersion.version());
        long longValue2 = 123456789L;
        writer.putLong(longValue2);
        writer.put(someBytes2, someBytes2.length);
        int checksum2 = writer.putChecksum();
        logFile.flush();

        // THEN
        try (ReadableChannel reader = logFile.getReader(position1)) {
            assertEquals(kernelVersion.version(), reader.getVersion());
            assertEquals(intValue, reader.getInt());
            assertEquals(longValue, reader.getLong());
            assertArrayEquals(someBytes1, readBytes(reader, someBytes1.length));
            assertEquals(checksum1, reader.getChecksum());
        }
        try (ReadableChannel reader = logFile.getReader(position2)) {
            assertEquals(kernelVersion.version(), reader.getVersion());
            assertEquals(longValue2, reader.getLong());
            assertArrayEquals(someBytes2, readBytes(reader, someBytes2.length));
            assertEquals(checksum2, reader.getChecksum());
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldVisitLogFile(KernelVersion kernelVersion) throws Exception {
        // GIVEN
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        LogFile logFile = logFiles.getLogFile();
        var transactionLogWriter = logFile.getTransactionLogWriter();
        var writer = transactionLogWriter.getChannel();
        LogPosition position = transactionLogWriter.getCurrentPosition();
        writer.beginChecksumForWriting();
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.putVersion(kernelVersion.version());
        for (int i = 0; i < 5; i++) {
            writer.put((byte) i);
        }
        writer.putChecksum();
        logFile.flush();

        // WHEN/THEN
        final AtomicBoolean called = new AtomicBoolean();
        logFile.accept(
                channel -> {
                    assertEquals(kernelVersion.version(), channel.getVersion());
                    for (int i = 0; i < 5; i++) {
                        assertEquals((byte) i, channel.get());
                    }
                    called.set(true);
                    return true;
                },
                position);
        assertTrue(called.get());
    }

    @Test
    void shouldCloseChannelInFailedAttemptToReadHeaderAfterOpen() throws Exception {
        // GIVEN a file which returns 1/2 log header size worth of bytes
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout.getTransactionLogsDirectory(), fs)
                .withTransactionIdStore(transactionIdStore)
                .withLogVersionRepository(logVersionRepository)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .build();
        int logVersion = 0;
        Path logFile = logFiles.getLogFile().getLogFileForVersion(logVersion);
        StoreChannel channel = mock(StoreChannel.class);
        when(channel.read(any(ByteBuffer.class))).thenAnswer((Answer<Integer>) invocation -> {
            Object[] args = invocation.getArguments();
            ((ByteBuffer) args[0]).put(new byte[] {1, 2, 3, 4});
            return 4;
        });
        when(fs.fileExists(logFile)).thenReturn(true);
        when(fs.read(logFile)).thenReturn(channel);

        // WHEN
        final var exception = assertThrows(
                IncompleteLogHeaderException.class, () -> logFiles.getLogFile().openForVersion(logVersion));
        verify(channel).close();
        assertEquals(0, exception.getSuppressed().length);
    }

    @Test
    void shouldSuppressFailureToCloseChannelInFailedAttemptToReadHeaderAfterOpen() throws Exception {
        // GIVEN a file which returns 1/2 log header size worth of bytes
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout.getTransactionLogsDirectory(), fs)
                .withTransactionIdStore(transactionIdStore)
                .withLogVersionRepository(logVersionRepository)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .build();
        int logVersion = 0;
        Path logFile = logFiles.getLogFile().getLogFileForVersion(logVersion);
        StoreChannel channel = mock(StoreChannel.class);
        when(channel.read(any(ByteBuffer.class))).thenAnswer((Answer<Integer>) invocation -> {
            Object[] args = invocation.getArguments();
            ((ByteBuffer) args[0]).put(new byte[] {1, 2, 3, 4});
            return 4;
        });
        when(fs.fileExists(logFile)).thenReturn(true);
        when(fs.read(logFile)).thenReturn(channel);
        doThrow(IOException.class).when(channel).close();

        // WHEN
        final var exception = assertThrows(
                IncompleteLogHeaderException.class, () -> logFiles.getLogFile().openForVersion(logVersion));
        verify(channel).close();
        assertEquals(1, exception.getSuppressed().length);
        assertInstanceOf(IOException.class, exception.getSuppressed()[0]);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void closeChannelThrowExceptionOnAttemptToAppendTransactionLogRecords(KernelVersion kernelVersion)
            throws IOException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        LogFile logFile = logFiles.getLogFile();
        var channel = logFile.getTransactionLogWriter().getChannel();

        life.shutdown();

        assertThrows(Throwable.class, () -> channel.put((byte) 7));
        assertThrows(Throwable.class, () -> channel.putInt(7));
        assertThrows(Throwable.class, () -> channel.putLong(7));
        assertThrows(Throwable.class, () -> channel.putDouble(7));
        assertThrows(Throwable.class, () -> channel.putFloat(7));
        assertThrows(Throwable.class, () -> channel.putShort((short) 7));
        assertThrows(Throwable.class, () -> channel.put(new byte[] {1, 2, 3}, 3));
        assertThrows(Throwable.class, logFile::flush);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldForceLogChannel(KernelVersion kernelVersion) throws Throwable {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        LogFile logFile = logFiles.getLogFile();
        var capturingChannel = wrappingFileSystem.getCapturingChannel();

        var flushesBefore = capturingChannel.getFlushCounter().get();
        var writesBefore = capturingChannel.getWriteAllCounter().get();

        logFile.locklessForce(LogAppendEvent.NULL);

        assertEquals(1, capturingChannel.getFlushCounter().get() - flushesBefore);
        assertEquals(0, capturingChannel.getWriteAllCounter().get() - writesBefore);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void combineLogFilesFromMultipleLocationsNonOverlappingFiles(KernelVersion kernelVersion) throws IOException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        Path additionalSource = testDirectory.directory("another");
        createFile(additionalSource, 2, kernelVersion);
        createFile(additionalSource, 3, kernelVersion);
        createFile(additionalSource, 4, kernelVersion);

        LogFile logFile = logFiles.getLogFile();
        assertEquals(1, logFile.getLogRangeInfo().highestVersion());

        logFile.combine(additionalSource, AppendIndexProvider.UNKNOWN_APPEND_INDEX);
        assertEquals(4, logFile.getLogRangeInfo().highestVersion());
        assertThat(Arrays.stream(logFile.getMatchedFiles())
                        .map(path -> path.getFileName().toString()))
                .contains(
                        "neostore.transaction.db.1",
                        "neostore.transaction.db.2",
                        "neostore.transaction.db.3",
                        "neostore.transaction.db.4");
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void combineLogFilesFromMultipleLocationsOverlappingFiles(KernelVersion kernelVersion) throws IOException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        Path additionalSource = testDirectory.directory("another");
        createFile(additionalSource, 0, kernelVersion);
        createFile(additionalSource, 1, kernelVersion);
        createFile(additionalSource, 2, kernelVersion);

        LogFile logFile = logFiles.getLogFile();
        assertEquals(1, logFile.getLogRangeInfo().highestVersion());

        logFile.combine(additionalSource, AppendIndexProvider.UNKNOWN_APPEND_INDEX);
        assertEquals(4, logFile.getLogRangeInfo().highestVersion());
        assertThat(Arrays.stream(logFile.getMatchedFiles())
                        .map(path -> path.getFileName().toString()))
                .contains(
                        "neostore.transaction.db.1",
                        "neostore.transaction.db.2",
                        "neostore.transaction.db.3",
                        "neostore.transaction.db.4");
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void combineShouldPreserveOrder(KernelVersion kernelVersion) throws IOException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        Path additionalSource = testDirectory.directory("another");

        int numberOfAdditionalFile = 20;

        for (int i = 0; i < numberOfAdditionalFile; i++) {
            createFile(additionalSource, i, i, kernelVersion);
        }

        LogFile logFile = logFiles.getLogFile();
        assertEquals(1, logFile.getLogRangeInfo().highestVersion());

        logFile.combine(additionalSource, AppendIndexProvider.UNKNOWN_APPEND_INDEX);

        assertEquals(numberOfAdditionalFile + 1, logFile.getLogRangeInfo().highestVersion());

        for (int i = 2; i < numberOfAdditionalFile + 2; i++) {
            int expectedCommitIdx = i - 2;
            LogHeader header = readLogHeader(fileSystem, logFile.getLogFileForVersion(i), INSTANCE);
            Long lastAppendIndex = header.getLastAppendIndex();
            assertThat(lastAppendIndex)
                    .withFailMessage(
                            "File %s should have commit idx %s instead of %s",
                            logFile.getLogFileForVersion(i), expectedCommitIdx, lastAppendIndex)
                    .isEqualTo(expectedCommitIdx);
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void combineLogFilesFromMultipleLocationsNonSequentialFiles(KernelVersion kernelVersion) throws IOException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        Path additionalSource1 = testDirectory.directory("another");
        createFile(additionalSource1, 0, kernelVersion);
        createFile(additionalSource1, 6, kernelVersion);
        createFile(additionalSource1, 8, kernelVersion);

        Path additionalSource2 = testDirectory.directory("another2");
        createFile(additionalSource2, 10, kernelVersion);
        createFile(additionalSource2, 26, kernelVersion);
        createFile(additionalSource2, 38, kernelVersion);

        LogFile logFile = logFiles.getLogFile();
        assertEquals(1, logFile.getLogRangeInfo().highestVersion());

        logFile.combine(additionalSource1, AppendIndexProvider.UNKNOWN_APPEND_INDEX);
        logFile.combine(additionalSource2, AppendIndexProvider.UNKNOWN_APPEND_INDEX);

        assertEquals(7, logFile.getLogRangeInfo().highestVersion());
        assertThat(Arrays.stream(logFile.getMatchedFiles())
                        .map(path -> path.getFileName().toString()))
                .contains(
                        "neostore.transaction.db.1",
                        "neostore.transaction.db.2",
                        "neostore.transaction.db.3",
                        "neostore.transaction.db.4",
                        "neostore.transaction.db.5",
                        "neostore.transaction.db.6",
                        "neostore.transaction.db.7");
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void logFilesExternalReadersRegistration(KernelVersion kernelVersion) throws IOException, ExecutionException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        LogFile logFile = logFiles.getLogFile();
        logFile.rotate();
        logFile.rotate();
        logFile.rotate();

        assertEquals(4, logFile.getLogRangeInfo().highestVersion());

        var channelMap = LongObjectMaps.mutable.<StoreChannel>empty();
        channelMap.put(1, logFile.openForVersion(1));
        channelMap.put(2, logFile.openForVersion(2));
        channelMap.put(3, logFile.openForVersion(3));

        ArrayList<Future<?>> futures = new ArrayList<>(10);
        try (ExecutorService registerCalls = Executors.newFixedThreadPool(5)) {
            for (int i = 0; i < 10; i++) {
                futures.add(registerCalls.submit(() -> logFile.registerExternalReaders(channelMap)));
            }
            Futures.getAll(futures);
        }

        var externalFileReaders = ((TransactionLogFile) logFile).getExternalFileReaders();
        try {
            assertThat(externalFileReaders).containsOnlyKeys(1L, 2L, 3L);
            for (var entry : externalFileReaders.entrySet()) {
                List<StoreChannel> channels = entry.getValue();
                assertThat(channels).hasSize(10);
                // all channels should be equal
                var exampleChannel = channels.get(0);
                for (StoreChannel channel : channels) {
                    assertEquals(channel, exampleChannel);
                }
            }
        } finally {
            logFile.terminateExternalReaders(3);
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void terminateLogFilesExternalReaders(KernelVersion kernelVersion) throws IOException, ExecutionException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        LogFile logFile = logFiles.getLogFile();
        logFile.rotate();
        logFile.rotate();
        logFile.rotate();
        logFile.rotate();

        assertEquals(5, logFile.getLogRangeInfo().highestVersion());

        var channelMap = LongObjectMaps.mutable.<StoreChannel>empty();
        channelMap.put(1, logFile.openForVersion(1));
        channelMap.put(2, logFile.openForVersion(2));
        channelMap.put(3, logFile.openForVersion(3));
        channelMap.put(4, logFile.openForVersion(4));

        ArrayList<Future<?>> futures = new ArrayList<>(10);
        try (ExecutorService registerCalls = Executors.newCachedThreadPool()) {
            for (int i = 0; i < 10; i++) {
                futures.add(registerCalls.submit(() -> logFile.registerExternalReaders(channelMap)));
            }
            Futures.getAll(futures);
        }

        logFile.terminateExternalReaders(3);

        try {
            var externalFileReaders = ((TransactionLogFile) logFile).getExternalFileReaders();
            assertThat(externalFileReaders).containsOnlyKeys(4L);
            for (var entry : externalFileReaders.entrySet()) {
                List<StoreChannel> channels = entry.getValue();
                assertThat(channels).hasSize(10);
                // all channels should be equal
                var exampleChannel = channels.get(0);
                for (StoreChannel channel : channels) {
                    assertEquals(channel, exampleChannel);
                }
            }
        } finally {
            logFile.terminateExternalReaders(4);
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void registerUnregisterLogFilesExternalReaders(KernelVersion kernelVersion) throws IOException, ExecutionException {
        LogFiles logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        LogFile logFile = logFiles.getLogFile();
        logFile.rotate();
        logFile.rotate();
        logFile.rotate();

        assertEquals(4, logFile.getLogRangeInfo().highestVersion());

        var channelMap = LongObjectMaps.mutable.<StoreChannel>empty();
        var channel1 = logFile.openForVersion(1);
        var channel2 = logFile.openForVersion(2);
        channelMap.put(1, channel1);
        channelMap.put(2, channel2);

        ArrayList<Future<?>> futures = new ArrayList<>(10);
        try (ExecutorService registerCalls = Executors.newCachedThreadPool()) {
            for (int i = 0; i < 10; i++) {
                futures.add(registerCalls.submit(() -> logFile.registerExternalReaders(channelMap)));
            }
        }
        Futures.getAll(futures);

        var externalFileReaders = ((TransactionLogFile) logFile).getExternalFileReaders();
        assertThat(externalFileReaders).containsOnlyKeys(1L, 2L);

        for (int i = 0; i < 100; i++) {
            logFile.unregisterExternalReader(1, channel1);
        }
        assertThat(externalFileReaders).containsOnlyKeys(2L);

        // removing wrong channel
        for (int i = 0; i < 19; i++) {
            logFile.unregisterExternalReader(2, channel1);
        }
        assertThat(externalFileReaders).containsOnlyKeys(2L);

        for (int i = 0; i < 9; i++) {
            logFile.unregisterExternalReader(2, channel2);
        }
        assertThat(externalFileReaders).containsOnlyKeys(2L);
        assertThat(externalFileReaders.get(2L)).hasSize(1);

        logFile.unregisterExternalReader(2, channel2);
        assertThat(externalFileReaders).isEmpty();
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void delete(KernelVersion kernelVersion) throws IOException {
        final var deletions = LongLists.mutable.empty();
        logFileVersionTracker = new LogFileVersionTracker() {
            @Override
            public void logDeleted(long version) {
                deletions.add(version);
            }

            @Override
            public void logCompleted(LogPosition endLogPosition) {}
        };

        final var logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        final var logFile = logFiles.getLogFile();
        logFile.rotate();
        logFile.rotate();
        logFile.rotate();

        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        final var lowestBeforeDelete = logRangeInfo.lowestVersion();
        assertThat(lowestBeforeDelete).isLessThan(logRangeInfo.highestVersion());

        logFile.delete(lowestBeforeDelete);
        assertThat(deletions.toArray()).containsExactly(lowestBeforeDelete);

        final var lowestAfterDelete = logFile.getLogRangeInfo().lowestVersion();
        assertThat(lowestBeforeDelete).isLessThan(lowestAfterDelete);

        logFile.delete(lowestAfterDelete);
        assertThat(deletions.toArray()).containsExactly(lowestBeforeDelete, lowestAfterDelete);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void rotate(KernelVersion kernelVersion) throws IOException {
        final var rotations = Lists.mutable.empty();
        logFileVersionTracker = new LogFileVersionTracker() {
            @Override
            public void logDeleted(long version) {}

            @Override
            public void logCompleted(LogPosition endLogPosition) {
                rotations.add(endLogPosition);
            }
        };

        final var logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        final var logFile = logFiles.getLogFile();

        final var lowestLogVersion = logFile.getLogRangeInfo().lowestVersion();
        logFile.rotate();
        logFile.rotate();

        final var listAssert = assertThat(rotations).hasSize(2);
        listAssert.element(0).satisfies(pos -> assertEndLogPosition(logFile, lowestLogVersion, (LogPosition) pos));
        listAssert.element(1).satisfies(pos -> assertEndLogPosition(logFile, lowestLogVersion + 1, (LogPosition) pos));
    }

    @Test
    void envelopedTruncateToEarlierVersionsShouldDeleteLaterLogs() throws IOException {
        // TODO MERGELOG Test can be removed once enveloped merge logs are activated
        truncateToEarlierVersionsShouldDeleteLaterLogs(GLORIOUS_FUTURE);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void truncateToEarlierVersionsShouldDeleteLaterLogs(KernelVersion kernelVersion) throws IOException {
        final var deleted = new HashSet<Long>();
        logFileVersionTracker = new LogFileVersionTracker() {
            @Override
            public void logDeleted(long version) {
                deleted.add(version);
            }

            @Override
            public void logCompleted(LogPosition endLogPosition) {}
        };

        final var logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        final var logFile = logFiles.getLogFile();

        final var lowestLogVersion = logFile.getLogRangeInfo().lowestVersion();
        logFile.rotate(); // lowestLogVersion + 1
        var writer = logFile.getTransactionLogWriter();
        final var filePos = writer.getCurrentPosition(); // truncate to here
        logFile.rotate(); // lowestLogVersion + 2 will be deleted
        logFile.rotate(); // lowestLogVersion + 3 will be deleted

        assertDoesNotThrow(() -> logFile.truncate(filePos));
        assertThat(logFile.getCurrentLogVersion()).isEqualTo(filePos.getLogVersion());
        assertThat(deleted).isEqualTo(Set.of(lowestLogVersion + 2L, lowestLogVersion + 3));
    }

    @Test
    void envelopedTruncateShouldThrowIfVersionMissing() throws IOException {
        // TODO MERGELOG  Test can be removed once enveloped merge logs are activated
        truncateShouldThrowIfVersionMissing(GLORIOUS_FUTURE);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void truncateShouldThrowIfVersionMissing(KernelVersion kernelVersion) throws IOException {
        final var logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        final var logFile = logFiles.getLogFile();

        final var lowestLogVersion = logFile.getLogRangeInfo().lowestVersion();
        var writer = logFile.getTransactionLogWriter();
        final var filePos = writer.getCurrentPosition(); // truncate to here
        logFile.rotate();
        logFile.rotate();
        logFile.delete(lowestLogVersion); // delete target version

        assertThrows(NoSuchFileException.class, () -> logFile.truncate(filePos));
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void ensureErrorsInLogFileVersionTrackerDontEscapeIntoLogFile(KernelVersion kernelVersion) throws IOException {
        logFileVersionTracker = new LogFileVersionTracker() {
            @Override
            public void logDeleted(long version) {
                throw new IllegalStateException("logDeleted");
            }

            @Override
            public void logCompleted(LogPosition endLogPosition) {
                throw new IllegalStateException("logCompleted");
            }
        };

        final var logFiles = buildLogFiles(kernelVersion);
        life.start();
        life.add(logFiles);

        final var logFile = logFiles.getLogFile();
        assertDoesNotThrow(() -> {
            logFile.rotate();
            logFile.rotate();
            logFile.delete(logFile.getLogRangeInfo().lowestVersion());
        });
    }

    private void assertEndLogPosition(LogFile logFile, long expectedVersion, LogPosition endLogPosition) {
        assertThat(endLogPosition.getLogVersion()).isEqualTo(expectedVersion);
        try {
            final var size = fileSystem.getFileSize(logFile.getLogFileForVersion(endLogPosition.getLogVersion()));
            assertThat(endLogPosition.getByteOffset()).isEqualTo(size);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static byte[] readBytes(ReadableChannel reader, int length) throws IOException {
        byte[] result = new byte[length];
        reader.get(result, length);
        return result;
    }

    private LogFiles buildLogFiles(KernelVersion kernelVersion) throws IOException {
        var builder = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        wrappingFileSystem,
                        fixed(kernelVersion),
                        () -> LogFormat.fromKernelVersion(kernelVersion))
                .withRotationThreshold(rotationThreshold)
                .withTransactionIdStore(transactionIdStore)
                .withLogVersionRepository(logVersionRepository)
                .withLogFileVersionTracker(logFileVersionTracker)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(STORE_ID);
        if (kernelVersion == KernelVersion.GLORIOUS_FUTURE) {
            final var futureEnabledConf = Config.newBuilder()
                    .set(
                            GraphDatabaseInternalSettings.latest_runtime_version,
                            DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
                    .set(GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version())
                    .build();
            builder.withConfig(futureEnabledConf);
        }
        return builder.build();
    }

    private static byte[] someBytes(int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = (byte) (i % 5);
        }
        return result;
    }

    private void startStop(
            CapturingNativeAccess capturingNativeAccess, LifeSupport lifeSupport, KernelVersion kernelVersion)
            throws IOException {
        LogFiles logFiles = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        fixed(kernelVersion),
                        () -> LogFormat.fromKernelVersion(kernelVersion))
                .withTransactionIdStore(transactionIdStore)
                .withLogVersionRepository(logVersionRepository)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(STORE_ID)
                .withDependencies(dependenciesOf(capturingNativeAccess))
                .build();

        lifeSupport.add(logFiles);
        lifeSupport.start();

        lifeSupport.shutdown();
    }

    private void createFile(Path filePath, long version, long lastAppendIndex, KernelVersion kernelVersion)
            throws IOException {

        var filesHelper = TransactionLogFilesHelper.forTransactions(filePath);
        try (StoreChannel storeChannel = fileSystem.write(filesHelper.getFileForVersion(version))) {
            LogFormat logFormat = LogFormat.fromKernelVersion(kernelVersion);
            LogHeader logHeader = logFormat.newHeader(
                    version,
                    lastAppendIndex,
                    LogHeader.UNKNOWN_TERM,
                    StoreIdentifier.newStoreIdentifier(STORE_ID),
                    256,
                    BASE_TX_CHECKSUM,
                    kernelVersion,
                    UNSPECIFIED_CREATION_TIME);
            writeLogHeader(storeChannel, logHeader, INSTANCE);
        }
    }

    private void createFile(Path filePath, long version, KernelVersion kernelVersion) throws IOException {
        createFile(filePath, version, 1, kernelVersion);
    }

    private static class CapturingNativeAccess implements NativeAccess {
        private int evictionCounter;
        private int adviseCounter;
        private int preallocateCounter;
        private int keepCounter;

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public NativeCallResult tryEvictFromCache(int fd) {
            evictionCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public NativeCallResult tryAdviseSequentialAccess(int fd) {
            adviseCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public NativeCallResult tryAdviseToKeepInCache(int fd) {
            keepCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public NativeCallResult tryPreallocateSpace(int fd, long bytes) {
            preallocateCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public ErrorTranslator errorTranslator() {
            return callResult -> false;
        }

        @Override
        public String describe() {
            return "Test only";
        }

        public int getEvictionCounter() {
            return evictionCounter;
        }

        public int getAdviseCounter() {
            return adviseCounter;
        }

        public int getKeepCounter() {
            return keepCounter;
        }

        public int getPreallocateCounter() {
            return preallocateCounter;
        }

        public void reset() {
            adviseCounter = 0;
            evictionCounter = 0;
            preallocateCounter = 0;
            keepCounter = 0;
        }
    }

    private static class CapturingChannelFileSystem extends DelegatingFileSystemAbstraction {
        private CapturingStoreChannel capturingChannel;

        CapturingChannelFileSystem(FileSystemAbstraction fs) {
            super(fs);
        }

        @Override
        public StoreChannel write(Path fileName) throws IOException {
            if (fileName.toString().contains(TransactionLogFilesHelper.DEFAULT_NAME)) {
                capturingChannel = new CapturingStoreChannel(super.write(fileName));
                return capturingChannel;
            }
            return super.write(fileName);
        }

        public CapturingStoreChannel getCapturingChannel() {
            return capturingChannel;
        }
    }

    private static class CapturingStoreChannel extends DelegatingStoreChannel<StoreChannel> {
        private final AtomicInteger writeAllCounter = new AtomicInteger();
        private final AtomicInteger flushCounter = new AtomicInteger();
        private final ReentrantLock writeAllLock = new ReentrantLock();

        private CapturingStoreChannel(StoreChannel delegate) {
            super(delegate);
        }

        @Override
        public void writeAll(ByteBuffer src) throws IOException {
            writeAllLock.lock();
            try {
                writeAllCounter.incrementAndGet();
                super.writeAll(src);
            } finally {
                writeAllLock.unlock();
            }
        }

        @Override
        public void flush() throws IOException {
            flushCounter.incrementAndGet();
            super.flush();
        }

        public AtomicInteger getWriteAllCounter() {
            return writeAllCounter;
        }

        public AtomicInteger getFlushCounter() {
            return flushCounter;
        }
    }
}
