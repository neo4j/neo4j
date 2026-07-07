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
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LogFilesMetadataTest {

    @Inject
    FileSystemAbstraction fs;

    @Inject
    TestDirectory testDirectory;

    LogsRepository logsRepository;
    EnvelopedLogHeaderCache logHeaderCache;

    @BeforeEach
    void setUp() {
        var baseFile = testDirectory.directory("logsFolder");
        logsRepository = new LogsRepository(fs, new SequentialFileNameHelper(baseFile.getParent(), "raftLog"));
        logHeaderCache = new EnvelopedLogHeaderCache();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleEmpty(boolean useCache) throws IOException {
        var logFilesMetadata =
                useCache ? new LogFilesMetadata(logsRepository, logHeaderCache) : new LogFilesMetadata(logsRepository);
        assertThat(logFilesMetadata.next()).isFalse();
        assertThat(logFilesMetadata.get()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReadHeaders(boolean useCache) throws IOException {
        var logHeader1 = LogFormat.V10.newHeader(
                4, 1, 1, StoreIdentifier.UNKNOWN, 512, 1, KernelVersion.GLORIOUS_FUTURE, UNSPECIFIED_CREATION_TIME);
        var logHeader2 = LogFormat.V10.newHeader(
                5, 2, 2, StoreIdentifier.UNKNOWN, 512, 2, KernelVersion.GLORIOUS_FUTURE, UNSPECIFIED_CREATION_TIME);
        var logHeader3 = LogFormat.V10.newHeader(
                6, 3, 3, StoreIdentifier.UNKNOWN, 512, 3, KernelVersion.GLORIOUS_FUTURE, UNSPECIFIED_CREATION_TIME);
        var logHeaders = new LogHeader[] {logHeader1, logHeader2, logHeader3};
        for (var logHeader : logHeaders) {
            var writeChannel = logsRepository.createWriteChannel(logHeader.getLogVersion());
            LogFormat.writeLogHeader(writeChannel.channel(), logHeader, EmptyMemoryTracker.INSTANCE);
            writeChannel.channel().flush();
            writeChannel.channel().close();
            if (useCache) {
                logHeaderCache.cache(logHeader);
            }
        }

        // reading forwards
        var logFilesMetadata =
                useCache ? new LogFilesMetadata(logsRepository, logHeaderCache) : new LogFilesMetadata(logsRepository);
        for (var logHeader : logHeaders) {
            assertThat(logFilesMetadata.next()).isTrue();
            var metadata = logFilesMetadata.get();
            assertThat(metadata.logHeader()).isEqualTo(logHeader);
            assertThat(metadata.version()).isEqualTo(logHeader.getLogVersion());
        }
        assertThat(logFilesMetadata.next()).isFalse();
        assertThat(logFilesMetadata.get()).isNull();

        // reading backwards
        logFilesMetadata = useCache
                ? new LogFilesMetadata(logsRepository, logHeaderCache, true)
                : new LogFilesMetadata(logsRepository, true);
        for (int i = logHeaders.length - 1; i >= 0; i--) {
            var logHeader = logHeaders[i];
            assertThat(logFilesMetadata.next()).isTrue();
            var metadata = logFilesMetadata.get();
            assertThat(metadata.logHeader()).isEqualTo(logHeader);
            assertThat(metadata.version()).isEqualTo(logHeader.getLogVersion());
        }
        assertThat(logFilesMetadata.next()).isFalse();
        assertThat(logFilesMetadata.get()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldIgnorePreAllocatedFiles(boolean useCache) throws IOException {
        var logHeader1 = LogFormat.V10.newHeader(
                0, 1, 1, StoreIdentifier.UNKNOWN, 512, 1, KernelVersion.GLORIOUS_FUTURE, UNSPECIFIED_CREATION_TIME);
        var logHeader2 = LogFormat.V10.newHeader(
                1, 2, 2, StoreIdentifier.UNKNOWN, 512, 2, KernelVersion.GLORIOUS_FUTURE, UNSPECIFIED_CREATION_TIME);
        var logHeader3 = LogFormat.V10.newHeader(
                2, 3, 3, StoreIdentifier.UNKNOWN, 512, 3, KernelVersion.GLORIOUS_FUTURE, UNSPECIFIED_CREATION_TIME);
        var logHeaders = new LogHeader[] {logHeader1, logHeader2, logHeader3};
        for (var i = 0; i < logHeaders.length; i++) {

            var writeChannel = logsRepository.createWriteChannel(i);
            LogFormat.writeLogHeader(writeChannel.channel(), logHeaders[i], EmptyMemoryTracker.INSTANCE);
            writeChannel.channel().flush();
            writeChannel.channel().close();
            if (useCache) {
                logHeaderCache.cache(logHeaders[i]);
            }
        }

        var zeroes = new byte[128];
        for (int i = 0; i < 2; i++) {
            var writeChannel = logsRepository.createWriteChannel(i + logHeaders.length);
            writeChannel.channel().writeAll(ByteBuffer.wrap(zeroes));
            writeChannel.channel().flush();
            writeChannel.channel().close();
        }

        // reading forwards
        var logFilesMetadata =
                useCache ? new LogFilesMetadata(logsRepository, logHeaderCache) : new LogFilesMetadata(logsRepository);
        for (int i = 0; i < logHeaders.length; i++) {
            assertThat(logFilesMetadata.next()).isTrue();
            var metadata = logFilesMetadata.get();
            assertThat(metadata.logHeader()).isEqualTo(logHeaders[i]);
            assertThat(metadata.version()).isEqualTo(i);
        }
        assertThat(logFilesMetadata.next()).isFalse();
        assertThat(logFilesMetadata.get()).isNull();

        // reading backwards
        logFilesMetadata = useCache
                ? new LogFilesMetadata(logsRepository, logHeaderCache, true)
                : new LogFilesMetadata(logsRepository, true);
        for (int i = logHeaders.length - 1; i >= 0; i--) {
            assertThat(logFilesMetadata.next()).isTrue();
            var metadata = logFilesMetadata.get();
            assertThat(metadata.logHeader()).isEqualTo(logHeaders[i]);
            assertThat(metadata.version()).isEqualTo(i);
        }
        assertThat(logFilesMetadata.next()).isFalse();
        assertThat(logFilesMetadata.get()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void hasNextShouldReturnFalseForPreallocatedFiles(boolean useCache) throws IOException {
        var zeroes = new byte[128];
        for (int i = 0; i < 2; i++) {
            var writeChannel = logsRepository.createWriteChannel(i);
            writeChannel.channel().writeAll(ByteBuffer.wrap(zeroes));
            writeChannel.channel().flush();
            writeChannel.channel().close();
        }

        var logFilesMetadata =
                useCache ? new LogFilesMetadata(logsRepository, logHeaderCache) : new LogFilesMetadata(logsRepository);
        assertThat(logFilesMetadata.next()).isFalse();
        assertThat(logFilesMetadata.get()).isNull();

        logFilesMetadata = useCache
                ? new LogFilesMetadata(logsRepository, logHeaderCache, true)
                : new LogFilesMetadata(logsRepository, true);
        assertThat(logFilesMetadata.next()).isFalse();
        assertThat(logFilesMetadata.get()).isNull();
    }
}
