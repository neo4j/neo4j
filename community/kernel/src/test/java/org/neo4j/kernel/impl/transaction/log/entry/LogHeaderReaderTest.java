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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.LOG_VERSION_MASK;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_HEADER_SIZE_3_5;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.LegacyStoreId;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class LogHeaderReaderTest {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    private long expectedLogVersion;
    private long expectedTxId;
    private LegacyStoreId expectedStoreId;

    @BeforeEach
    void setUp() {
        expectedLogVersion = random.nextLong(0, LOG_VERSION_MASK);
        expectedTxId = random.nextLong();
        expectedStoreId = new LegacyStoreId(random.nextLong(), random.nextLong(), random.nextLong());
    }

    @Test
    void shouldReadAnOldLogHeaderFromAByteChannel() throws IOException {
        var buffer = ByteBuffers.allocate(CURRENT_FORMAT_LOG_HEADER_SIZE, ByteOrder.BIG_ENDIAN, INSTANCE);
        byte oldVersion = 6;
        buffer.putLong(encodeLogVersion(expectedLogVersion, oldVersion));
        buffer.putLong(expectedTxId);

        var channel = new InMemoryClosableChannel(buffer.array(), true, true, ByteOrder.LITTLE_ENDIAN);

        var result = readLogHeader(channel, true, null, INSTANCE);

        assertThat(result).isEqualTo(new LogHeader(oldVersion, expectedLogVersion, expectedTxId, LOG_HEADER_SIZE_3_5));
    }

    @Test
    void shouldReadALogHeaderFromAByteChannel() throws IOException {
        var buffer = ByteBuffers.allocate(CURRENT_FORMAT_LOG_HEADER_SIZE, ByteOrder.BIG_ENDIAN, INSTANCE);
        buffer.putLong(encodeLogVersion(expectedLogVersion, CURRENT_LOG_FORMAT_VERSION));
        buffer.putLong(expectedTxId);
        buffer.putLong(expectedStoreId.getCreationTime());
        buffer.putLong(expectedStoreId.getRandomId());
        buffer.putLong(expectedStoreId.getStoreVersion());
        buffer.putLong(0); // reserved
        buffer.putLong(0); // reserved
        buffer.putLong(0); // reserved

        var channel = new InMemoryClosableChannel(buffer.array(), true, true, ByteOrder.LITTLE_ENDIAN);

        var result = readLogHeader(channel, true, null, INSTANCE);

        assertThat(result)
                .isEqualTo(new LogHeader(
                        CURRENT_LOG_FORMAT_VERSION,
                        expectedLogVersion,
                        expectedTxId,
                        expectedStoreId,
                        CURRENT_FORMAT_LOG_HEADER_SIZE));
    }

    @Test
    void shouldFailWhenUnableToReadALogHeaderFromAChannel() throws IOException {
        var buffer = ByteBuffers.allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        buffer.put((byte) 0xAF);

        var channel = new InMemoryClosableChannel(buffer.array(), true, true, ByteOrder.LITTLE_ENDIAN);

        assertThatThrownBy(() -> readLogHeader(channel, true, null, INSTANCE))
                .isInstanceOf(IncompleteLogHeaderException.class);
    }

    @Test
    void shouldReadALogHeaderFromAFile() throws IOException {
        var file = testDirectory.file("ReadLogHeader");

        var buffer = ByteBuffers.allocate(CURRENT_FORMAT_LOG_HEADER_SIZE, ByteOrder.BIG_ENDIAN, INSTANCE);
        buffer.putLong(encodeLogVersion(expectedLogVersion, CURRENT_LOG_FORMAT_VERSION));
        buffer.putLong(expectedTxId);
        buffer.putLong(expectedStoreId.getCreationTime());
        buffer.putLong(expectedStoreId.getRandomId());
        buffer.putLong(expectedStoreId.getStoreVersion());

        try (var stream = fileSystem.openAsOutputStream(file, false)) {
            stream.write(buffer.array());
        }

        var result = readLogHeader(fileSystem, file, INSTANCE);

        assertThat(result)
                .isEqualTo(new LogHeader(
                        CURRENT_LOG_FORMAT_VERSION,
                        expectedLogVersion,
                        expectedTxId,
                        expectedStoreId,
                        CURRENT_FORMAT_LOG_HEADER_SIZE));
    }

    @Test
    void shouldFailWhenUnableToReadALogHeaderFromAFile() throws IOException {
        var file = testDirectory.file("ReadLogHeader");

        ((StoreChannel) fileSystem.write(file)).close();

        assertThatThrownBy(() -> readLogHeader(fileSystem, file, INSTANCE))
                .isInstanceOf(IncompleteLogHeaderException.class)
                .hasMessageContaining(file.getFileName().toString());
    }

    @Test
    void readEmptyPreallocatedFileHeaderAsNoHeader() throws IOException {
        var channel =
                new InMemoryClosableChannel(new byte[CURRENT_LOG_FORMAT_VERSION], true, true, ByteOrder.LITTLE_ENDIAN);

        var result = readLogHeader(channel, true, null, INSTANCE);

        assertThat(result).isNull();
    }
}
