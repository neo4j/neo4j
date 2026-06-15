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
package org.neo4j.io.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.memory.ByteBuffers.allocate;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.ByteUnit;
import org.neo4j.util.concurrent.Futures;

class EphemeralFileSystemTest {
    private EphemeralFileSystemAbstraction fs;

    @BeforeEach
    void setUp() {
        fs = new EphemeralFileSystemAbstraction();
    }

    @AfterEach
    void tearDown() throws IOException {
        fs.close();
    }

    @Test
    void allowStoreThatExceedDefaultSize() throws IOException {
        Path aFile = Path.of("test");
        StoreChannel channel = fs.write(aFile);

        ByteBuffer buffer = allocate(Long.BYTES, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        int mebiBytes = (int) ByteUnit.mebiBytes(1);
        for (int position = mebiBytes + 42; position < 10_000_000; position += mebiBytes) {
            buffer.putLong(1);
            buffer.flip();
            channel.writeAll(buffer, position);
            buffer.clear();
        }
        channel.close();
    }

    @Test
    void shouldNotLoseDataForcedBeforeFileSystemCrashes() throws Exception {
        try (EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction()) {
            // given
            int numberOfBytesForced = 8;

            Path aFile = Path.of("yo");

            StoreChannel channel = fs.write(aFile);
            writeLong(channel, 1111);

            // when
            channel.force(true);
            writeLong(channel, 2222);
            fs.crash();

            // then
            StoreChannel readChannel = fs.read(aFile);
            assertThat(readChannel.size()).isEqualTo(numberOfBytesForced);

            assertThat(readLong(readChannel).getLong()).isEqualTo(1111);
        }
    }

    @Test
    void shouldBeConsistentAfterConcurrentWritesAndCrashes() throws Exception {
        try (ExecutorService executorService = Executors.newCachedThreadPool();
                EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction()) {
            Path aFile = Path.of("contendedFile");
            for (int attempt = 0; attempt < 100; attempt++) {
                Collection<Callable<Void>> workers = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    workers.add(() -> {
                        try {
                            StoreChannel channel = fs.write(aFile);
                            channel.position(0);
                            writeLong(channel, 1);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

                    workers.add(() -> {
                        fs.crash();
                        return null;
                    });
                }

                List<Future<Void>> futures = executorService.invokeAll(workers);
                Futures.getAllResults(futures);
                verifyFileIsEitherEmptyOrContainsLongIntegerValueOne(fs.write(aFile));
            }
        }
    }

    @Test
    void shouldBeConsistentAfterConcurrentWritesAndForces() throws Exception {
        try (ExecutorService executorService = Executors.newCachedThreadPool()) {
            for (int attempt = 0; attempt < 100; attempt++) {
                try (EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction()) {
                    Path aFile = Path.of("contendedFile");

                    Collection<Callable<Void>> workers = new ArrayList<>();
                    for (int i = 0; i < 100; i++) {
                        workers.add(() -> {
                            try {
                                StoreChannel channel = fs.write(aFile);
                                channel.position(channel.size());
                                writeLong(channel, 1);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });

                        workers.add(() -> {
                            StoreChannel channel = fs.write(aFile);
                            channel.force(true);
                            return null;
                        });
                    }

                    List<Future<Void>> futures = executorService.invokeAll(workers);
                    Futures.getAllResults(futures);

                    fs.crash();
                    verifyFileIsFullOfLongIntegerOnes(fs.write(aFile));
                }
            }
        }
    }

    @Test
    void releaseResourcesOnClose() throws IOException {
        try (EphemeralFileSystemAbstraction fileSystemAbstraction = new EphemeralFileSystemAbstraction()) {
            Path testDir = Path.of("testDir");
            Path testFile = Path.of("testFile");
            fileSystemAbstraction.mkdir(testDir);
            fileSystemAbstraction.write(testFile);

            assertThat(fileSystemAbstraction.fileExists(testFile)).isTrue();
            assertThat(fileSystemAbstraction.fileExists(testFile)).isTrue();

            fileSystemAbstraction.close();

            assertThat(fileSystemAbstraction.isClosed()).isTrue();
            assertThat(fileSystemAbstraction.fileExists(testFile)).isFalse();
            assertThat(fileSystemAbstraction.fileExists(testFile)).isFalse();
        }
    }

    private static void verifyFileIsFullOfLongIntegerOnes(StoreChannel channel) {
        try {
            long claimedSize = channel.size();
            ByteBuffer buffer = allocate((int) claimedSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            channel.readAll(buffer);
            buffer.flip();

            for (int position = 0; position < claimedSize; position += 8) {
                long value = buffer.getLong(position);
                assertThat(value).isEqualTo(1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void verifyFileIsEitherEmptyOrContainsLongIntegerValueOne(StoreChannel channel) {
        try {
            long claimedSize = channel.size();
            ByteBuffer buffer = allocate(8, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            channel.read(buffer, 0);
            buffer.flip();

            if (claimedSize == 8) {
                assertThat(buffer.getLong()).isEqualTo(1);
            } else {
                assertThatThrownBy(buffer::getLong).isInstanceOf(BufferUnderflowException.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ByteBuffer readLong(StoreChannel readChannel) throws IOException {
        ByteBuffer readBuffer = allocate(8, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        readChannel.readAll(readBuffer);
        readBuffer.flip();
        return readBuffer;
    }

    private static void writeLong(StoreChannel channel, long value) throws IOException {
        ByteBuffer buffer = allocate(8, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        buffer.putLong(value);
        buffer.flip();
        channel.write(buffer);
    }
}
