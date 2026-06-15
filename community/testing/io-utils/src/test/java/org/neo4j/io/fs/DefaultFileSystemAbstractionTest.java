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

import static java.lang.String.format;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;
import static org.neo4j.io.fs.FileSystemAbstraction.DEFAULT_OUTPUT_STREAM_BUFFER_SIZE;
import static org.neo4j.io.fs.FileSystemAbstraction.INVALID_FILE_DESCRIPTOR;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DisabledForRoot;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
public class DefaultFileSystemAbstractionTest extends FileSystemAbstractionTest {

    @Inject
    RandomSupport random;

    @Override
    protected FileSystemAbstraction buildFileSystemAbstraction() {
        return new DefaultFileSystemAbstraction();
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void retrieveFileDescriptor() throws IOException {
        Path testFile = testDirectory.file("testFile");
        try (StoreChannel storeChannel = fsa.write(testFile)) {
            int fileDescriptor = fsa.getFileDescriptor(storeChannel);
            assertThat(fileDescriptor).isGreaterThan(0);
        }
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void retrieveWindowsFileDescriptor() throws IOException {
        Path testFile = testDirectory.file("testFile");
        try (StoreChannel storeChannel = fsa.write(testFile)) {
            int fileDescriptor = fsa.getFileDescriptor(storeChannel);
            assertThat(fileDescriptor).isEqualTo(INVALID_FILE_DESCRIPTOR);
        }
    }

    @Test
    void retrieveFileDescriptorOnClosedChannel() throws IOException {
        Path testFile = testDirectory.file("testFile");
        StoreChannel escapedChannel;
        try (StoreChannel storeChannel = fsa.write(testFile)) {
            escapedChannel = storeChannel;
        }
        int fileDescriptor = fsa.getFileDescriptor(escapedChannel);
        assertThat(fileDescriptor).isEqualTo(INVALID_FILE_DESCRIPTOR);
    }

    @Test
    void retrieveBlockSize() throws IOException {
        var testFile = testDirectory.createFile("testBlock");
        long blockSize = fsa.getBlockSize(testFile);
        assertThat(isPowerOfTwo(blockSize)).isTrue();
        assertThat(blockSize).isGreaterThanOrEqualTo(512L);
    }

    @Test
    void readFileWithInputStream() throws IOException {
        var testFile = testDirectory.createFile("testFile");
        int size = current().nextInt((int) ByteUnit.mebiBytes(13));
        byte[] sourceData = random.nextBytes(size);
        Files.write(testFile, sourceData);

        byte[] contentFromDrive;
        try (var stream = fsa.openAsInputStream(testFile)) {
            contentFromDrive = stream.readAllBytes();
        }

        assertThat(contentFromDrive).isEqualTo(sourceData);
    }

    @Test
    void readFileWithDifferentStream() throws IOException {
        var testFile = testDirectory.createFile("testFile");
        int size = current().nextInt((int) ByteUnit.mebiBytes(2));
        byte[] sourceData = random.nextBytes(size);
        Files.write(testFile, sourceData);

        for (int bufferSize = 1; bufferSize < sourceData.length; bufferSize += (int) ByteUnit.kibiBytes(1)) {
            assertThat(readContent(testFile, bufferSize, sourceData.length)).isEqualTo(sourceData);
        }
    }

    @Test
    void writeFileWithOutputStream() throws IOException {
        var testFile = testDirectory.createFile("testFile");
        int size = current().nextInt((int) ByteUnit.mebiBytes(13));

        byte[] sourceData = random.nextBytes(size);
        try (var stream = fsa.openAsOutputStream(testFile, false)) {
            for (byte aByte : sourceData) {
                stream.write(aByte);
            }
        }

        byte[] contentFromDrive = Files.readAllBytes(testFile);
        assertThat(contentFromDrive).isEqualTo(sourceData);
    }

    @Test
    void writeFileWithOutputStreamWithDifferentBuffer() throws IOException {
        var testFile = testDirectory.createFile("testFile");
        int size = current().nextInt((int) ByteUnit.mebiBytes(20));

        byte[] sourceData = random.nextBytes(size);
        int baseBufferSize = DEFAULT_OUTPUT_STREAM_BUFFER_SIZE;
        try (var channel = new DefaultFileSystemAbstraction.NativeByteBufferOutputStream(
                        fsa.write(testFile), baseBufferSize);
                var buffered = new BufferedOutputStream(channel, baseBufferSize + random.nextInt(10, 455))) {
            for (int i = 0; i < sourceData.length; ) {
                if (random.nextBoolean()) {
                    buffered.write(sourceData[i]);
                    i++;
                } else {
                    int remaining = size - i;
                    int bytesToWrite = remaining == 1 ? 1 : random.nextInt(1, Math.min(remaining, 256));
                    buffered.write(sourceData, i, bytesToWrite);
                    i += bytesToWrite;
                }
            }
        }

        byte[] contentFromDrive = Files.readAllBytes(testFile);
        assertThat(contentFromDrive).isEqualTo(sourceData);
    }

    @Test
    void mappingFile() throws IOException {
        Path testFile = testDirectory.file("testFile");
        try (DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            StoreFileChannel channel = fs.open(
                    testFile, Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ));
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.PRIVATE, 0, 32);
            mapped.putLong(0, 42L);
            assertThat(mapped.getLong(0)).isEqualTo(42L);
        }
    }

    @Test
    // Windows doesn't seem to be able to set a directory read only without complex ACLs
    @DisabledOnOs(OS.WINDOWS)
    @DisabledForRoot
    void shouldFailGracefullyWhenPathCannotBeCreated() throws Exception {
        Files.createDirectories(path);
        assertThat(fsa.fileExists(path)).isTrue();
        Files.setPosixFilePermissions(
                path,
                EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.GROUP_READ,
                        PosixFilePermission.OTHERS_READ));
        path = path.resolve("some_file");

        assertThat(fsa.isDirectory(path)).isFalse();

        assertThatThrownBy(() -> fsa.mkdirs(path))
                .isInstanceOf(IOException.class)
                .hasMessage(format(UNABLE_TO_CREATE_DIRECTORY_FORMAT, path))
                .hasCauseInstanceOf(AccessDeniedException.class);
    }

    private byte[] readContent(Path testFile, int bufferSize, int dataLength) throws IOException {
        byte[] contentFromDrive = new byte[dataLength];
        int contentIndex = 0;
        byte[] buffer = new byte[bufferSize];
        try (var stream = fsa.openAsInputStream(testFile)) {
            while (true) {
                int readData = stream.read(buffer);
                if (readData == -1) {
                    return contentFromDrive;
                }
                System.arraycopy(buffer, 0, contentFromDrive, contentIndex, readData);
                contentIndex += readData;
            }
        }
    }
}
