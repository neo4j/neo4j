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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;
import static org.neo4j.io.fs.FileSystemAbstraction.INVALID_FILE_DESCRIPTOR;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DisabledForRoot;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
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
        assertTrue(isPowerOfTwo(blockSize), "Observed block size: " + blockSize);
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

        assertArrayEquals(sourceData, contentFromDrive);
    }

    @Test
    void readFileWithDifferentStream() throws IOException {
        var testFile = testDirectory.createFile("testFile");
        int size = current().nextInt((int) ByteUnit.mebiBytes(2));
        byte[] sourceData = random.nextBytes(size);
        Files.write(testFile, sourceData);

        for (int bufferSize = 1; bufferSize < sourceData.length; bufferSize += (int) ByteUnit.kibiBytes(1)) {
            assertArrayEquals(sourceData, readContent(testFile, bufferSize, sourceData.length));
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
        assertArrayEquals(sourceData, contentFromDrive);
    }

    @Test
    void writeFileWithOutputStreamWithDifferentBuffer() throws IOException {
        var testFile = testDirectory.createFile("testFile");
        int size = current().nextInt((int) ByteUnit.mebiBytes(20));

        byte[] sourceData = random.nextBytes(size);
        try (var channel = new DefaultFileSystemAbstraction.NativeByteBufferOutputStream(
                        (StoreFileChannel) fsa.write(testFile));
                var buffered =
                        new BufferedOutputStream(channel, (int) (ByteUnit.kibiBytes(8) + random.nextInt(10, 455)))) {
            for (byte aByte : sourceData) {
                buffered.write(aByte);
            }
        }

        byte[] contentFromDrive = Files.readAllBytes(testFile);
        assertArrayEquals(sourceData, contentFromDrive);
    }

    @Test
    // Windows doesn't seem to be able to set a directory read only without complex ACLs
    @DisabledOnOs(OS.WINDOWS)
    @DisabledForRoot
    void shouldFailGracefullyWhenPathCannotBeCreated() throws Exception {
        Files.createDirectories(path);
        assertTrue(fsa.fileExists(path));
        Files.setPosixFilePermissions(
                path,
                EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.GROUP_READ,
                        PosixFilePermission.OTHERS_READ));
        path = path.resolve("some_file");

        IOException exception = assertThrows(IOException.class, () -> fsa.mkdirs(path));
        assertFalse(fsa.isDirectory(path));
        String expectedMessage = format(UNABLE_TO_CREATE_DIRECTORY_FORMAT, path);
        assertThat(exception.getMessage()).isEqualTo(expectedMessage);
        Throwable cause = exception.getCause();
        assertThat(cause).isInstanceOf(AccessDeniedException.class);
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
