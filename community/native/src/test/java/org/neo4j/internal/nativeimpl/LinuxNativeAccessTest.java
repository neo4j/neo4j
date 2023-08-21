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
package org.neo4j.internal.nativeimpl;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.commons.lang3.reflect.FieldUtils.getDeclaredField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.nativeimpl.NativeAccess.ERROR;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

class LinuxNativeAccessTest {
    private final LinuxNativeAccess nativeAccess = new LinuxNativeAccess();

    @Test
    @DisabledOnOs(OS.LINUX)
    void disabledOnNonLinux() {
        assertFalse(nativeAccess.isAvailable());
    }

    @Nested
    @EnabledOnOs(OS.LINUX)
    class AccessLinuxMethodsTest {
        @TempDir
        Path tempFile;

        @Test
        void availableOnLinux() {
            assertTrue(nativeAccess.isAvailable());
        }

        @Test
        void accessErrorMessageOnError() throws IOException, IllegalAccessException, ClassNotFoundException {
            Path file = tempFile.resolve("file");
            int descriptor = getClosedDescriptor(file);
            var nativeCallResult = nativeAccess.tryPreallocateSpace(descriptor, 1024);
            assertNotEquals(0, nativeCallResult.getErrorCode());
            assertThat(nativeCallResult.getErrorMessage()).isNotEmpty();
        }

        @Test
        void failToPreallocateOnLinuxForIncorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            var preallocateResult = nativeAccess.tryPreallocateSpace(0, 1024);
            assertEquals(ERROR, preallocateResult.getErrorCode());
            assertTrue(preallocateResult.isError());

            var negativeDescriptor = nativeAccess.tryPreallocateSpace(-1, 1024);
            assertEquals(ERROR, negativeDescriptor.getErrorCode());
            assertTrue(negativeDescriptor.isError());

            Path file = tempFile.resolveSibling("file");
            int descriptor = getClosedDescriptor(file);
            assertNotEquals(0, nativeAccess.tryPreallocateSpace(descriptor, 1024));
        }

        @Test
        void preallocateCacheOnLinuxForCorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            FileStore fileStore = Files.getFileStore(tempFile);
            long blockSize = fileStore.getBlockSize();
            Path file = tempFile.resolve("preallocated1");
            Path file2 = tempFile.resolve("preallocated2");
            Path file3 = tempFile.resolve("preallocated3");
            long size1 = blockSize - 1;
            long size2 = blockSize;
            long size3 = 2 * blockSize;

            preallocate(file, size1);
            preallocate(file2, size2);
            preallocate(file3, size3);

            assertEquals(size1, Files.size(file));
            assertEquals(size2, Files.size(file2));
            assertEquals(size3, Files.size(file3));
        }

        @Test
        void failToAdviseSequentialOnLinuxForIncorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            var nativeCallResult = nativeAccess.tryAdviseSequentialAccess(0);
            assertEquals(ERROR, nativeCallResult.getErrorCode());
            assertTrue(nativeCallResult.isError());

            var negativeDescriptorResult = nativeAccess.tryAdviseSequentialAccess(-1);
            assertEquals(ERROR, negativeDescriptorResult.getErrorCode());
            assertTrue(negativeDescriptorResult.isError());

            Path file = tempFile.resolve("sequentialFile");
            int descriptor = getClosedDescriptor(file);
            assertNotEquals(0, nativeAccess.tryAdviseSequentialAccess(descriptor));
        }

        @Test
        void ootOfDiskErrorCheck() {
            assertTrue(nativeAccess.errorTranslator().isOutOfDiskSpace(new NativeCallResult(28, "Out of space jam!")));
        }

        @Test
        void adviseSequentialAccessOnLinuxForCorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            Path file = tempFile.resolve("correctSequentialFile");
            try (Channel channel = FileChannel.open(file, READ, WRITE, CREATE)) {
                int descriptor = getDescriptor(channel);
                var nativeCallResult = nativeAccess.tryAdviseSequentialAccess(descriptor);
                assertEquals(0, nativeCallResult.getErrorCode());
                assertFalse(nativeCallResult.isError());
            }
        }

        @Test
        void failToSkipCacheOnLinuxForIncorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            assertEquals(ERROR, nativeAccess.tryEvictFromCache(0).getErrorCode());
            assertEquals(ERROR, nativeAccess.tryEvictFromCache(-1).getErrorCode());

            Path file = tempFile.resolve("file");
            int descriptor = getClosedDescriptor(file);
            assertNotEquals(0, nativeAccess.tryEvictFromCache(descriptor));
        }

        @Test
        void skipCacheOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException, ClassNotFoundException {
            Path file = tempFile.resolve("file");
            try (Channel channel = FileChannel.open(file, READ, WRITE, CREATE)) {
                int descriptor = getDescriptor(channel);
                assertFalse(nativeAccess.tryEvictFromCache(descriptor).isError());
            }
        }
    }

    private void preallocate(Path file, long bytes) throws IOException, IllegalAccessException, ClassNotFoundException {
        try (Channel channel = FileChannel.open(file, READ, WRITE, CREATE)) {
            int descriptor = getDescriptor(channel);
            assertFalse(nativeAccess.tryPreallocateSpace(descriptor, bytes).isError());
        }
    }

    private static int getClosedDescriptor(Path file)
            throws IOException, IllegalAccessException, ClassNotFoundException {
        try (Channel channel = FileChannel.open(file, READ, WRITE, CREATE)) {
            return getDescriptor(channel);
        }
    }

    private static int getDescriptor(Channel channel) throws ClassNotFoundException, IllegalAccessException {
        Class<?> fileChannelImpl = Class.forName("sun.nio.ch.FileChannelImpl");
        FileDescriptor fd =
                (FileDescriptor) getDeclaredField(fileChannelImpl, "fd", true).get(channel);
        return getDeclaredField(FileDescriptor.class, "fd", true).getInt(fd);
    }
}
