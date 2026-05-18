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
        assertThat(nativeAccess.isAvailable()).isFalse();
    }

    @Nested
    @EnabledOnOs(OS.LINUX)
    class AccessLinuxMethodsTest {
        @TempDir
        Path tempFile;

        @Test
        void availableOnLinux() {
            assertThat(nativeAccess.isAvailable()).isTrue();
        }

        @Test
        void accessErrorMessageOnError() throws IOException, IllegalAccessException, ClassNotFoundException {
            Path file = tempFile.resolve("file");
            int descriptor = getClosedDescriptor(file);
            var nativeCallResult = nativeAccess.tryPreallocateSpace(descriptor, 1024);
            assertThat(nativeCallResult.getErrorCode()).isNotZero();
            assertThat(nativeCallResult.getErrorMessage()).isNotEmpty();
        }

        @Test
        void failToPreallocateOnLinuxForIncorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            var preallocateResult = nativeAccess.tryPreallocateSpace(0, 1024);
            assertThat(preallocateResult.getErrorCode()).isEqualTo(ERROR);
            assertThat(preallocateResult.isError()).isTrue();

            var negativeDescriptor = nativeAccess.tryPreallocateSpace(-1, 1024);
            assertThat(negativeDescriptor.getErrorCode()).isEqualTo(ERROR);
            assertThat(negativeDescriptor.isError()).isTrue();

            Path file = tempFile.resolveSibling("file");
            int descriptor = getClosedDescriptor(file);
            assertThat(nativeAccess.tryPreallocateSpace(descriptor, 1024)).isNotEqualTo(0);
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

            assertThat(Files.size(file)).isEqualTo(size1);
            assertThat(Files.size(file2)).isEqualTo(size2);
            assertThat(Files.size(file3)).isEqualTo(size3);
        }

        @Test
        void failToAdviseSequentialOnLinuxForIncorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            var nativeCallResult = nativeAccess.tryAdviseSequentialAccess(0);
            assertThat(nativeCallResult.getErrorCode()).isEqualTo(ERROR);
            assertThat(nativeCallResult.isError()).isTrue();

            var negativeDescriptorResult = nativeAccess.tryAdviseSequentialAccess(-1);
            assertThat(negativeDescriptorResult.getErrorCode()).isEqualTo(ERROR);
            assertThat(negativeDescriptorResult.isError()).isTrue();

            Path file = tempFile.resolve("sequentialFile");
            int descriptor = getClosedDescriptor(file);
            assertThat(nativeAccess.tryAdviseSequentialAccess(descriptor)).isNotEqualTo(0);
        }

        @Test
        void ootOfDiskErrorCheck() {
            assertThat(nativeAccess.errorTranslator().isOutOfDiskSpace(new NativeCallResult(28, "Out of space jam!")))
                    .isTrue();
        }

        @Test
        void adviseSequentialAccessOnLinuxForCorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            Path file = tempFile.resolve("correctSequentialFile");
            try (Channel channel = FileChannel.open(file, READ, WRITE, CREATE)) {
                int descriptor = getDescriptor(channel);
                var nativeCallResult = nativeAccess.tryAdviseSequentialAccess(descriptor);
                assertThat(nativeCallResult.getErrorCode()).isZero();
                assertThat(nativeCallResult.isError()).isFalse();
            }
        }

        @Test
        void failToSkipCacheOnLinuxForIncorrectDescriptor()
                throws IOException, IllegalAccessException, ClassNotFoundException {
            assertThat(nativeAccess.tryEvictFromCache(0).getErrorCode()).isEqualTo(ERROR);
            assertThat(nativeAccess.tryEvictFromCache(-1).getErrorCode()).isEqualTo(ERROR);

            Path file = tempFile.resolve("file");
            int descriptor = getClosedDescriptor(file);
            assertThat(nativeAccess.tryEvictFromCache(descriptor)).isNotEqualTo(0);
        }

        @Test
        void skipCacheOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException, ClassNotFoundException {
            Path file = tempFile.resolve("file");
            try (Channel channel = FileChannel.open(file, READ, WRITE, CREATE)) {
                int descriptor = getDescriptor(channel);
                assertThat(nativeAccess.tryEvictFromCache(descriptor).isError()).isFalse();
            }
        }
    }

    private void preallocate(Path file, long bytes) throws IOException, IllegalAccessException, ClassNotFoundException {
        try (Channel channel = FileChannel.open(file, READ, WRITE, CREATE)) {
            int descriptor = getDescriptor(channel);
            assertThat(nativeAccess.tryPreallocateSpace(descriptor, bytes).isError())
                    .isFalse();
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
