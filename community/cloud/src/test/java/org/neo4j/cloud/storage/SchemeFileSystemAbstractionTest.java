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
package org.neo4j.cloud.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.cloud.storage.StorageUtils.APPEND_OPTIONS;
import static org.neo4j.cloud.storage.StorageUtils.READ_OPTIONS;
import static org.neo4j.cloud.storage.StorageUtils.WRITE_OPTIONS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.internal.matchers.ArrayEquals;
import org.mockito.internal.progress.ThreadSafeMockingProgress;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class SchemeFileSystemAbstractionTest {

    private static final String SCHEME = "testing";

    private static final Path FS_PATH = Path.of("/local/stuff");

    private StorageSystemProvider systemProvider;

    private FileSystemAbstraction fs;

    private SchemeFileSystemAbstraction schemeFs;

    private StoragePath schemePath;

    @BeforeEach
    void setup() {
        systemProvider = mock(StorageSystemProvider.class);
        when(systemProvider.getScheme()).thenReturn(SCHEME);

        final var storageSystem = mock(StorageSystem.class);
        when(storageSystem.scheme()).thenReturn(SCHEME);
        when(storageSystem.provider()).thenReturn(systemProvider);
        when(storageSystem.uriPrefix()).thenReturn(SCHEME + "://remote");

        when(systemProvider.create(any())).thenReturn(storageSystem);

        schemePath = new StoragePath(storageSystem, PathRepresentation.of("/stuff"));
        when(systemProvider.getPath(eq(URI.create(SCHEME + "://remote/stuff")))).thenReturn(schemePath);

        fs = mock(FileSystemAbstraction.class);

        final var providerFactory = new StorageSystemProviderFactory(SCHEME) {
            @Override
            public StorageSystemProvider createStorageSystemProvider(
                    ChunkChannelSupplier tempSupplier,
                    Config config,
                    InternalLogProvider logProvider,
                    MemoryTracker memoryTracker,
                    ClassLoader classLoader) {
                return systemProvider;
            }

            @Override
            protected String storageSystemProviderClass() {
                return "not used!";
            }
        };
        schemeFs = new SchemeFileSystemAbstraction(
                fs,
                Set.of(providerFactory),
                Config.defaults(),
                NullLogProvider.getInstance(),
                EmptyMemoryTracker.INSTANCE);
    }

    @Test
    void resolvableSchemes() {
        assertThat(schemeFs.resolvableSchemes()).containsExactlyInAnyOrder(SCHEME, "file");
    }

    @Test
    void canResolve() {
        assertThat(schemeFs.canResolve(URI.create(SCHEME + "://stuff")))
                .as("handled via the storage system")
                .isTrue();
        assertThat(schemeFs.canResolve(SCHEME + "://stuff"))
                .as("handled via the storage system")
                .isTrue();
        assertThat(schemeFs.canResolve(SCHEME.toUpperCase(Locale.ROOT) + "://stuff"))
                .as("handled via the storage system")
                .isTrue();

        assertThat(schemeFs.canResolve(URI.create("file:///stuff")))
                .as("handled via the fallback file system")
                .isTrue();
        assertThat(schemeFs.canResolve("file:///stuff"))
                .as("handled via the fallback file system")
                .isTrue();
        assertThat(schemeFs.canResolve("FILE:///stuff"))
                .as("handled via the fallback file system")
                .isTrue();
        assertThat(schemeFs.canResolve("/stuff"))
                .as("handled via the fallback file system")
                .isTrue();

        assertThat(schemeFs.canResolve(URI.create("boom://stuff")))
                .as("not handled via the storage system")
                .isFalse();
        assertThat(schemeFs.canResolve("boom://stuff"))
                .as("not handled via the storage system")
                .isFalse();
    }

    @Test
    void resolveNonFileSchemes() throws IOException {
        final var remotePath = SCHEME + "://remote/stuff";
        assertThat(schemeFs.resolve(remotePath)).isEqualTo(schemePath);
        assertThat(schemeFs.resolve(URI.create(remotePath))).isEqualTo(schemePath);

        final var otherPath = "other://remote/stuff";
        assertThatThrownBy(() -> schemeFs.resolve(otherPath)).isInstanceOf(ProviderMismatchException.class);
        assertThatThrownBy(() -> schemeFs.resolve(URI.create(otherPath))).isInstanceOf(ProviderMismatchException.class);

        verifyNoInteractions(fs);
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Windows prepends the Z directory as part of the URI resolution, i.e. local\\stuff != Z:\\local\\stuff")
    void resolveLocalPaths() throws IOException {
        assertThat(schemeFs.resolve(FS_PATH.toString())).isEqualTo(FS_PATH);
        assertThat(schemeFs.resolve(FS_PATH.toUri())).isEqualTo(FS_PATH);
        verifyNoInteractions(fs);
    }

    @Test
    void open() throws IOException {
        final var options = Set.<OpenOption>of(StandardOpenOption.READ);

        when(fs.open(eq(FS_PATH), eq(options))).thenReturn(mock(StoreChannel.class));
        when(systemProvider.newByteChannel(eq(schemePath), eq(options))).thenReturn(mock(SeekableByteChannel.class));

        assertThat(schemeFs.open(FS_PATH, options)).isNotNull().isNotInstanceOf(StorageChannel.class);
        assertThat(schemeFs.open(schemePath, options)).isInstanceOf(StorageChannel.class);
    }

    @Test
    void read() throws IOException {
        when(fs.read(eq(FS_PATH))).thenReturn(mock(StoreChannel.class));
        when(systemProvider.newByteChannel(eq(schemePath), eq(READ_OPTIONS)))
                .thenReturn(mock(SeekableByteChannel.class));

        assertThat(schemeFs.read(FS_PATH)).isNotNull().isNotInstanceOf(StorageChannel.class);
        assertThat(schemeFs.read(schemePath)).isNotNull().isInstanceOf(StorageChannel.class);
    }

    @Test
    void write() throws IOException {
        when(fs.write(eq(FS_PATH))).thenReturn(mock(StoreChannel.class));
        when(systemProvider.newByteChannel(eq(schemePath), eq(WRITE_OPTIONS)))
                .thenReturn(mock(SeekableByteChannel.class));

        assertThat(schemeFs.write(FS_PATH)).isNotNull().isNotInstanceOf(StorageChannel.class);
        assertThat(schemeFs.write(schemePath)).isNotNull().isInstanceOf(StorageChannel.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void openAsOutputStream(boolean append) throws IOException {
        final var options = (append ? APPEND_OPTIONS : WRITE_OPTIONS).toArray(OpenOption[]::new);

        when(fs.openAsOutputStream(eq(FS_PATH), eq(append))).thenReturn(mock(OutputStream.class));
        when(systemProvider.newOutputStream(eq(schemePath), eq(options))).thenReturn(mock(OutputStream.class));

        assertThat(schemeFs.openAsOutputStream(FS_PATH, append)).isNotNull();
        assertThat(schemeFs.openAsOutputStream(schemePath, append)).isNotNull();
    }

    @Test
    void openAsInputStream() throws IOException {
        when(fs.openAsInputStream(eq(FS_PATH))).thenReturn(mock(InputStream.class));
        when(systemProvider.newInputStream(eq(schemePath))).thenReturn(mock(InputStream.class));

        assertThat(schemeFs.openAsInputStream(FS_PATH)).isNotNull();
        assertThat(schemeFs.openAsInputStream(schemePath)).isNotNull();
    }

    @Test
    void truncate() throws IOException {
        final var size = 42L;

        final var channel = mock(SeekableByteChannel.class);
        when(systemProvider.newByteChannel(eq(schemePath), eq(WRITE_OPTIONS))).thenReturn(channel);

        schemeFs.truncate(FS_PATH, size);
        verify(fs).truncate(eq(FS_PATH), eq(size));

        schemeFs.truncate(schemePath, size);
        verify(channel).truncate(eq(size));
    }

    @Test
    void fileExists() throws Exception {
        verifyFileSystemCall("fileExists", FS_PATH);
        verifyFileSystemCall("fileExists", schemePath);
    }

    @Test
    void mkdir() throws Exception {
        verifyFileSystemCall("mkdir", FS_PATH);
        verifyFileSystemCall("mkdir", schemePath);
    }

    @Test
    void mkdirs() throws Exception {
        verifyFileSystemCall("mkdirs", FS_PATH);
        verifyFileSystemCall("mkdirs", schemePath);
    }

    @Test
    void getFileSize() throws Exception {
        verifyFileSystemCall("getFileSize", FS_PATH);
        verifyFileSystemCall("getFileSize", schemePath);
    }

    @Test
    void getBlockSize() throws Exception {
        verifyFileSystemCall("getBlockSize", FS_PATH);
        assertThatThrownBy(() -> schemeFs.getBlockSize(schemePath)).isInstanceOf(IOException.class);
    }

    @Test
    void delete() throws Exception {
        verifyFileSystemCall("delete", FS_PATH);
        verifyFileSystemCall("delete", schemePath);
    }

    @Test
    void deleteFile() throws Exception {
        verifyFileSystemCall("deleteFile", FS_PATH);
        verifyFileSystemCall("deleteFile", schemePath);
    }

    @Test
    void deleteRecursively() throws Exception {
        verifyFileSystemCall("deleteRecursively", FS_PATH);
        verifyFileSystemCall("deleteRecursively", schemePath);
    }

    @Test
    void deleteRecursivelyWithFilter() throws Exception {
        Predicate<Path> filter = (Path path) -> true;
        verifyFileSystemCall("deleteRecursively", FS_PATH, filter);
        verifyFileSystemCall("deleteRecursively", schemePath, filter);
    }

    @Test
    void renameFile() throws Exception {
        final var other = Path.of("/other");
        final var options = new StandardCopyOption[] {StandardCopyOption.REPLACE_EXISTING};
        verifyFileSystemCall("renameFile", FS_PATH, other, options);
        verifyFileSystemCall("renameFile", schemePath, other, options);
    }

    @Test
    void listFiles() throws Exception {
        verifyFileSystemCall("listFiles", FS_PATH);
        verifyFileSystemCall("listFiles", schemePath);
    }

    @Test
    void listFilesWithFilter() throws Exception {
        Filter<Path> filter = (Path path) -> true;
        verifyFileSystemCall("listFiles", FS_PATH, filter);
        verifyFileSystemCall("listFiles", schemePath, filter);
    }

    @Test
    void isDirectory() throws Exception {
        verifyFileSystemCall("isDirectory", FS_PATH);
        verifyFileSystemCall("isDirectory", schemePath);
    }

    @Test
    void moveToDirectory() throws Exception {
        final var other = Path.of("/other");
        verifyFileSystemCall("moveToDirectory", FS_PATH, other);
        verifyFileSystemCall("moveToDirectory", schemePath, other);
    }

    @Test
    void copyToDirectory() throws Exception {
        final var other = Path.of("/other");
        verifyFileSystemCall("copyToDirectory", FS_PATH, other);
        verifyFileSystemCall("copyToDirectory", schemePath, other);
    }

    @Test
    void copyFile() throws Exception {
        final var other = Path.of("/other");
        final var options = new StandardCopyOption[] {StandardCopyOption.REPLACE_EXISTING};
        verifyFileSystemCall("copyFile", FS_PATH, other, options);
        verifyFileSystemCall("copyFile", schemePath, other, options);
    }

    @Test
    void copyRecursively() throws Exception {
        final var other = Path.of("/other");
        verifyFileSystemCall("copyRecursively", FS_PATH, other);
        verifyFileSystemCall("copyRecursively", schemePath, other);
    }

    @Test
    void lastModifiedTime() throws Exception {
        verifyFileSystemCall("lastModifiedTime", FS_PATH);
        verifyFileSystemCall("lastModifiedTime", schemePath);
    }

    @Test
    void deleteFileOrThrow() throws Exception {
        verifyFileSystemCall("deleteFileOrThrow", FS_PATH);
        verifyFileSystemCall("deleteFileOrThrow", schemePath);
    }

    @Test
    void createTempDirectory() throws Exception {
        final var prefix = "prefix";
        verifyFileSystemCall("createTempDirectory", prefix);
    }

    @Test
    void createTempDirectoryWithDir() throws Exception {
        final var prefix = "prefix";
        verifyFileSystemCall("createTempDirectory", FS_PATH, prefix);
        verifyFileSystemCall("createTempDirectory", schemePath, prefix);
    }

    @Test
    void createTempFile() throws Exception {
        final var prefix = "prefix";
        final var suffix = "suffix";
        verifyFileSystemCall("createTempFile", prefix, suffix);
    }

    @Test
    void createTempFileWithDir() throws Exception {
        final var prefix = "prefix";
        final var suffix = "suffix";
        verifyFileSystemCall("createTempFile", FS_PATH, prefix, suffix);
        verifyFileSystemCall("createTempFile", schemePath, prefix, suffix);
    }

    @Test
    void isPersistent() {
        final var otherFs = new SchemeFileSystemAbstraction(
                fs, Set.of(), Config.defaults(), NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE);

        when(fs.isPersistent()).thenReturn(false);
        assertThat(otherFs.isPersistent())
                .as("no storage systems and fallback system is also not persistent")
                .isFalse();

        when(fs.isPersistent()).thenReturn(true);
        assertThat(schemeFs.isPersistent())
                .as("no storage systems but fallback system is persistent")
                .isTrue();

        when(fs.isPersistent()).thenReturn(false);
        assertThat(schemeFs.isPersistent())
                .as("All storage systems are persistent")
                .isTrue();
    }

    @Test
    void fileWatcher() {
        assertThatThrownBy(schemeFs::fileWatcher).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void closeNoProvidersCreated() throws IOException {
        schemeFs.close();

        verify(systemProvider, never()).close();
        verify(fs, never()).close();
    }

    @Test
    void close() throws IOException {
        assertThat(schemeFs.resolve(SCHEME + "://remote/stuff")).isNotNull();
        schemeFs.close();

        verify(systemProvider).close();
        verify(fs, never()).close();
    }

    private void verifyFileSystemCall(String methodName, Object... parameters) throws Exception {
        final var method = method(methodName, parameters.length);
        method.invoke(schemeFs, parameters);
        method.invoke(verify(fs), parameters);
        final var progress = ThreadSafeMockingProgress.mockingProgress();
        progress.getArgumentMatcherStorage().reportMatcher(new ArrayEquals(parameters));
        progress.reset();
    }

    private static Method method(String methodName, int paramCount) throws NoSuchMethodException {
        for (var method : FileSystemAbstraction.class.getDeclaredMethods()) {
            if (method.getName().equals(methodName) && method.getParameterCount() == paramCount) {
                return method;
            }
        }
        throw new NoSuchMethodException(methodName);
    }
}
