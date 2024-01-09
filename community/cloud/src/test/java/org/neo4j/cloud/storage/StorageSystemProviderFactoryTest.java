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
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier;
import org.neo4j.configuration.Config;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class StorageSystemProviderFactoryTest {

    private static final String SCHEME = "testing";

    private final TestStorageSystemProviderFactory factory = new TestStorageSystemProviderFactory();

    @Test
    void scheme() {
        assertThat(factory.scheme()).isEqualTo(SCHEME);
        assertThat(factory.matches(SCHEME)).isTrue();
        assertThat(factory.matches("something")).isFalse();
    }

    @Test
    void createStorageSystemProvider() {
        final var tempSupplier = mock(ChunkChannelSupplier.class);
        final var config = Config.defaults();
        final var classLoader = ClassLoader.getSystemClassLoader();
        assertThat(factory.createStorageSystemProvider(
                        tempSupplier, config, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE, classLoader))
                .isNotNull();

        factory.setProviderClass(String.class.getName());
        assertThatThrownBy(() -> factory.createStorageSystemProvider(
                        tempSupplier, config, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE, classLoader))
                .isInstanceOf(UncheckedIOException.class);
    }

    private static class TestStorageSystemProviderFactory extends StorageSystemProviderFactory {

        private String providerClass = TestStorageSystemProvider.class.getName();

        private TestStorageSystemProviderFactory() {
            super(SCHEME);
        }

        private void setProviderClass(String providerClass) {
            this.providerClass = providerClass;
        }

        @Override
        protected String storageSystemProviderClass() {
            return providerClass;
        }
    }

    public static class TestStorageSystemProvider extends StorageSystemProvider {

        public TestStorageSystemProvider(
                ChunkChannelSupplier tempSupplier,
                Config config,
                InternalLogProvider logProvider,
                MemoryTracker memoryTracker) {
            super(SCHEME, tempSupplier, config, logProvider, memoryTracker);
        }

        @Override
        public SeekableByteChannel newByteChannel(StoragePath path, Set<? extends OpenOption> options) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected OutputStream openAsOutputStream(StoragePath fileName, Set<? extends OpenOption> options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream openAsInputStream(StoragePath fileName) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected StorageSystem create(URI storageUri) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected StorageLocation resolve(URI uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDirectory(Path dir, FileAttribute<?>... attrs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copy(Path source, Path target, CopyOption... options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkAccess(Path path, AccessMode... modes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) {
            throw new UnsupportedOperationException();
        }
    }
}
