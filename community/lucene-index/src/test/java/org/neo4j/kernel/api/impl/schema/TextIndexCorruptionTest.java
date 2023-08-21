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
package org.neo4j.kernel.api.impl.schema;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.apache.lucene.index.CorruptIndexException;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class TextIndexCorruptionTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    private final AssertableLogProvider logProvider = new AssertableLogProvider(true);
    private final IndexProvider.Monitor monitor = new LoggingMonitor(logProvider.getLog("test"));

    @Test
    void shouldRequestIndexPopulationIfTheIndexIsCorrupt() {
        // Given
        long faultyIndexId = 1;
        CorruptIndexException error = new CorruptIndexException("It's broken.", "");

        TextIndexProvider provider = newFaultyIndexProvider(faultyIndexId, error);

        // When
        IndexDescriptor descriptor = forSchema(forLabel(1, 1), provider.getProviderDescriptor())
                .withName("index_" + faultyIndexId)
                .materialise(faultyIndexId);
        InternalIndexState initialState = provider.getInitialState(descriptor, NULL_CONTEXT, Sets.immutable.empty());

        // Then
        assertThat(initialState).isEqualTo(InternalIndexState.POPULATING);
        assertThat(logProvider).containsException(error);
    }

    @Test
    void shouldRequestIndexPopulationFailingWithFileNotFoundException() {
        // Given
        long faultyIndexId = 1;
        NoSuchFileException error = new NoSuchFileException("/some/path/somewhere");

        TextIndexProvider provider = newFaultyIndexProvider(faultyIndexId, error);

        // When
        IndexDescriptor descriptor = forSchema(forLabel(1, 1), provider.getProviderDescriptor())
                .withName("index_" + faultyIndexId)
                .materialise(faultyIndexId);
        InternalIndexState initialState = provider.getInitialState(descriptor, NULL_CONTEXT, Sets.immutable.empty());

        // Then
        assertThat(initialState).isEqualTo(InternalIndexState.POPULATING);
        assertThat(logProvider).containsException(error);
    }

    @Test
    void shouldRequestIndexPopulationWhenFailingWithEOFException() {
        // Given
        long faultyIndexId = 1;
        EOFException error = new EOFException("/some/path/somewhere");

        TextIndexProvider provider = newFaultyIndexProvider(faultyIndexId, error);

        // When
        IndexDescriptor descriptor = forSchema(forLabel(1, 1), provider.getProviderDescriptor())
                .withName("index_" + faultyIndexId)
                .materialise(faultyIndexId);
        InternalIndexState initialState = provider.getInitialState(descriptor, NULL_CONTEXT, Sets.immutable.empty());

        // Then
        assertThat(initialState).isEqualTo(InternalIndexState.POPULATING);
        assertThat(logProvider).containsException(error);
    }

    private TextIndexProvider newFaultyIndexProvider(long faultyIndexId, Exception error) {
        DirectoryFactory directoryFactory = mock(DirectoryFactory.class);
        Path indexRootFolder = testDirectory.homePath();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(monitor);
        return new TextIndexProvider(
                fs, directoryFactory, directoriesByProvider(indexRootFolder), monitors, Config.defaults(), readOnly()) {
            @Override
            protected IndexStorageFactory buildIndexStorageFactory(
                    FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory) {
                FaultyIndexStorageFactory storageFactory =
                        new FaultyIndexStorageFactory(faultyIndexId, error, directoryFactory, directoryStructure());
                return storageFactory;
            }
        };
    }

    private class FaultyIndexStorageFactory extends IndexStorageFactory {
        final long faultyIndexId;
        final Exception error;

        FaultyIndexStorageFactory(
                long faultyIndexId,
                Exception error,
                DirectoryFactory directoryFactory,
                IndexDirectoryStructure directoryStructure) {
            super(directoryFactory, fs, directoryStructure);
            this.faultyIndexId = faultyIndexId;
            this.error = error;
        }

        @Override
        public PartitionedIndexStorage indexStorageOf(long indexId) {
            return indexId == faultyIndexId ? newFaultyPartitionedIndexStorage() : super.indexStorageOf(indexId);
        }

        PartitionedIndexStorage newFaultyPartitionedIndexStorage() {
            try {
                PartitionedIndexStorage storage = mock(PartitionedIndexStorage.class);
                when(storage.listFolders()).thenReturn(singletonList(Path.of("/some/path/somewhere/1")));
                when(storage.openDirectory(any())).thenThrow(error);
                return storage;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
