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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.PopulationProgress.DONE;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.async.AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.EagerValueIndexEntryUpdate.add;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.text.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure.Factory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.ElementIdMapper;

@TestDirectoryExtension
class LuceneIndexProviderTest {
    private static final IndexDescriptor descriptor = forSchema(
                    forLabel(1, 1), AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR)
            .withName("index_1")
            .materialise(1);

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDir;

    private Path graphDbDir;

    @BeforeEach
    void setup() {
        graphDbDir = testDir.homePath();
    }

    @ParameterizedTest
    @EnumSource
    void shouldFailToInvokePopulatorInReadOnlyMode(LuceneContext luceneContext) {
        Config config = Config.defaults();
        TextIndexProvider readOnlyIndexProvider =
                getLuceneIndexProvider(config, DirectoryFactory.inMemory(luceneContext), fileSystem, graphDbDir);
        assertThrows(
                UnsupportedOperationException.class,
                () -> readOnlyIndexProvider.getPopulator(
                        descriptor,
                        new IndexSamplingConfig(config),
                        heapBufferFactory(1024),
                        INSTANCE,
                        SIMPLE_TOKEN_LOOKUP,
                        ElementIdMapper.PLACEHOLDER,
                        Sets.immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY));
    }

    @ParameterizedTest
    @EnumSource
    void shouldCreateReadOnlyAccessorInReadOnlyMode(LuceneContext luceneContext) throws Exception {
        DirectoryFactory directoryFactory = DirectoryFactory.persistent(luceneContext);
        createEmptySchemaIndex(directoryFactory);

        Config readOnlyConfig = Config.defaults(read_only_database_default, true);
        TextIndexProvider readOnlyIndexProvider =
                getLuceneIndexProvider(readOnlyConfig, directoryFactory, fileSystem, graphDbDir);
        IndexAccessor onlineAccessor = getIndexAccessor(readOnlyConfig, readOnlyIndexProvider);

        assertThrows(UnsupportedOperationException.class, onlineAccessor::drop);
    }

    @ParameterizedTest
    @EnumSource
    void indexUpdateNotAllowedInReadOnlyMode(LuceneContext luceneContext) {
        Config readOnlyConfig = Config.defaults(read_only_database_default, true);
        TextIndexProvider readOnlyIndexProvider = getLuceneIndexProvider(
                readOnlyConfig, DirectoryFactory.inMemory(luceneContext), fileSystem, graphDbDir);

        assertThrows(
                UnsupportedOperationException.class,
                () -> getIndexAccessor(readOnlyConfig, readOnlyIndexProvider)
                        .newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false));
    }

    @ParameterizedTest
    @EnumSource
    void indexForceMustBeAllowedInReadOnlyMode(LuceneContext luceneContext) throws Exception {
        // IndexAccessor.force is used in check-pointing, and must be allowed in read-only mode as it would otherwise
        // prevent backups from working.
        Config readOnlyConfig = Config.defaults(read_only_database_default, true);
        TextIndexProvider readOnlyIndexProvider = getLuceneIndexProvider(
                readOnlyConfig, DirectoryFactory.inMemory(luceneContext), fileSystem, graphDbDir);

        // We assert that 'force' does not throw an exception
        getIndexAccessor(readOnlyConfig, readOnlyIndexProvider)
                .force(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
    }

    @ParameterizedTest
    @EnumSource
    void shouldHandleConcurrentUpdates(LuceneContext luceneContext) throws Throwable {
        // Given an active lucene index populator
        Config config = Config.defaults();
        TextIndexProvider provider = createIndexProvider(luceneContext, config);
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig(config);
        ByteBufferFactory bufferFactory = heapBufferFactory((int) kibiBytes(100));
        IndexPopulator populator = provider.getPopulator(
                descriptor,
                samplingConfig,
                bufferFactory,
                INSTANCE,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        Race race = new Race();

        // And the underlying index files are created
        populator.create();

        // When multiple threads are populating the index
        AtomicLong nextEntityId = new AtomicLong();
        race.addContestants(2, throwing(() -> {
            for (int value = 0; value < 3000; value++) {
                populator.add(
                        List.of(add(nextEntityId.getAndIncrement(), descriptor, stringValue(String.valueOf(value)))),
                        NULL_CONTEXT);
            }
        }));

        // And updated concurrently
        race.addContestant(throwing(() -> {
            try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
                for (int value = 0; value < 1000; value++) {
                    updater.process(EagerValueIndexEntryUpdate.change(
                            value, descriptor, stringValue(String.valueOf(value)), stringValue(String.valueOf(value))));
                }
            }
        }));
        race.go();

        // Then the index population completes
        assertThat(populator.progress(DONE).getCompleted()).isEqualTo(1);
        IndexSample sample = populator.sample(NULL_CONTEXT);
        assertThat(sample.sampleSize()).isBetween(6000L, 7000L);
        assertThat(sample.uniqueValues()).isEqualTo(3000L);
        assertThat(sample.indexSize()).isGreaterThanOrEqualTo(6000L);
        populator.close(true, NULL_CONTEXT);
    }

    private TextIndexProvider createIndexProvider(LuceneContext luceneContext, Config config) {
        DirectoryFactory directoryFactory = DirectoryFactory.inMemory(luceneContext);
        Factory directoryStructureFactory = directoriesByProvider(testDir.homePath());
        return new TextIndexProvider(
                fileSystem,
                directoryFactory,
                directoryStructureFactory,
                new Monitors(),
                config,
                writable(),
                NullLogProvider.getInstance());
    }

    private void createEmptySchemaIndex(DirectoryFactory directoryFactory) throws IOException {
        Config config = Config.defaults();
        TextIndexProvider indexProvider = getLuceneIndexProvider(config, directoryFactory, fileSystem, graphDbDir);
        IndexAccessor onlineAccessor = getIndexAccessor(config, indexProvider);
        onlineAccessor.close();
    }

    private static IndexAccessor getIndexAccessor(Config readOnlyConfig, TextIndexProvider indexProvider)
            throws IOException {
        return indexProvider.getOnlineAccessor(
                descriptor,
                new IndexSamplingConfig(readOnlyConfig),
                SIMPLE_TOKEN_LOOKUP,
                ElementIdMapper.PLACEHOLDER,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
    }

    private static TextIndexProvider getLuceneIndexProvider(
            Config config, DirectoryFactory directoryFactory, FileSystemAbstraction fs, Path graphDbDir) {
        return new TextIndexProvider(
                fs,
                directoryFactory,
                directoriesByProvider(graphDbDir),
                new Monitors(),
                config,
                readOnly(),
                NullLogProvider.getInstance());
    }
}
