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
package org.neo4j.kernel.api.impl.schema.populator;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Collection;
import java.util.List;
import java.util.function.LongFunction;
import java.util.stream.LongStream;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.text.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexDirectoryStructure.Factory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class TextIndexPopulatingUpdaterIT {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDir;

    private static final IndexDescriptor INDEX_DESCRIPTOR = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 42))
            .withName("index_1")
            .materialise(1);

    @ParameterizedTest
    @EnumSource
    void shouldSampleAdditions(LuceneContext luceneContext) throws Exception {
        // Given
        TextIndexProvider provider = createIndexProvider(luceneContext);
        IndexPopulator populator = getPopulator(provider, INDEX_DESCRIPTOR);

        // When
        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            updater.process(add(1, INDEX_DESCRIPTOR, "foo"));
            updater.process(add(2, INDEX_DESCRIPTOR, "bar"));
            updater.process(add(3, INDEX_DESCRIPTOR, "baz"));
            updater.process(add(4, INDEX_DESCRIPTOR, "bar"));
        }

        // Then
        assertThat(populator.sample(NULL_CONTEXT)).isEqualTo(new IndexSample(4, 3, 4));
    }

    @ParameterizedTest
    @EnumSource
    void shouldSampleUpdates(LuceneContext luceneContext) throws Exception {
        // Given
        TextIndexProvider provider = createIndexProvider(luceneContext);
        IndexPopulator populator = getPopulator(provider, INDEX_DESCRIPTOR);

        // When
        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            updater.process(add(1, INDEX_DESCRIPTOR, "initial1"));
            updater.process(add(2, INDEX_DESCRIPTOR, "initial2"));
            updater.process(add(3, INDEX_DESCRIPTOR, "new2"));
            updater.process(change(1, INDEX_DESCRIPTOR, "initial1", "new1"));
            updater.process(change(1, INDEX_DESCRIPTOR, "initial2", "new2"));
        }

        // Then samples calculated with documents pending merge
        assertThat(populator.sample(NULL_CONTEXT)).isEqualTo(new IndexSample(3, 4, 5));
    }

    @ParameterizedTest
    @EnumSource
    void shouldSampleRemovals(LuceneContext luceneContext) throws Exception {
        // Given
        TextIndexProvider provider = createIndexProvider(luceneContext);
        IndexPopulator populator = getPopulator(provider, INDEX_DESCRIPTOR);

        // When
        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            updater.process(add(1, INDEX_DESCRIPTOR, "foo"));
            updater.process(add(2, INDEX_DESCRIPTOR, "bar"));
            updater.process(add(3, INDEX_DESCRIPTOR, "baz"));
            updater.process(add(4, INDEX_DESCRIPTOR, "qux"));
            updater.process(remove(1, INDEX_DESCRIPTOR, "foo"));
            updater.process(remove(2, INDEX_DESCRIPTOR, "bar"));
            updater.process(remove(4, INDEX_DESCRIPTOR, "qux"));
        }

        // Then samples calculated with documents pending merge
        assertThat(populator.sample(NULL_CONTEXT)).isEqualTo(new IndexSample(1, 4, 4));
    }

    @ParameterizedTest
    @EnumSource
    final void shouldIgnoreAddedUnsupportedValueTypes(LuceneContext luceneContext) throws Exception {
        // given  the population of an empty index
        Collection<IndexEntryUpdate> externalUpdates =
                generateUpdates(10, id -> EagerValueIndexEntryUpdate.add(id, INDEX_DESCRIPTOR, unsupportedValue(id)));
        // when   processing the addition of unsupported value types
        // then   updates should not have been indexed
        test(luceneContext, List.of(), externalUpdates, 0);
    }

    @ParameterizedTest
    @EnumSource
    final void shouldIgnoreRemovedUnsupportedValueTypes(LuceneContext luceneContext) throws Exception {
        // given  the population of an empty index
        Collection<IndexEntryUpdate> externalUpdates = generateUpdates(
                10, id -> EagerValueIndexEntryUpdate.remove(id, INDEX_DESCRIPTOR, unsupportedValue(id)));
        // when   processing the removal of unsupported value types
        // then   updates should not have been indexed
        test(luceneContext, List.of(), externalUpdates, 0);
    }

    @ParameterizedTest
    @EnumSource
    final void shouldIgnoreChangesBetweenUnsupportedValueTypes(LuceneContext luceneContext) throws Exception {
        // given  the population of an empty index
        Collection<IndexEntryUpdate> externalUpdates = generateUpdates(
                10,
                id -> EagerValueIndexEntryUpdate.change(
                        id, INDEX_DESCRIPTOR, unsupportedValue(id), unsupportedValue(id + 1)));
        // when   processing the change between unsupported value types
        // then   updates should not have been indexed
        test(luceneContext, List.of(), externalUpdates, 0);
    }

    @ParameterizedTest
    @EnumSource
    final void shouldNotIgnoreChangesUnsupportedValueTypesToSupportedValueTypes(LuceneContext luceneContext)
            throws Exception {
        // given  the population of an empty index
        Collection<IndexEntryUpdate> externalUpdates = generateUpdates(
                10,
                id -> EagerValueIndexEntryUpdate.change(
                        id, INDEX_DESCRIPTOR, unsupportedValue(id), supportedValue(id)));
        // when   processing the change from an unsupported to a supported value type
        // then   updates should have been indexed as additions
        test(luceneContext, List.of(), externalUpdates, externalUpdates.size());
    }

    @ParameterizedTest
    @EnumSource
    final void shouldNotIgnoreChangesSupportedValueTypesToUnsupportedValueTypes(LuceneContext luceneContext)
            throws Exception {
        // given  the population of an empty index
        Collection<IndexEntryUpdate> internalUpdates =
                generateUpdates(10, id1 -> EagerValueIndexEntryUpdate.add(id1, INDEX_DESCRIPTOR, supportedValue(id1)));
        Collection<IndexEntryUpdate> externalUpdates = generateUpdates(
                10,
                id -> EagerValueIndexEntryUpdate.change(
                        id, INDEX_DESCRIPTOR, supportedValue(id), unsupportedValue(id)));
        // when   processing the change from a supported to an unsupported value type
        // then   updates should have been indexed as removals
        test(luceneContext, internalUpdates, externalUpdates, 0);
    }

    private void test(
            LuceneContext luceneContext,
            Collection<IndexEntryUpdate> internalUpdates,
            Collection<IndexEntryUpdate> externalUpdates,
            long expectedIndexSize)
            throws Exception {

        TextIndexProvider provider = createIndexProvider(luceneContext);
        IndexPopulator populator = getPopulator(provider, INDEX_DESCRIPTOR);
        populator.add(internalUpdates, NULL_CONTEXT);

        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            for (IndexEntryUpdate update : externalUpdates) {
                updater.process(update);
            }
        }

        IndexSample sample = populator.sample(NULL_CONTEXT);
        assertThat(sample.indexSize()).isEqualTo(expectedIndexSize);
    }

    private static Value supportedValue(long i) {
        return Values.of("string_" + i);
    }

    private static Value unsupportedValue(long i) {
        return Values.of(i);
    }

    private static Collection<IndexEntryUpdate> generateUpdates(long n, LongFunction<IndexEntryUpdate> updateFunction) {
        return LongStream.range(0L, n).mapToObj(updateFunction).toList();
    }

    private IndexPopulator getPopulator(TextIndexProvider provider, SchemaDescriptorSupplier supplier)
            throws Exception {
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig(Config.defaults());
        IndexDescriptor index = forSchema(supplier.schema(), getIndexProviderDescriptor())
                .withName("some_name")
                .materialise(1);
        ByteBufferFactory bufferFactory = heapBufferFactory((int) kibiBytes(100));
        IndexPopulator populator = provider.getPopulator(
                index,
                samplingConfig,
                bufferFactory,
                INSTANCE,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        populator.create();
        return populator;
    }

    protected IndexProviderDescriptor getIndexProviderDescriptor() {
        return AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR;
    }

    private TextIndexProvider createIndexProvider(LuceneContext luceneContext) {
        DirectoryFactory directoryFactory = DirectoryFactory.inMemory(luceneContext);
        Factory directoryStructureFactory = directoriesByProvider(testDir.homePath());
        return new TextIndexProvider(
                fileSystem,
                directoryFactory,
                directoryStructureFactory,
                new Monitors(),
                Config.defaults(),
                writable(),
                NullLogProvider.getInstance());
    }
}
