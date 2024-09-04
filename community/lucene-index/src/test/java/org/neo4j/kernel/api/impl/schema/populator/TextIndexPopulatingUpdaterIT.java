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
import org.junit.jupiter.api.Test;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class TextIndexPopulatingUpdaterIT {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDir;

    private static final SchemaDescriptorSupplier SCHEMA_DESCRIPTOR = () -> SchemaDescriptors.forLabel(1, 42);

    @Test
    void shouldSampleAdditions() throws Exception {
        // Given
        var provider = createIndexProvider();
        var populator = getPopulator(provider, SCHEMA_DESCRIPTOR);

        // When
        try (var updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            updater.process(add(1, SCHEMA_DESCRIPTOR, "foo"));
            updater.process(add(2, SCHEMA_DESCRIPTOR, "bar"));
            updater.process(add(3, SCHEMA_DESCRIPTOR, "baz"));
            updater.process(add(4, SCHEMA_DESCRIPTOR, "bar"));
        }

        // Then
        assertThat(populator.sample(NULL_CONTEXT)).isEqualTo(new IndexSample(4, 3, 4));
    }

    @Test
    void shouldSampleUpdates() throws Exception {
        // Given
        var provider = createIndexProvider();
        var populator = getPopulator(provider, SCHEMA_DESCRIPTOR);

        // When
        try (var updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            updater.process(add(1, SCHEMA_DESCRIPTOR, "initial1"));
            updater.process(add(2, SCHEMA_DESCRIPTOR, "initial2"));
            updater.process(add(3, SCHEMA_DESCRIPTOR, "new2"));
            updater.process(change(1, SCHEMA_DESCRIPTOR, "initial1", "new1"));
            updater.process(change(1, SCHEMA_DESCRIPTOR, "initial2", "new2"));
        }

        // Then samples calculated with documents pending merge
        assertThat(populator.sample(NULL_CONTEXT)).isEqualTo(new IndexSample(3, 4, 5));
    }

    @Test
    void shouldSampleRemovals() throws Exception {
        // Given
        var provider = createIndexProvider();
        var populator = getPopulator(provider, SCHEMA_DESCRIPTOR);

        // When
        try (var updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            updater.process(add(1, SCHEMA_DESCRIPTOR, "foo"));
            updater.process(add(2, SCHEMA_DESCRIPTOR, "bar"));
            updater.process(add(3, SCHEMA_DESCRIPTOR, "baz"));
            updater.process(add(4, SCHEMA_DESCRIPTOR, "qux"));
            updater.process(remove(1, SCHEMA_DESCRIPTOR, "foo"));
            updater.process(remove(2, SCHEMA_DESCRIPTOR, "bar"));
            updater.process(remove(4, SCHEMA_DESCRIPTOR, "qux"));
        }

        // Then samples calculated with documents pending merge
        assertThat(populator.sample(NULL_CONTEXT)).isEqualTo(new IndexSample(1, 4, 4));
    }

    @Test
    final void shouldIgnoreAddedUnsupportedValueTypes() throws Exception {
        // given  the population of an empty index
        final var externalUpdates =
                generateUpdates(10, id -> IndexEntryUpdate.add(id, SCHEMA_DESCRIPTOR, unsupportedValue(id)));
        // when   processing the addition of unsupported value types
        // then   updates should not have been indexed
        test(List.of(), externalUpdates, 0);
    }

    @Test
    final void shouldIgnoreRemovedUnsupportedValueTypes() throws Exception {
        // given  the population of an empty index
        final var externalUpdates =
                generateUpdates(10, id -> IndexEntryUpdate.remove(id, SCHEMA_DESCRIPTOR, unsupportedValue(id)));
        // when   processing the removal of unsupported value types
        // then   updates should not have been indexed
        test(List.of(), externalUpdates, 0);
    }

    @Test
    final void shouldIgnoreChangesBetweenUnsupportedValueTypes() throws Exception {
        // given  the population of an empty index
        final var externalUpdates = generateUpdates(
                10,
                id -> IndexEntryUpdate.change(id, SCHEMA_DESCRIPTOR, unsupportedValue(id), unsupportedValue(id + 1)));
        // when   processing the change between unsupported value types
        // then   updates should not have been indexed
        test(List.of(), externalUpdates, 0);
    }

    @Test
    final void shouldNotIgnoreChangesUnsupportedValueTypesToSupportedValueTypes() throws Exception {
        // given  the population of an empty index
        final var externalUpdates = generateUpdates(
                10, id -> IndexEntryUpdate.change(id, SCHEMA_DESCRIPTOR, unsupportedValue(id), supportedValue(id)));
        // when   processing the change from an unsupported to a supported value type
        // then   updates should have been indexed as additions
        test(List.of(), externalUpdates, externalUpdates.size());
    }

    @Test
    final void shouldNotIgnoreChangesSupportedValueTypesToUnsupportedValueTypes() throws Exception {
        // given  the population of an empty index
        final var internalUpdates =
                generateUpdates(10, id1 -> IndexEntryUpdate.add(id1, SCHEMA_DESCRIPTOR, supportedValue(id1)));
        final var externalUpdates = generateUpdates(
                10, id -> IndexEntryUpdate.change(id, SCHEMA_DESCRIPTOR, supportedValue(id), unsupportedValue(id)));
        // when   processing the change from a supported to an unsupported value type
        // then   updates should have been indexed as removals
        test(internalUpdates, externalUpdates, 0);
    }

    private void test(
            Collection<IndexEntryUpdate<?>> internalUpdates,
            Collection<IndexEntryUpdate<?>> externalUpdates,
            long expectedIndexSize)
            throws Exception {

        final var provider = createIndexProvider();
        final var populator = getPopulator(provider, SCHEMA_DESCRIPTOR);
        populator.add(internalUpdates, NULL_CONTEXT);

        try (var updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            for (final var update : externalUpdates) {
                updater.process(update);
            }
        }

        final var sample = populator.sample(NULL_CONTEXT);
        assertThat(sample.indexSize()).isEqualTo(expectedIndexSize);
    }

    private Value supportedValue(long i) {
        return Values.of("string_" + i);
    }

    private Value unsupportedValue(long i) {
        return Values.of(i);
    }

    private Collection<IndexEntryUpdate<?>> generateUpdates(long n, LongFunction<IndexEntryUpdate<?>> updateFunction) {
        return LongStream.range(0L, n).mapToObj(updateFunction).toList();
    }

    private IndexPopulator getPopulator(TextIndexProvider provider, SchemaDescriptorSupplier supplier)
            throws Exception {
        var samplingConfig = new IndexSamplingConfig(Config.defaults());
        var index = forSchema(supplier.schema(), getIndexProviderDescriptor())
                .withName("some_name")
                .materialise(1);
        var bufferFactory = heapBufferFactory((int) kibiBytes(100));
        var populator = provider.getPopulator(
                index,
                samplingConfig,
                bufferFactory,
                INSTANCE,
                mock(TokenNameLookup.class),
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        populator.create();
        return populator;
    }

    protected IndexProviderDescriptor getIndexProviderDescriptor() {
        return AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR;
    }

    private TextIndexProvider createIndexProvider() {
        var directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        var directoryStructureFactory = directoriesByProvider(testDir.homePath());
        return new TextIndexProvider(
                fileSystem, directoryFactory, directoryStructureFactory, new Monitors(), Config.defaults(), writable());
    }
}
