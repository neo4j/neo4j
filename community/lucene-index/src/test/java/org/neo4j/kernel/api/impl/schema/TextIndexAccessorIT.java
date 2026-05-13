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

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.async.AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.EagerValueIndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory.DatabaseIdResolver;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.text.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueType;

@RandomSupportExtension
@TestDirectoryExtension
public class TextIndexAccessorIT {

    private static final ValueType[] SUPPORTED_TYPES = Stream.of(ValueType.ALL_TYPES)
            .filter(type -> type.valueGroup.category() == ValueCategory.TEXT)
            .toArray(ValueType[]::new);

    private static final ValueType[] UNSUPPORTED_TYPES = Stream.of(ValueType.ALL_TYPES)
            .filter(type -> type.valueGroup.category() != ValueCategory.TEXT)
            .toArray(ValueType[]::new);

    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory directory;

    private final LifeSupport life = new LifeSupport();
    private TextIndexProvider indexProvider;
    private IndexSamplingConfig samplingConfig;
    private Config config;

    void setUp(LuceneContext luceneContext) {
        Path path = directory.directory("db");
        NamedDatabaseId defaultDatabaseId = DatabaseIdFactory.from(
                DEFAULT_DATABASE_NAME, UUID.randomUUID()); // UUID required, but ignored by config lookup
        config = Config.defaults();
        DatabaseIdResolver databaseIdResolver = mock(ConfigBasedLookupFactory.DatabaseIdResolver.class);
        Mockito.when(databaseIdResolver.resolve(DEFAULT_DATABASE_NAME))
                .thenReturn(Optional.of(defaultDatabaseId.databaseId()));
        ConfigBasedLookupFactory readOnlyLookup = new ConfigBasedLookupFactory(config, databaseIdResolver);
        DefaultReadOnlyDatabases globalChecker = new DefaultReadOnlyDatabases(readOnlyLookup);
        ConfigReadOnlyDatabaseListener listener = new ConfigReadOnlyDatabaseListener(globalChecker, config);
        DatabaseReadOnlyChecker readOnlyChecker = globalChecker.forDatabase(defaultDatabaseId);
        indexProvider = new TextIndexProvider(
                directory.getFileSystem(),
                DirectoryFactory.persistent(luceneContext),
                directoriesByProvider(path),
                new Monitors(),
                config,
                readOnlyChecker,
                NullLogProvider.getInstance());
        life.add(listener);
        life.add(indexProvider);
        life.start();
        samplingConfig = new IndexSamplingConfig(config);
    }

    @AfterEach
    void close() {
        life.shutdown();
    }

    @ParameterizedTest
    @EnumSource
    void shouldIterateAllDocumentsEvenWhenContainingDeletions(LuceneContext luceneContext) throws Exception {
        setUp(luceneContext);
        // given
        int nodes = 100;
        MutableLongSet expectedNodes = LongSets.mutable.withInitialCapacity(nodes);
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("TestIndex")
                .materialise(99);
        populateWithInitialNodes(indexDescriptor, nodes, expectedNodes);
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                indexDescriptor,
                samplingConfig,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // when
            removeSomeNodes(indexDescriptor, nodes / 2, accessor, expectedNodes);

            // then
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
                MutableLongSet readIds = LongSets.mutable.empty();
                reader.forEach(readIds::add);
                assertThat(readIds).isEqualTo(expectedNodes);
            }
        }
    }

    @ParameterizedTest
    @EnumSource
    void failToAcquireIndexWriterWhileReadOnly(LuceneContext luceneContext) throws Exception {
        setUp(luceneContext);
        int nodes = 100;
        MutableLongSet expectedNodes = LongSets.mutable.withInitialCapacity(nodes);
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("TestIndex")
                .materialise(99);
        populateWithInitialNodes(indexDescriptor, nodes, expectedNodes);
        config.set(GraphDatabaseSettings.read_only_databases, Set.of(DEFAULT_DATABASE_NAME));
        try (IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor(
                indexDescriptor,
                samplingConfig,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> onlineAccessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false));
        }
    }

    @ParameterizedTest
    @EnumSource
    void shouldIterateAllDocumentsEvenWhenContainingDeletionsInOnlySomeLeaves(LuceneContext luceneContext)
            throws Exception {
        setUp(luceneContext);
        // given
        int nodes = 300_000;
        MutableLongSet expectedNodes = LongSets.mutable.empty();
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2, 3, 4, 5))
                .withName("TestIndex")
                .materialise(99);
        populateWithInitialNodes(indexDescriptor, nodes, expectedNodes);
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                indexDescriptor,
                samplingConfig,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // when
            removeSomeNodes(indexDescriptor, 2, accessor, expectedNodes);

            // then
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
                MutableLongSet readIds = LongSets.mutable.empty();
                reader.forEach(readIds::add);
                assertThat(readIds).isEqualTo(expectedNodes);
            }
        }
    }

    @ParameterizedTest
    @EnumSource
    void shouldReadAllDocumentsInSchemaIndexAfterRandomAdditionsAndDeletions(LuceneContext luceneContext)
            throws Exception {
        setUp(luceneContext);
        // given
        IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 1))
                .withName("test")
                .materialise(1);
        TokenNameLookup tokenNameLookup = mock(TokenNameLookup.class);
        populateWithInitialNodes(descriptor, 0, new LongHashSet());
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                samplingConfig,
                tokenNameLookup,
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // when
            BitSet expectedEntities = writeRandomThings(accessor, descriptor);
            int expectedCount = expectedEntities.cardinality();

            // then
            int count = 0;
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
                for (Long entityId : reader) {
                    count++;
                    assertThat(expectedEntities.get(toIntExact(entityId))).isTrue();
                }
            }
            assertThat(count).isEqualTo(expectedCount);
        }
    }

    @ParameterizedTest
    @EnumSource
    void shouldPartitionAndReadAllDocuments(LuceneContext luceneContext) throws Exception {
        setUp(luceneContext);
        // given
        IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 1))
                .withName("test")
                .materialise(1);
        MutableLongSet expectedNodes = new LongHashSet();
        populateWithInitialNodes(descriptor, random.nextInt(1_000, 10_000), expectedNodes);

        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                samplingConfig,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // when
            MutableLongSet readNodes = new LongHashSet();
            IndexEntriesReader[] partitionReaders =
                    accessor.newAllEntriesValueReader(random.nextInt(2, 16), NULL_CONTEXT);
            for (IndexEntriesReader partitionReader : partitionReaders) {
                while (partitionReader.hasNext()) {
                    boolean added = readNodes.add(partitionReader.next());
                    assertThat(added).isTrue();
                }
                partitionReader.close();
            }
            assertThat(readNodes).isEqualTo(expectedNodes);
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void updaterShouldIgnoreUnsupportedTypes(LuceneContext luceneContext, ValueType unsupportedType) throws Exception {
        setUp(luceneContext);
        IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 1))
                .withName("test")
                .materialise(1);
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                samplingConfig,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // given  an empty index
            // when   an unsupported value type is added
            try (IndexUpdater updater =
                    accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                Value unsupportedValue = random.randomValues().nextValueOfType(unsupportedType);
                updater.process(
                        EagerValueIndexEntryUpdate.add(idGenerator().getAsLong(), descriptor, unsupportedValue));
            }

            // then   it should not be indexed, and thus not visible
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
                assertThat(reader).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void updaterShouldChangeUnsupportedToSupportedByAdd(LuceneContext luceneContext, ValueType unsupportedType)
            throws Exception {
        setUp(luceneContext);
        IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 1))
                .withName("test")
                .materialise(1);
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                samplingConfig,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // when   an unsupported value type is added
            long entityId = idGenerator().getAsLong();
            Value unsupportedValue = random.randomValues().nextValueOfType(unsupportedType);
            try (IndexUpdater updater =
                    accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                updater.process(EagerValueIndexEntryUpdate.add(entityId, descriptor, unsupportedValue));
            }

            // then   it should not be indexed, and thus not visible
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
                assertThat(reader).isEmpty();
            }

            // when   the unsupported value type is changed to a supported value type
            try (IndexUpdater updater =
                    accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                Value supportedValue = random.randomValues().nextValueOfTypes(SUPPORTED_TYPES);
                updater.process(
                        EagerValueIndexEntryUpdate.change(entityId, descriptor, unsupportedValue, supportedValue));
            }

            // then   it should be added to the index, and thus now visible
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
                assertThat(reader).containsExactlyInAnyOrder(entityId);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void updaterShouldChangeSupportedToUnsupportedByRemove(LuceneContext luceneContext, ValueType unsupportedType)
            throws Exception {
        setUp(luceneContext);
        IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 1))
                .withName("test")
                .materialise(1);
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                samplingConfig,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // given  an empty index
            // when   a supported value type is added
            long entityId = idGenerator().getAsLong();
            Value supportedValue = random.randomValues().nextValueOfTypes(SUPPORTED_TYPES);
            try (IndexUpdater updater =
                    accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                updater.process(EagerValueIndexEntryUpdate.add(entityId, descriptor, supportedValue));
            }

            // then   it should be added to the index, and thus visible
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
                assertThat(reader).containsExactlyInAnyOrder(entityId);
            }

            // when   the supported value type is changed to an unsupported value type
            try (IndexUpdater updater =
                    accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                Value unsupportedValue = random.randomValues().nextValueOfType(unsupportedType);
                updater.process(
                        EagerValueIndexEntryUpdate.change(entityId, descriptor, supportedValue, unsupportedValue));
            }

            // then   it should be removed from the index, and thus no longer visible
            try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
                assertThat(reader).isEmpty();
            }
        }
    }

    private static void removeSomeNodes(
            IndexDescriptor indexDescriptor, int nodes, IndexAccessor accessor, MutableLongSet expectedNodes)
            throws IndexEntryConflictException {
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            for (long id = 0; id < nodes; id++) {
                updater.process(EagerValueIndexEntryUpdate.remove(id, indexDescriptor, values(indexDescriptor, id)));
                expectedNodes.remove(id);
            }
        }
    }

    private void populateWithInitialNodes(IndexDescriptor indexDescriptor, int nodes, MutableLongSet expectedNodes)
            throws IndexEntryConflictException, IOException {
        IndexPopulator populator = indexProvider.getPopulator(
                indexDescriptor,
                samplingConfig,
                ByteBufferFactory.heapBufferFactory((int) kibiBytes(100)),
                INSTANCE,
                mock(TokenNameLookup.class),
                ElementIdMapper.PLACEHOLDER,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        Collection<IndexEntryUpdate> initialData = new ArrayList<>();
        populator.create();
        for (long id = 0; id < nodes; id++) {
            Value[] values = values(indexDescriptor, id);
            initialData.add(add(id, indexDescriptor, values));
            expectedNodes.add(id);
            if (initialData.size() >= 100) {
                populator.add(initialData, NULL_CONTEXT);
                initialData.clear();
            }
        }
        if (!initialData.isEmpty()) {
            populator.add(initialData, NULL_CONTEXT);
        }
        populator.scanCompleted(nullInstance, mock(IndexPopulator.PopulationWorkScheduler.class), NULL_CONTEXT);
        populator.close(true, NULL_CONTEXT);
    }

    private static Value[] values(IndexDescriptor properties, long id) {
        int numProperties = properties.schema().getPropertyIds().length;
        Value[] values = new Value[numProperties];
        for (int i = 0; i < numProperties; i++) {
            values[i] = value(id * numProperties + i);
        }
        return values;
    }

    private static TextValue value(long id) {
        return stringValue("string_" + id);
    }

    private BitSet writeRandomThings(IndexAccessor index, IndexDescriptor descriptor)
            throws IndexEntryConflictException {
        int rounds = 200;
        int updatesPerRound = 200;
        BitSet liveEntityIds = new BitSet(rounds * updatesPerRound);
        MutableLong highEntityId = new MutableLong();
        for (int i = 0; i < rounds; i++) {
            try (IndexUpdater updater = index.newUpdater(IndexUpdateMode.RECOVERY, NULL_CONTEXT, false)) {
                for (int j = 0; j < updatesPerRound; j++) {
                    IndexEntryUpdate update = randomUpdate(highEntityId, liveEntityIds, descriptor, random.random());
                    updater.process(update);
                }
            }
            if (random.nextInt(100) == 0) {
                index.force(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
            }
        }
        index.force(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
        return liveEntityIds;
    }

    private static IndexEntryUpdate randomUpdate(
            MutableLong highEntityId, BitSet liveEntityIds, IndexDescriptor descriptor, Random random) {
        if (highEntityId.longValue() > 0 && random.nextInt(10) == 0) {
            long entityId = -1;
            for (int i = 0; i < 10; i++) {
                long tentativeEntityId = random.nextInt(highEntityId.intValue());
                if (liveEntityIds.get(toIntExact(tentativeEntityId))) {
                    entityId = tentativeEntityId;
                    break;
                }
            }
            if (entityId != -1) {
                liveEntityIds.clear(toIntExact(entityId));
                return EagerValueIndexEntryUpdate.remove(entityId, descriptor, stringValue(String.valueOf(entityId)));
            }
        }

        long entityId = highEntityId.getAndIncrement();
        liveEntityIds.set(toIntExact(entityId));
        return EagerValueIndexEntryUpdate.add(entityId, descriptor, stringValue(String.valueOf(entityId)));
    }

    private static LongSupplier idGenerator() {
        return new AtomicLong(0)::incrementAndGet;
    }

    private static List<Arguments> unsupportedTypes() {
        List<Arguments> arguments = new ArrayList<>();
        for (LuceneContext luceneContext : LuceneContext.values()) {
            for (ValueType unsupportedType : UNSUPPORTED_TYPES) {
                arguments.add(Arguments.of(luceneContext, unsupportedType));
            }
        }
        return arguments;
    }
}
