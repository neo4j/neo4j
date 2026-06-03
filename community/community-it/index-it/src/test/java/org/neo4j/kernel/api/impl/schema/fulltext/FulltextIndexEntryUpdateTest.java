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
package org.neo4j.kernel.api.impl.schema.fulltext;

import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.neo4j.common.EmptyDependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory.DatabaseIdResolver;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SemanticSearchSchemaDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.KernelVersionProviders;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.FulltextV2IndexProviderFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith(SoftAssertionsExtension.class)
class FulltextIndexEntryUpdateTest {
    private static final Config CONFIG = Config.defaults();
    private static final IndexSamplingConfig SAMPLING_CONFIG = new IndexSamplingConfig(CONFIG);

    private final LifeSupport life = new LifeSupport();
    private final TokenHolders tokenHolders = new TokenHolders(
            new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_PROPERTY_KEY),
            new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_LABEL),
            new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_RELATIONSHIP_TYPE));
    private final IndexPopulator.PopulationWorkScheduler populationWorkScheduler =
            new IndexPopulator.PopulationWorkScheduler() {
                @Override
                public <T> JobHandle<T> schedule(
                        IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job) {
                    return jobScheduler.schedule(
                            Group.INDEX_POPULATION_WORK, new JobMonitoringParams(null, null, null), job);
                }
            };

    private IndexProvider provider;
    private IndexDescriptor index;
    private JobScheduler jobScheduler;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @InjectSoftAssertions
    private SoftAssertions softly;

    @BeforeEach
    final void setup() {
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer();
        CursorContextFactory contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
        NamedDatabaseId defaultDatabaseId = DatabaseIdFactory.from(
                DEFAULT_DATABASE_NAME, UUID.randomUUID()); // UUID required, but ignored by config lookup
        DatabaseIdResolver databaseIdResolver = mock(ConfigBasedLookupFactory.DatabaseIdResolver.class);
        Mockito.when(databaseIdResolver.resolve(DEFAULT_DATABASE_NAME))
                .thenReturn(Optional.of(defaultDatabaseId.databaseId()));
        ConfigBasedLookupFactory configBasedLookup = new ConfigBasedLookupFactory(CONFIG, databaseIdResolver);
        DefaultReadOnlyDatabases readOnlyDatabases = new DefaultReadOnlyDatabases(configBasedLookup);
        DatabaseReadOnlyChecker readOnlyChecker = readOnlyDatabases.forDatabase(defaultDatabaseId);
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        provider = new FulltextV2IndexProviderFactory()
                .create(
                        pageCache,
                        fs,
                        NullLogService.getInstance(),
                        new Monitors(),
                        CONFIG,
                        KernelVersionProviders.latestFromConfig(CONFIG),
                        readOnlyChecker,
                        HostedOnMode.SINGLE,
                        RecoveryCleanupWorkCollector.ignore(),
                        databaseLayout,
                        tokenHolders,
                        jobScheduler,
                        contextFactory,
                        cacheTracer,
                        EmptyDependencyResolver.EMPTY_RESOLVER);
        life.add(provider);
        life.start();

        SemanticSearchSchemaDescriptor schema =
                SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {123}, new int[] {321});
        index = provider.completeConfiguration(
                IndexPrototype.forSchema(schema)
                        .withIndexType(provider.getIndexType())
                        .withIndexProvider(provider.getProviderDescriptor())
                        .withName("FulltextIndex")
                        .materialise(0),
                StorageEngineIndexingBehaviour.EMPTY);
    }

    @AfterEach
    final void teardown() throws Exception {
        life.shutdown();
        jobScheduler.shutdown();
    }

    // Populator

    @Test
    final void populatorShouldNotIgnoreSupportedValueTypes() throws Exception {
        Collection<Long> ids = generateIds(0, 10);
        Collection<EagerValueIndexEntryUpdate> updates =
                generateUpdates(ids, id -> EagerValueIndexEntryUpdate.add(id, index, supportedValue(id)));
        populatorTest(updates, ids);
    }

    @Test
    final void populatorShouldIgnoreUnsupportedValueTypes() throws Exception {
        Collection<Long> ids = generateIds(0, 10);
        Collection<EagerValueIndexEntryUpdate> updates =
                generateUpdates(ids, id -> EagerValueIndexEntryUpdate.add(id, index, unsupportedValue(id)));
        populatorTest(updates, List.of());
    }

    private void populatorTest(Collection<EagerValueIndexEntryUpdate> updates, Iterable<Long> expectedIds)
            throws Exception {
        IndexPopulator populator = getPopulator();
        try {
            populator.add(updates, CursorContext.NULL_CONTEXT);
            completePopulation(populator);
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
        assertIndexed(expectedIds);
    }

    // PopulatingUpdater

    @Test
    final void populatingUpdaterShouldNotIgnoreAddedSupportedValueType() throws Exception {
        Collection<Long> ids = generateIds(0, 10);
        Collection<EagerValueIndexEntryUpdate> updates =
                generateUpdates(ids, id -> EagerValueIndexEntryUpdate.add(id, index, supportedValue(id)));
        populatingUpdaterTest(updates, ids);
    }

    @Test
    final void populatingUpdaterShouldIgnoreAddedUnsupportedValueType() throws Exception {
        Collection<Long> ids = generateIds(0, 10);
        Collection<EagerValueIndexEntryUpdate> updates =
                generateUpdates(ids, id -> EagerValueIndexEntryUpdate.add(id, index, unsupportedValue(id)));
        populatingUpdaterTest(updates, List.of());
    }

    @Test
    final void populatingUpdaterShouldNotIgnoreRemovedSupportedValueType() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> removedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, supportedValue(id))),
                        generateUpdates(
                                removedIds, id -> EagerValueIndexEntryUpdate.remove(id, index, supportedValue(id))))
                .flatMap(Collection::stream)
                .toList();

        Set<Long> expectedIds = new HashSet<>(addedIds);
        expectedIds.removeAll(removedIds);
        populatingUpdaterTest(updates, expectedIds);
    }

    @Test
    final void populatingUpdaterShouldIgnoreRemovedUnsupportedValueType() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> removedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(
                                addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, unsupportedValue(id))),
                        generateUpdates(
                                removedIds, id -> EagerValueIndexEntryUpdate.remove(id, index, unsupportedValue(id))))
                .flatMap(Collection::stream)
                .toList();
        populatingUpdaterTest(updates, List.of());
    }

    @Test
    final void populatingUpdaterShouldNotIgnoreChangedBetweenSupportedValueTypes() throws Exception {
        Collection<Long> ids = generateIds(0, 20);
        Collection<EagerValueIndexEntryUpdate> updates = generateUpdates(
                ids, id -> EagerValueIndexEntryUpdate.change(id, index, supportedValue(id), supportedValue(id + 1)));
        populatingUpdaterTest(updates, ids);
    }

    @Test
    final void populatingUpdaterShouldTreatChangedFromUnsupportedToSupportedValueTypesAsAdded() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> changedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(
                                addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, unsupportedValue(id))),
                        generateUpdates(
                                changedIds,
                                id -> EagerValueIndexEntryUpdate.change(
                                        id, index, unsupportedValue(id), supportedValue(id))))
                .flatMap(Collection::stream)
                .toList();
        populatingUpdaterTest(updates, changedIds);
    }

    @Test
    final void populatingUpdaterShouldTreatChangedFromSupportedToUnsupportedValueTypesAsRemoved() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> changedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, supportedValue(id))),
                        generateUpdates(
                                changedIds,
                                id -> EagerValueIndexEntryUpdate.change(
                                        id, index, supportedValue(id), unsupportedValue(id))))
                .flatMap(Collection::stream)
                .toList();

        Set<Long> expectedIds = new HashSet<>(addedIds);
        expectedIds.removeAll(changedIds);
        populatingUpdaterTest(updates, expectedIds);
    }

    @Test
    final void populatingUpdaterShouldIgnoreChangedBetweenUnsupportedValueTypes() throws Exception {
        Collection<Long> ids = generateIds(0, 20);
        Collection<EagerValueIndexEntryUpdate> updates = generateUpdates(
                ids,
                id -> EagerValueIndexEntryUpdate.change(id, index, unsupportedValue(id), unsupportedValue(id + 1)));
        populatingUpdaterTest(updates, List.of());
    }

    private void populatingUpdaterTest(Iterable<EagerValueIndexEntryUpdate> updates, Iterable<Long> expectedIds)
            throws Exception {
        IndexPopulator populator = getPopulator();
        try (IndexUpdater updater = getPopulatingUpdater(populator)) {
            for (EagerValueIndexEntryUpdate update : updates) {
                updater.process(update);
            }
            completePopulation(populator);
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
        assertIndexed(expectedIds);
    }

    // Updater

    @Test
    final void updaterShouldNotIgnoreAddedSupportedValueType() throws Exception {
        Collection<Long> ids = generateIds(0, 10);
        Collection<EagerValueIndexEntryUpdate> updates =
                generateUpdates(ids, id -> EagerValueIndexEntryUpdate.add(id, index, supportedValue(id)));
        updaterTest(updates, ids);
    }

    @Test
    final void updaterShouldIgnoreAddedUnsupportedValueType() throws Exception {
        Collection<Long> ids = generateIds(0, 10);
        Collection<EagerValueIndexEntryUpdate> updates =
                generateUpdates(ids, id -> EagerValueIndexEntryUpdate.add(id, index, unsupportedValue(id)));
        updaterTest(updates, List.of());
    }

    @Test
    final void updaterShouldNotIgnoreRemovedSupportedValueType() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> removedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, supportedValue(id))),
                        generateUpdates(
                                removedIds, id -> EagerValueIndexEntryUpdate.remove(id, index, supportedValue(id))))
                .flatMap(Collection::stream)
                .toList();

        Set<Long> expectedIds = new HashSet<>(addedIds);
        expectedIds.removeAll(removedIds);
        updaterTest(updates, expectedIds);
    }

    @Test
    final void updaterShouldIgnoreRemovedUnsupportedValueType() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> removedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(
                                addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, unsupportedValue(id))),
                        generateUpdates(
                                removedIds, id -> EagerValueIndexEntryUpdate.remove(id, index, unsupportedValue(id))))
                .flatMap(Collection::stream)
                .toList();
        updaterTest(updates, List.of());
    }

    @Test
    final void updaterShouldNotIgnoreChangedBetweenSupportedValueTypes() throws Exception {
        Collection<Long> ids = generateIds(0, 20);
        Collection<EagerValueIndexEntryUpdate> updates = generateUpdates(
                ids, id -> EagerValueIndexEntryUpdate.change(id, index, supportedValue(id), supportedValue(id + 1)));
        updaterTest(updates, ids);
    }

    @Test
    final void updaterShouldTreatChangedFromUnsupportedToSupportedValueTypesAsAdded() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> changedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(
                                addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, unsupportedValue(id))),
                        generateUpdates(
                                changedIds,
                                id -> EagerValueIndexEntryUpdate.change(
                                        id, index, unsupportedValue(id), supportedValue(id))))
                .flatMap(Collection::stream)
                .toList();
        updaterTest(updates, changedIds);
    }

    @Test
    final void updaterShouldTreatChangedFromSupportedToUnsupportedValueTypesAsRemoved() throws Exception {
        Collection<Long> addedIds = generateIds(0, 20);
        Collection<Long> changedIds = generateIds(11, 17);
        List<EagerValueIndexEntryUpdate> updates = Stream.of(
                        generateUpdates(addedIds, id -> EagerValueIndexEntryUpdate.add(id, index, supportedValue(id))),
                        generateUpdates(
                                changedIds,
                                id -> EagerValueIndexEntryUpdate.change(
                                        id, index, supportedValue(id), unsupportedValue(id))))
                .flatMap(Collection::stream)
                .toList();

        Set<Long> expectedIds = new HashSet<>(addedIds);
        expectedIds.removeAll(changedIds);
        updaterTest(updates, expectedIds);
    }

    @Test
    final void updaterShouldIgnoreChangedBetweenUnsupportedValueTypes() throws Exception {
        Collection<Long> ids = generateIds(0, 20);
        Collection<EagerValueIndexEntryUpdate> updates = generateUpdates(
                ids,
                id -> EagerValueIndexEntryUpdate.change(id, index, unsupportedValue(id), unsupportedValue(id + 1)));
        updaterTest(updates, List.of());
    }

    private void updaterTest(Iterable<EagerValueIndexEntryUpdate> updates, Iterable<Long> expectedIds)
            throws Exception {
        try (IndexAccessor accessor = getAccessor();
                IndexUpdater updater = getUpdater(accessor)) {
            for (EagerValueIndexEntryUpdate update : updates) {
                updater.process(update);
            }
        }
        assertIndexed(expectedIds);
    }

    private static Collection<EagerValueIndexEntryUpdate> generateUpdates(
            Collection<Long> ids, Function<Long, EagerValueIndexEntryUpdate> toUpdate) {
        return ids.stream().map(toUpdate).toList();
    }

    private static Collection<Long> generateIds(long from, long to) {
        return LongStream.range(from, to).boxed().collect(Collectors.toUnmodifiableSet());
    }

    private static Value supportedValue(long i) {
        return Values.of("string_" + i);
    }

    private static Value unsupportedValue(long i) {
        return Values.of(i);
    }

    private IndexPopulator getPopulator() throws IOException {
        IndexPopulator populator = provider.getPopulator(
                index,
                SAMPLING_CONFIG,
                ByteBufferFactory.heapBufferFactory((int) ByteUnit.kibiBytes(100)),
                EmptyMemoryTracker.INSTANCE,
                tokenHolders.lookupWithIds(),
                ElementIdMapper.PLACEHOLDER,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        populator.create();
        return populator;
    }

    private void completePopulation(IndexPopulator populator) throws IndexEntryConflictException {
        populator.scanCompleted(PhaseTracker.nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
    }

    private static IndexUpdater getPopulatingUpdater(IndexPopulator populator) {
        return populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT);
    }

    private IndexAccessor getAccessor() throws IOException {
        return provider.getOnlineAccessor(
                index,
                SAMPLING_CONFIG,
                tokenHolders.lookupWithIds(),
                ElementIdMapper.PLACEHOLDER,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
    }

    private static IndexUpdater getUpdater(IndexAccessor accessor) {
        return accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false);
    }

    private static BoundedIterable<Long> getReader(IndexAccessor accessor) {
        return accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT);
    }

    private void assertIndexed(Iterable<Long> expectedIds) throws Exception {
        try (IndexAccessor accessor = getAccessor();
                BoundedIterable<Long> reader = getReader(accessor)) {
            softly.assertThat(reader).containsExactlyInAnyOrderElementsOf(expectedIds);
        }
    }
}
