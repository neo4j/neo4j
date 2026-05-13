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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.INDEX_TYPES;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.RANGE_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TOKEN_DESCRIPTOR;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forAnyEntityTokens;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.token.ReadOnlyTokenCreator.READ_ONLY;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.primitive.ObjectFloatMaps;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.primitive.MutableObjectFloatMap;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.factory.Multimaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.IndexImporterFactory.LifecycleContext;
import org.neo4j.batchimport.api.IndexesLifecycleManager.CreationListener;
import org.neo4j.batchimport.api.IndexesLifecycleManager.DropListener;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.index.KernelSchemaLifecycleContext;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailure;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogMetadataProviderImpl;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.ElementIdMapper;

@RandomSupportExtension
@Neo4jLayoutExtension
@PageCacheExtension
class KernelIndexesLifecycleManagerTest {

    // for relationships and nodes and for all the different index types
    private static final int TOKEN_COUNT = INDEX_TYPES.size() * 2 * 2;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private PageCache pageCache;

    private final JobScheduler scheduler = JobSchedulerFactory.createScheduler();

    private final ReadableStorageEngine storageEngine = mock(ReadableStorageEngine.class);

    private final StoreCursors storeCursors = mock(StoreCursors.class);

    private final StorageReader storageReader = mock(StorageReader.class);

    private final StorageNodeCursor nodeCursor = mock(StorageNodeCursor.class);

    private final StorageRelationshipScanCursor relCursor = mock(StorageRelationshipScanCursor.class);

    private KernelIndexesLifecycleManager indexesLifecycleManager;

    @BeforeEach
    void setUp() throws Exception {
        scheduler.init();

        when(storageEngine.getOpenOptions()).thenReturn(immutable.empty());
        when(storageEngine.createStorageCursors(any())).thenReturn(storeCursors);
        when(storageEngine.newReader()).thenReturn(storageReader);
        when(storageEngine.indexingBehaviour()).thenReturn(StorageEngineIndexingBehaviour.EMPTY);

        when(storageReader.indexesGetAll()).thenReturn(Collections.emptyIterator());
        when(storageReader.allocateNodeCursor(any(), any(), any())).thenReturn(nodeCursor);
        when(storageReader.allocateRelationshipScanCursor(any(), any(), any())).thenReturn(relCursor);

        Config config = Config.defaults();
        KernelVersion kernelVersion = KernelVersion.getLatestVersion(config);
        indexesLifecycleManager = new KernelIndexesLifecycleManager(new KernelSchemaLifecycleContext(
                config,
                storageEngine,
                databaseLayout,
                fs,
                pageCache,
                new LogMetadataProviderImpl(
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        LogFormat.fromConfigAndKernelVersion(config, kernelVersion),
                        kernelVersion),
                scheduler,
                new TokenHolders(
                        tokenHolder(TokenHolder.TYPE_LABEL),
                        tokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE),
                        tokenHolder(TokenHolder.TYPE_PROPERTY_KEY)),
                ElementIdMapper.PLACEHOLDER,
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                NullLogService.getInstance(),
                Collector.EMPTY,
                INSTANCE));
    }

    @AfterEach
    void shutdown() throws Exception {
        indexesLifecycleManager.close();
        scheduler.shutdown();
    }

    @Test
    void factoryRequiresCorrectContext() {
        IndexImporterFactoryImpl factory = new IndexImporterFactoryImpl();
        assertThatThrownBy(() -> factory.getLifecycleManager(new DuffContext(fs)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Index creation requires an instance of BulkIndexCreationContext");
    }

    private static Stream<Arguments> completeConfiguration() {
        return INDEX_TYPES.entrySet().stream().map(entry -> {
            IndexProviderDescriptor indexProviderDescriptor = entry.getKey();
            IndexType indexType = entry.getValue();
            boolean addsDefaultConfig = indexType == IndexType.POINT || indexType == IndexType.FULLTEXT;
            return Arguments.of(indexProviderDescriptor, indexType, addsDefaultConfig);
        });
    }

    @ParameterizedTest
    @MethodSource
    void completeConfiguration(IndexProviderDescriptor providerDescriptor, IndexType type, boolean addsDefaultConfig) {
        IndexPrototype prototype =
                forSchema(forLabel(1, 2)).withName("basic").withIndexType(type).withIndexProvider(providerDescriptor);

        IndexDescriptor descriptor = withDefaultConfig(prototype).materialise(3);
        assertThat(descriptor.getCapability()).isEqualTo(IndexCapability.NO_CAPABILITY);

        IndexDescriptor completedDescriptor = indexesLifecycleManager.completeConfiguration(descriptor);
        assertThat(completedDescriptor.getCapability())
                .as("completion should set the correct capability")
                .isNotEqualTo(IndexCapability.NO_CAPABILITY);
        if (addsDefaultConfig) {
            assertThat(descriptor.getIndexConfig().asMap())
                    .as("use the default index config for descriptor")
                    .isEmpty();

            assertThat(completedDescriptor.getIndexConfig().asMap())
                    .as("completion should set the default index config for descriptor")
                    .isNotEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("indexes")
    void createSingle(IndexDescriptor descriptor) throws Exception {
        assertCreation(descriptor);
    }

    @Test
    void createAll() throws Exception {
        assertCreation(indexes().map(args -> (IndexDescriptor) args.get()[0]).toArray(IndexDescriptor[]::new));
    }

    @Test
    void createWithError() throws IOException {
        MutableObjectFloatMap<IndexDescriptor> deltas = ObjectFloatMaps.mutable.empty();
        MutableBoolean completed = new MutableBoolean();
        MutableBoolean checkPointed = new MutableBoolean();

        // this isn't valid config (RANGE + LOOKUP) => boom
        IndexDescriptor descriptor = forSchema(forLabel(1, 2))
                .withName("duff")
                .withIndexType(IndexType.RANGE)
                .withIndexProvider(TOKEN_DESCRIPTOR)
                .materialise(0);
        indexesLifecycleManager.create(
                new CreationListener() {
                    @Override
                    public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                        deltas.updateValue(indexDescriptor, 0.0f, f -> f + percentDelta);
                    }

                    @Override
                    public void onFailure(IndexDescriptor indexDescriptor, KernelException error) {
                        assertThat(indexDescriptor).isEqualTo(descriptor);
                        assertThat(error)
                                .hasMessageContainingAll(
                                        "Failed to populate index", "type='RANGE'", "indexProvider='token-lookup-1.0'");
                    }

                    @Override
                    public void onCreationCompleted(boolean withSuccess) {
                        completed.setValue(withSuccess);
                    }

                    @Override
                    public void onCheckpointingCompleted() {
                        checkPointed.setTrue();
                    }
                },
                List.of(descriptor));

        assertThat(deltas.get(descriptor))
                .as("should not have completed the progress")
                .isEqualTo(0.0f);

        assertThat(fs.fileExists(directoriesByProvider(databaseLayout.databaseDirectory())
                        .forProvider(descriptor.getIndexProvider())
                        .directoryForIndex(descriptor.getId())))
                .as("should still create the directory structure of the index")
                .isTrue();

        assertThat(completed.booleanValue())
                .as("should complete the creation steps with failure flag")
                .isFalse();
        assertThat(checkPointed.booleanValue())
                .as("should NOT checkpoint on failure")
                .isFalse();
    }

    @Test
    void longerRunningIndexingReportsCorrectly()
            throws IOException, IndexPopulationFailedKernelException, IndexNotFoundKernelException {
        IndexingService indexingService = mock(IndexingService.class);
        try (KernelIndexesLifecycleManager lifecycleManager = indexesLifecycleManager(indexingService)) {
            IndexDescriptor descriptor1 = forSchema(forLabel(1, 2))
                    .withName("descriptor1")
                    .withIndexType(IndexType.RANGE)
                    .withIndexProvider(RANGE_DESCRIPTOR)
                    .materialise(1);
            IndexDescriptor descriptor2 = forSchema(forLabel(3, 4))
                    .withName("descriptor2")
                    .withIndexType(IndexType.RANGE)
                    .withIndexProvider(RANGE_DESCRIPTOR)
                    .materialise(2);

            float d1p1 = 0.0f;
            float d2p1 = 0.0f;
            float d1p2 = 0.1f;
            float d2p2 = 0.1f;
            float d2p3 = 0.2f;
            float d2p4 = 0.5f;
            float d2p5 = 0.9f;
            float d2p6 = 1.0f;

            IndexProxy tentativeProxy = populatingProxy(descriptor2, d2p6);

            MutableSet<IndexProxy> state1 =
                    Sets.mutable.of(populatingProxy(descriptor1, d1p1), populatingProxy(descriptor2, d2p1));
            MutableSet<IndexProxy> state2 =
                    Sets.mutable.of(populatingProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p2));
            MutableSet<IndexProxy> state3 =
                    Sets.mutable.of(failedProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p3));
            MutableSet<IndexProxy> state4 =
                    Sets.mutable.of(failedProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p4));
            MutableSet<IndexProxy> state5 =
                    Sets.mutable.of(failedProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p5));
            MutableSet<IndexProxy> state6 = Sets.mutable.of(failedProxy(descriptor1, d1p2), tentativeProxy);
            MutableSet<IndexProxy> state7 = Sets.mutable.of(failedProxy(descriptor1, d1p2), onlineProxy(descriptor2));
            //noinspection unchecked
            when(indexingService.getIndexProxies()).thenReturn(state1, state2, state3, state4, state5, state6, state7);

            MutableBoolean completed = new MutableBoolean();
            MutableList<IndexDescriptor> errors = Lists.mutable.empty();
            MutableListMultimap<IndexDescriptor, Float> progress = Multimaps.mutable.list.empty();
            lifecycleManager.create(
                    new CreationListener() {
                        @Override
                        public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                            progress.put(indexDescriptor, percentDelta);
                        }

                        @Override
                        public void onFailure(IndexDescriptor indexDescriptor, KernelException error) {
                            errors.add(indexDescriptor);
                        }

                        @Override
                        public void onCreationCompleted(boolean withSuccess) {
                            completed.setValue(withSuccess);
                        }

                        @Override
                        public void onCheckpointingCompleted() {
                            fail("should not perform checkpointing as errors expected");
                        }
                    },
                    List.of(descriptor1, descriptor2));

            assertThat(completed.booleanValue())
                    .as("should finish with an error")
                    .isFalse();
            assertThat(errors).as("should complete with the one error").containsExactly(descriptor1);
            assertThat(progress.get(descriptor1)).containsExactly(d1p2);
            assertThat(progress.get(descriptor2)).hasSize(5).satisfies(deltas -> {
                for (Float delta : deltas) {
                    assertThat(delta).isGreaterThan(0.0f);
                }
            });

            verify(indexingService, times(1)).activateIndex(eq(descriptor2));
        }
    }

    @Test
    void drop() throws IOException {
        IndexDescriptor descriptor1 = forSchema(forLabel(1, 2))
                .withName("descriptor1")
                .withIndexType(IndexType.RANGE)
                .withIndexProvider(RANGE_DESCRIPTOR)
                .materialise(1);
        IndexDescriptor descriptor2 = forSchema(forLabel(3, 4))
                .withName("descriptor2")
                .withIndexType(IndexType.RANGE)
                .withIndexProvider(RANGE_DESCRIPTOR)
                .materialise(2);
        IndexDescriptor descriptor3 = forSchema(forLabel(5, 6))
                .withName("descriptor3")
                .withIndexType(IndexType.RANGE)
                .withIndexProvider(RANGE_DESCRIPTOR)
                .materialise(3);

        IndexingService indexingService = mock(IndexingService.class);

        IllegalArgumentException boom = new IllegalArgumentException("boom");
        doThrow(boom).when(indexingService).dropIndex(eq(descriptor3));

        try (KernelIndexesLifecycleManager lifecycleManager = indexesLifecycleManager(indexingService)) {
            lifecycleManager.drop(
                    new DropListener() {
                        @Override
                        public boolean onDrop(IndexDescriptor indexDescriptor) {
                            assertThat(indexDescriptor).isIn(descriptor1, descriptor2);
                            return indexDescriptor == descriptor1;
                        }

                        @Override
                        public void onDropFailed(IndexDescriptor indexDescriptor, RuntimeException ex) {
                            assertThat(indexDescriptor).isEqualTo(descriptor3);
                            assertThat(ex).isEqualTo(boom);
                        }

                        @Override
                        public void onDropCompleted(int dropOk, int dropFailed) {
                            assertThat(dropOk)
                                    .as("should record that descriptor1 was OK")
                                    .isOne();
                            assertThat(dropFailed)
                                    .as("should record that both descriptor2 and descriptor3 failed")
                                    .isEqualTo(2);
                        }
                    },
                    List.of(descriptor1, descriptor2, descriptor3));
        }
    }

    private KernelIndexesLifecycleManager indexesLifecycleManager(IndexingService indexingService) throws IOException {
        Config config = Config.defaults();
        KernelVersion kernelVersion = KernelVersion.getLatestVersion(config);
        KernelSchemaLifecycleContext context = new KernelSchemaLifecycleContext(
                config,
                storageEngine,
                databaseLayout,
                fs,
                pageCache,
                new LogMetadataProviderImpl(
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        LogFormat.fromConfigAndKernelVersion(config, kernelVersion),
                        kernelVersion),
                scheduler,
                new TokenHolders(
                        tokenHolder(TokenHolder.TYPE_LABEL),
                        tokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE),
                        tokenHolder(TokenHolder.TYPE_PROPERTY_KEY)),
                ElementIdMapper.PLACEHOLDER,
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                NullLogService.getInstance(),
                Collector.EMPTY,
                INSTANCE);
        return new KernelIndexesLifecycleManager(context) {
            @Override
            protected IndexingService createIndexingService(LifeSupport life, KernelSchemaLifecycleContext context) {
                return indexingService;
            }
        };
    }

    private void assertCreation(IndexDescriptor... descriptors) throws Exception {
        // WHEN
        MutableObjectFloatMap<IndexDescriptor> deltas = ObjectFloatMaps.mutable.empty();
        MutableBoolean completed = new MutableBoolean();
        MutableBoolean checkPointed = new MutableBoolean();
        indexesLifecycleManager.create(
                new CreationListener() {
                    @Override
                    public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                        deltas.updateValue(indexDescriptor, 0.0f, f -> f + percentDelta);
                    }

                    @Override
                    public void onFailure(IndexDescriptor indexDescriptor, KernelException error) {
                        fail("should not report any error for %s: %s", indexDescriptor, error);
                    }

                    @Override
                    public void onCreationCompleted(boolean withSuccess) {
                        completed.setValue(withSuccess);
                    }

                    @Override
                    public void onCheckpointingCompleted() {
                        checkPointed.setTrue();
                    }
                },
                List.of(descriptors));

        // THEN
        for (IndexDescriptor descriptor : descriptors) {
            assertThat(deltas.get(descriptor))
                    .as("should have completed the progress")
                    .isEqualTo(1.0f);

            assertThat(fs.fileExists(directoriesByProvider(databaseLayout.databaseDirectory())
                            .forProvider(descriptor.getIndexProvider())
                            .directoryForIndex(descriptor.getId())))
                    .as("should create the directory structure of the index")
                    .isTrue();
        }

        assertThat(completed.booleanValue())
                .as("should complete the creation steps")
                .isTrue();
        assertThat(checkPointed.booleanValue())
                .as("should checkpoint the results indexes")
                .isTrue();
    }

    private static IndexProxy populatingProxy(IndexDescriptor descriptor, float percent) {
        PopulationProgress populationProgress = mock(PopulationProgress.class);
        when(populationProgress.getProgress()).thenReturn(percent);

        IndexProxy proxy = mock(IndexProxy.class);
        when(proxy.getState()).thenReturn(InternalIndexState.POPULATING);
        when(proxy.getDescriptor()).thenReturn(descriptor);
        when(proxy.getIndexPopulationProgress()).thenReturn(populationProgress);

        if (percent == 1.0f) {
            try {
                when(proxy.awaitStoreScanCompleted(anyLong(), any())).thenReturn(true);
            } catch (IndexPopulationFailedKernelException | InterruptedException ex) {
                // mocking so ignore
            }
        }

        return proxy;
    }

    @SuppressWarnings("SameParameterValue")
    private static IndexProxy failedProxy(IndexDescriptor descriptor, float percent) {
        PopulationProgress populationProgress = mock(PopulationProgress.class);
        when(populationProgress.getProgress()).thenReturn(percent);

        IndexPopulationFailure failure = mock(IndexPopulationFailure.class);
        when(failure.asIndexPopulationFailure(any(), any()))
                .thenReturn(IndexPopulationFailedKernelException.indexPopulationFailed("boom", new IOException()));

        IndexProxy proxy = mock(IndexProxy.class);
        when(proxy.getState()).thenReturn(InternalIndexState.FAILED);
        when(proxy.getDescriptor()).thenReturn(descriptor);
        when(proxy.getIndexPopulationProgress()).thenReturn(populationProgress);
        when(proxy.getPopulationFailure()).thenReturn(failure);
        return proxy;
    }

    @SuppressWarnings("SameParameterValue")
    private static IndexProxy onlineProxy(IndexDescriptor descriptor) {
        PopulationProgress populationProgress = mock(PopulationProgress.class);
        when(populationProgress.getProgress()).thenReturn(1.0f);

        IndexProxy proxy = mock(IndexProxy.class);
        when(proxy.getState()).thenReturn(InternalIndexState.ONLINE);
        when(proxy.getDescriptor()).thenReturn(descriptor);
        when(proxy.getIndexPopulationProgress()).thenReturn(populationProgress);
        return proxy;
    }

    private static TokenHolder tokenHolder(String typePropertyKey) {
        CreatingTokenHolder tokenHolder = new CreatingTokenHolder(READ_ONLY, typePropertyKey);
        tokenHolder.setInitialTokens(IntStream.range(0, TOKEN_COUNT)
                .mapToObj(i -> new NamedToken(typePropertyKey + i, i))
                .toList());
        return tokenHolder;
    }

    private static Stream<Arguments> indexes() {
        MutableInt counter = new MutableInt();
        return Stream.of(EntityType.NODE, EntityType.RELATIONSHIP)
                .flatMap(entityType -> INDEX_TYPES.entrySet().stream().map(entry -> {
                    IndexProviderDescriptor providerDescriptor = entry.getKey();
                    IndexType indexType = entry.getValue();
                    int ruleId = counter.getAndIncrement();
                    IndexPrototype indexPrototype;
                    if (indexType.isLookup()) {
                        indexPrototype = forSchema(forAnyEntityTokens(entityType));
                    } else {
                        if (entityType == EntityType.NODE) {
                            indexPrototype = forSchema(forLabel(ruleId + 1, ruleId + 2));
                        } else {
                            indexPrototype = forSchema(forRelType(ruleId + 1, ruleId + 2));
                        }
                    }

                    indexPrototype = indexPrototype
                            .withName("index_" + ruleId)
                            .withIndexProvider(providerDescriptor)
                            .withIndexType(indexType);

                    return Arguments.of(withDefaultConfig(indexPrototype).materialise(ruleId));
                }));
    }

    private static IndexPrototype withDefaultConfig(IndexPrototype indexPrototype) {
        if (indexPrototype.getIndexType() == IndexType.VECTOR) {
            VectorIndexVersion version = VectorIndexVersion.fromDescriptor(indexPrototype.getIndexProvider());
            VectorIndexSettings settings = VectorIndexSettings.create();
            if (version == VectorIndexVersion.V1_0) {
                settings.withDimensions(666).withSimilarityFunction("COSINE");
            }
            indexPrototype = indexPrototype.withIndexConfig(settings.toIndexConfigWith(version));
        }
        return indexPrototype;
    }

    private record DuffContext(FileSystemAbstraction fs) implements LifecycleContext {}
}
