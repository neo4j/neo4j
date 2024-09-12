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
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.INDEX_TYPES;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TOKEN_DESCRIPTOR;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forAnyEntityTokens;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.token.ReadOnlyTokenCreator.READ_ONLY;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.primitive.ObjectFloatMaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.IndexImporterFactory.CreationContext;
import org.neo4j.batchimport.api.IndexesCreator.CreationListener;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.index.BulkIndexCreationContext;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
@Neo4jLayoutExtension
@PageCacheExtension
class BulkIndexesCreatorTest {

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

    private BulkIndexesCreator indexesCreator;

    @BeforeEach
    void setUp() throws Exception {
        scheduler.init();

        when(storageEngine.getOpenOptions()).thenReturn(immutable.empty());
        when(storageEngine.createStorageCursors(any())).thenReturn(storeCursors);
        when(storageEngine.newReader()).thenReturn(storageReader);
        when(storageEngine.indexingBehaviour()).thenReturn(StorageEngineIndexingBehaviour.EMPTY);

        when(storageReader.allocateNodeCursor(any(), any(), any())).thenReturn(nodeCursor);
        when(storageReader.allocateRelationshipScanCursor(any(), any(), any())).thenReturn(relCursor);

        final var config = Config.defaults();
        indexesCreator = new BulkIndexesCreator(new BulkIndexCreationContext(
                config,
                storageEngine,
                databaseLayout,
                fs,
                pageCache,
                new MetadataCache(KernelVersion.getLatestVersion(config)),
                scheduler,
                new TokenHolders(
                        tokenHolder(TokenHolder.TYPE_LABEL),
                        tokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE),
                        tokenHolder(TokenHolder.TYPE_PROPERTY_KEY)),
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                NullLogService.getInstance(),
                INSTANCE));
    }

    @AfterEach
    void shutdown() throws Exception {
        scheduler.shutdown();
    }

    @Test
    void factoryRequiresCorrectContext() {
        final var factory = new IndexImporterFactoryImpl();
        assertThatThrownBy(() -> factory.getCreator(new DuffContext(fs)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Index creation requires an instance of BulkIndexCreationContext");
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
    void createWithError() {
        // WHEN
        final var deltas = ObjectFloatMaps.mutable.<IndexDescriptor>empty();
        final var completed = new MutableBoolean();
        final var checkPointed = new MutableBoolean();

        // this isn't valid config (RANGE + LOOKUP) => boom
        final var descriptor = forSchema(forLabel(1, 2))
                .withName("duff")
                .withIndexType(IndexType.RANGE)
                .withIndexProvider(TOKEN_DESCRIPTOR)
                .materialise(0);
        assertThatThrownBy(() -> indexesCreator.create(
                        new CreationListener() {
                            @Override
                            public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                                deltas.updateValue(indexDescriptor, 0.0f, f -> f + percentDelta);
                            }

                            @Override
                            public void onCreationCompleted() {
                                completed.setTrue();
                            }

                            @Override
                            public void onCheckpointingCompleted() {
                                checkPointed.setTrue();
                            }
                        },
                        List.of(descriptor)))
                .isInstanceOf(IOException.class)
                .hasMessageContainingAll(
                        "failed to complete",
                        "Failed to populate index",
                        "type='RANGE'",
                        "indexProvider='token-lookup-1.0'");

        assertThat(deltas.get(descriptor))
                .as("should not have completed the progress")
                .isEqualTo(0.0f);

        assertThat(fs.fileExists(directoriesByProvider(databaseLayout.databaseDirectory())
                        .forProvider(descriptor.getIndexProvider())
                        .directoryForIndex(descriptor.getId())))
                .as("should still create the directory structure of the index")
                .isTrue();

        assertThat(completed.booleanValue())
                .as("should NOT complete the creation steps")
                .isFalse();
        assertThat(checkPointed.booleanValue())
                .as("should NOT checkpoint on failure")
                .isFalse();
    }

    private void assertCreation(IndexDescriptor... descriptors) throws Exception {
        // WHEN
        final var deltas = ObjectFloatMaps.mutable.<IndexDescriptor>empty();
        final var completed = new MutableBoolean();
        final var checkPointed = new MutableBoolean();
        indexesCreator.create(
                new CreationListener() {
                    @Override
                    public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                        deltas.updateValue(indexDescriptor, 0.0f, f -> f + percentDelta);
                    }

                    @Override
                    public void onCreationCompleted() {
                        completed.setTrue();
                    }

                    @Override
                    public void onCheckpointingCompleted() {
                        checkPointed.setTrue();
                    }
                },
                List.of(descriptors));

        // THEN
        for (var descriptor : descriptors) {
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

    private static TokenHolder tokenHolder(String typePropertyKey) {
        var tokenHolder = new CreatingTokenHolder(READ_ONLY, typePropertyKey);
        tokenHolder.setInitialTokens(IntStream.range(0, TOKEN_COUNT)
                .mapToObj(i -> new NamedToken(typePropertyKey + i, i))
                .toList());
        return tokenHolder;
    }

    private static Stream<Arguments> indexes() {
        final var counter = new MutableInt();
        return Stream.of(EntityType.NODE, EntityType.RELATIONSHIP)
                .flatMap(entityType -> INDEX_TYPES.entrySet().stream().map(entry -> {
                    final var providerDescriptor = entry.getKey();
                    final var indexType = entry.getValue();
                    final var ruleId = counter.getAndIncrement();
                    IndexPrototype indexPrototype;
                    if (providerDescriptor == TOKEN_DESCRIPTOR) {
                        indexPrototype = forSchema(forAnyEntityTokens(entityType));
                    } else {
                        if (entityType == EntityType.NODE) {
                            indexPrototype = forSchema(forLabel(ruleId + 1, ruleId + 2));
                        } else {
                            indexPrototype = forSchema(forRelType(ruleId + 1, ruleId + 2));
                        }

                        if (providerDescriptor == AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR) {
                            indexPrototype = indexPrototype.withIndexConfig(IndexConfig.with(Map.of(
                                    IndexSetting.vector_Dimensions().getSettingName(),
                                    Values.intValue(666),
                                    IndexSetting.vector_Similarity_Function().getSettingName(),
                                    Values.stringValue("COSINE"))));
                        }
                    }

                    return Arguments.of(indexPrototype
                            .withName("index_" + ruleId)
                            .withIndexProvider(providerDescriptor)
                            .withIndexType(indexType)
                            .materialise(ruleId));
                }));
    }

    private record DuffContext(FileSystemAbstraction fs) implements CreationContext {}
}
