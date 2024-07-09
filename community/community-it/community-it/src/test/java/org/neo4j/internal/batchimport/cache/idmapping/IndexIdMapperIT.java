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
package org.neo4j.internal.batchimport.cache.idmapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.batchimport.api.input.Collector.STRICT;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.storageengine.api.StorageEngineFactory.defaultStorageEngine;
import static org.neo4j.token.ReadOnlyTokenCreator.READ_ONLY;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.PropertyValueLookup;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.PopulationWorkJobScheduler;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Values;

@PageCacheExtension
class IndexIdMapperIT {
    // Id < this threshold is mapped 1-to-1 to the input ID equivalent.
    // Id >= this threshold is mapped to an input ID equivalent of an id - threshold
    // This is to have a deterministic PropertyValueLookup and be able to inject duplicates
    private static final int ID_THRESHOLD = 1_000;
    private static final LongToObjectFunction<Object> ID_FUNCTION =
            id -> id < ID_THRESHOLD ? String.valueOf(id) : String.valueOf(id - ID_THRESHOLD);
    private static final PropertyValueLookup ID_LOOKUP = () -> new PropertyValueLookup.Lookup() {
        @Override
        public Object lookupProperty(long nodeId) {
            return ID_FUNCTION.valueOf(nodeId);
        }

        @Override
        public void close() {}
    };

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    private final Groups groups = new Groups();
    private final Group globalGroup = groups.getOrCreate(null);
    private final Map<String, IndexAccessor> accessors = new HashMap<>();
    private final Map<String, IndexDescriptor> descriptors = new HashMap<>();
    private final LifeSupport life = new LifeSupport();
    private final ImmutableSet<OpenOption> openOptions = Sets.immutable.of(PageCacheOpenOptions.BIG_ENDIAN);
    private final StorageEngineIndexingBehaviour indexingBehaviour = StorageEngineIndexingBehaviour.EMPTY;
    private JobScheduler jobScheduler;
    private IndexIdMapper idMapper;
    private TokenHolders tokenHolders;
    private IndexProviderMap indexProviders;
    private IndexProviderMap tempIndexProviders;
    private PopulationWorkJobScheduler workScheduler;
    private IndexStatisticsStore indexStatisticsStore;

    @BeforeEach
    void init() throws IOException {
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        var indexProvidersAccess = life.add(new DefaultIndexProvidersAccess(
                defaultStorageEngine(),
                fs,
                Config.defaults(),
                jobScheduler,
                NullLogService.getInstance(),
                NULL,
                NULL_CONTEXT_FACTORY));
        var layout = Neo4jLayout.of(directory.homePath()).databaseLayout("db");
        var tempLayout = Neo4jLayout.of(directory.homePath()).databaseLayout("temp");
        tokenHolders = new TokenHolders(
                tokenHolder(TokenHolder.TYPE_PROPERTY_KEY),
                tokenHolder(TokenHolder.TYPE_LABEL),
                tokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE));
        indexProviders = indexProvidersAccess.access(pageCache, layout, writable(), tokenHolders);
        tempIndexProviders = indexProvidersAccess.access(pageCache, tempLayout, writable(), tokenHolders);
        workScheduler = new PopulationWorkJobScheduler(jobScheduler, layout);
        fs.mkdirs(layout.databaseDirectory());
        indexStatisticsStore = life.add(new IndexStatisticsStore(
                pageCache,
                fs,
                RecordDatabaseLayout.convert(layout),
                immediate(),
                false,
                NULL_CONTEXT_FACTORY,
                NULL,
                openOptions));
        life.start();
    }

    @AfterEach
    void close() {
        IOUtils.closeAllUnchecked(idMapper, life::shutdown, jobScheduler);
    }

    private void start() throws IOException {
        idMapper = new IndexIdMapper(
                accessors,
                tempIndexProviders,
                tokenHolders,
                descriptors,
                workScheduler,
                openOptions,
                Configuration.DEFAULT,
                NULL,
                indexStatisticsStore,
                groups,
                indexingBehaviour);
    }

    @Test
    void shouldGetAndPutOnSingleIndex() throws Exception {
        // given
        buildInitialIndex(globalGroup, 1L, 0, 0, sequentialNodes(0, 100));
        start();

        // when
        put(1234567, globalGroup);
        prepare(STRICT);

        // then
        assertId(0, globalGroup);
        assertId(12, globalGroup);
        assertId(1234567, globalGroup);
    }

    @Test
    void shouldGetAndPutOnMultipleIndexes() throws Exception {
        // given
        var group1 = groups.getOrCreate("one");
        var group2 = groups.getOrCreate("two");
        buildInitialIndex(group1, 1L, 0, 0, sequentialNodes(0, 100));
        buildInitialIndex(group2, 2L, 1, 1, sequentialNodes(100, 100));
        start();

        // when
        put(1234567, group1);
        put(7654321, group2);
        prepare(STRICT);

        // then
        assertId(12, group1);
        assertId(112, group2);
        assertId(1234567, group1);
        assertId(7654321, group2);
    }

    @Test
    void shouldFindDuplicateNodesInMultipleIndexes() throws Exception {
        // given
        var group1 = groups.getOrCreate("one");
        var group2 = groups.getOrCreate("two");
        buildInitialIndex(group1, 1L, 0, 0, sequentialNodes(0, 100));
        buildInitialIndex(group2, 2L, 1, 1, sequentialNodes(100, 100));
        start();

        // when
        var duplicateNode1 = ID_THRESHOLD + 9;
        var duplicateNode2 = ID_THRESHOLD + 123;
        put(duplicateNode1, group1);
        put(duplicateNode2, group2);
        var collector = mock(Collector.class);
        prepare(collector);

        // then
        assertId(9, group1);
        assertId(123, group2);
        verify(collector).collectDuplicateNode(ID_FUNCTION.valueOf(duplicateNode1), duplicateNode1, group1);
        verify(collector).collectDuplicateNode(ID_FUNCTION.valueOf(duplicateNode2), duplicateNode2, group2);
        assertThat(asLongSet(idMapper.leftOverDuplicateNodesIds()))
                .isEqualTo(LongSets.immutable.of(duplicateNode1, duplicateNode2));
    }

    @Test
    void shouldFindNodesThatAreDuplicatesInTheIncrement() throws IOException, IndexEntryConflictException {
        // given
        var group = groups.getOrCreate("group");
        buildInitialIndex(group, 1L, 0, 1, sequentialNodes(0, 100));
        start();

        // when
        idMapper.put("110", 101, group);
        idMapper.put("110", 102, group);
        var collector = mock(Collector.class);
        prepare(collector);

        // then
        verify(collector).collectDuplicateNode("110", 102, group);
    }

    @Test
    void shouldStoreIncrementalIndexStatistics() throws Exception {
        // given
        var indexId = 1L;
        buildInitialIndex(globalGroup, indexId, 0, 0, sequentialNodes(0, 100));
        start();

        // when
        var count = 10;
        for (int i = 0; i < count; i++) {
            put(1234567 + i, globalGroup);
        }
        prepare(STRICT);

        // then
        var indexSample = indexStatisticsStore.indexSample(indexId);
        assertThat(indexSample.indexSize()).isEqualTo(count);
        assertThat(indexSample.sampleSize()).isEqualTo(count);
        assertThat(indexSample.uniqueValues()).isEqualTo(count);
    }

    private LongSet asLongSet(LongIterator ids) {
        var set = LongSets.mutable.empty();
        while (ids.hasNext()) {
            set.add(ids.next());
        }
        return set;
    }

    private void prepare(Collector collector) {
        idMapper.completeBuild(collector, Runnable::run);
        idMapper.validate(collector);
        idMapper.prepare(ID_LOOKUP, collector, ProgressMonitorFactory.NONE);
    }

    private void put(long nodeId, Group group) {
        idMapper.put(ID_FUNCTION.valueOf(nodeId), nodeId, group);
    }

    private void assertId(long nodeId, Group group) {
        try (var getter = idMapper.newGetter()) {
            assertThat(getter.get(ID_FUNCTION.valueOf(nodeId), group)).isEqualTo(nodeId);
        }
    }

    private Map<Object, Long> sequentialNodes(long startId, int count) {
        var data = new HashMap<Object, Long>();
        for (var i = 0; i < count; i++) {
            long entityId = startId + i;
            data.put(ID_FUNCTION.valueOf(entityId), entityId);
        }
        return data;
    }

    private void buildInitialIndex(Group group, long indexId, int labelId, int propertyKeyId, Map<Object, Long> data)
            throws IOException, IndexEntryConflictException {
        var indexProvider = indexProviders.getDefaultProvider();
        var descriptor = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(labelId, propertyKeyId))
                .withName(group.descriptiveName())
                .withIndexProvider(indexProvider.getProviderDescriptor())
                .materialise(indexId);
        var indexSamplingConfig = new IndexSamplingConfig(Config.defaults());
        var accessor = indexProvider.getOnlineAccessor(
                descriptor, indexSamplingConfig, tokenHolders, openOptions, indexingBehaviour);
        try (var updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            for (var dataEntry : data.entrySet()) {
                updater.process(IndexEntryUpdate.add(dataEntry.getValue(), descriptor, Values.of(dataEntry.getKey())));
            }
        }
        accessor.force(FileFlushEvent.NULL, NULL_CONTEXT);
        accessors.put(group.name(), accessor);
        descriptors.put(group.name(), descriptor);
    }

    private TokenHolder tokenHolder(String typePropertyKey) {
        var tokenHolder = new CreatingTokenHolder(READ_ONLY, typePropertyKey);
        var initialTokens = new ArrayList<NamedToken>();
        for (var i = 0; i < 10; i++) {
            initialTokens.add(new NamedToken("Token" + i, i));
        }
        tokenHolder.setInitialTokens(initialTokens);
        return tokenHolder;
    }
}
