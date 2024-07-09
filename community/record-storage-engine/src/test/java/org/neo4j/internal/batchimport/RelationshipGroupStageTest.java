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
package org.neo4j.internal.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.batchimport.api.Configuration.DEFAULT;
import static org.neo4j.batchimport.api.Configuration.withBatchSize;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.HEAP;
import static org.neo4j.internal.batchimport.staging.ExecutionMonitor.INVISIBLE;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@ExtendWith(RandomExtension.class)
class RelationshipGroupStageTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    private NeoStores stores;
    private RelationshipGroupStore store;

    @BeforeEach
    void openStore() {
        var idGeneratorFactory = new DefaultIdGeneratorFactory(
                directory.getFileSystem(), immediate(), false, PageCacheTracer.NULL, "db", true, true);
        stores = new StoreFactory(
                        DatabaseLayout.ofFlat(directory.homePath()),
                        Config.defaults(),
                        idGeneratorFactory,
                        pageCache,
                        PageCacheTracer.NULL,
                        directory.getFileSystem(),
                        NullLogProvider.getInstance(),
                        NULL_CONTEXT_FACTORY,
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL)
                .openNeoStores(StoreType.RELATIONSHIP_GROUP);
        store = stores.getRelationshipGroupStore();
    }

    @AfterEach
    void closeStore() {
        stores.close();
    }

    @Test
    void shouldWriteGroupsFromCache() {
        // given
        var cache = new NodeRelationshipCache(HEAP, 10, INSTANCE);
        var highNodeId = 100_000;
        cache.setNodeCount(highNodeId);
        var numRelationships = highNodeId * 10;
        var numRelationshipTypes = 3;
        for (var r = 0; r < 2; r++) {
            // Do two rounds with the exact same data (hence the random reset) where:
            // - first round increment the counts
            // - second round add the data to the cache
            random.reset();
            for (var i = 0; i < numRelationships; i++) {
                var nodeId = random.nextLong(highNodeId);
                var typeId = random.nextInt(numRelationshipTypes);
                var direction = random.nextBoolean() ? Direction.OUTGOING : Direction.INCOMING;
                if (r == 0) {
                    cache.incrementCount(nodeId);
                } else {
                    cache.getAndPutRelationship(nodeId, typeId, direction, i, false);
                }
            }
        }

        // when
        var stage = new RelationshipGroupStage(
                "groups",
                withBatchSize(DEFAULT, 100),
                store,
                cache,
                NULL_CONTEXT_FACTORY,
                context -> new CachedStoreCursors(stores, context));
        var config = new Configuration.Overridden(DEFAULT) {
            @Override
            public int maxNumberOfWorkerThreads() {
                return 4;
            }
        };
        superviseDynamicExecution(INVISIBLE, config, stage);

        // then
        var groupHighId = store.getIdGenerator().getHighId();
        var group = store.newRecord();
        try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
            for (var id = store.getNumberOfReservedLowIds(); id < groupHighId; id++) {
                store.getRecordByCursor(id, group, RecordLoad.NORMAL, cursor);
                assertThat(cache.isDense(group.getOwningNode())).isTrue();
                try (var verifier = new GroupDataVerifier(group)) {
                    cache.getFirstRel(group.getOwningNode(), verifier);
                }
            }
        }
    }

    private static class GroupDataVerifier implements NodeRelationshipCache.GroupVisitor, AutoCloseable {
        private final RelationshipGroupRecord group;
        private boolean seenType;

        GroupDataVerifier(RelationshipGroupRecord group) {
            this.group = group;
        }

        @Override
        public long visit(long nodeId, int typeId, long out, long in, long loop) {
            if (typeId == group.getType()) {
                assertThat(seenType).isFalse();
                seenType = true;
                assertThat(group.getFirstOut()).isEqualTo(out);
                assertThat(group.getFirstIn()).isEqualTo(in);
                assertThat(group.getFirstLoop()).isEqualTo(loop);
            }
            return 0; // doesn't matter in this context
        }

        @Override
        public void close() {
            assertThat(seenType).isTrue();
        }
    }
}
