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
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.transaction.log.LogTailMetadata.EMPTY_LOG_TAIL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@EphemeralPageCacheExtension
class NodeInputIdPropertyLookupTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    private NeoStores store;

    @BeforeEach
    void start() {
        var layout = Neo4jLayout.of(directory.homePath()).databaseLayout(DEFAULT_DATABASE_NAME);
        var fs = directory.getFileSystem();
        store = new StoreFactory(
                        layout,
                        Config.defaults(),
                        new DefaultIdGeneratorFactory(
                                fs, immediate(), false, NULL, layout.getDatabaseName(), true, true),
                        pageCache,
                        NULL,
                        fs,
                        NullLogProvider.getInstance(),
                        NULL_CONTEXT_FACTORY,
                        false,
                        EMPTY_LOG_TAIL)
                .openNeoStores(PROPERTY_ARRAY, PROPERTY_STRING, StoreType.PROPERTY);
    }

    @AfterEach
    void stop() {
        store.close();
    }

    @Test
    void shouldHandleLookupFromMultipleThreads() {
        // given
        var propertyStore = store.getPropertyStore();
        var lookup = new NodeInputIdPropertyLookup(propertyStore, () -> new CachedStoreCursors(store, NULL_CONTEXT));
        var numNodes = 1_000;
        LongFunction<String> valueFunction = String::valueOf;
        DynamicAllocatorProvider allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(store);
        createValues(propertyStore, numNodes, valueFunction, allocatorProvider);

        // when
        var race = new Race().withEndCondition(() -> false);
        race.addContestants(
                4,
                () -> {
                    try (var threadLookup = lookup.newLookup()) {
                        var rng = ThreadLocalRandom.current();
                        for (var i = 0; i < 1_000; i++) {
                            var nodeId = rng.nextLong(numNodes);
                            var value = threadLookup.lookupProperty(nodeId, EmptyMemoryTracker.INSTANCE);
                            // then
                            assertThat(value).isEqualTo(valueFunction.apply(nodeId));
                        }
                    }
                },
                1);
        race.goUnchecked();
    }

    private void createValues(
            PropertyStore propertyStore,
            int numNodes,
            LongFunction<String> valueFunction,
            DynamicAllocatorProvider allocatorProvider) {
        try (var cursor = propertyStore.openPageCursorForWriting(0, NULL_CONTEXT);
                var cursors = new CachedStoreCursors(store, NULL_CONTEXT)) {
            IdGenerator idGenerator = propertyStore.getIdGenerator();
            for (var nodeId = 0; nodeId < numNodes; nodeId++) {
                var block = new PropertyBlock();
                var record = propertyStore.newRecord();
                PropertyStore.encodeValue(
                        block,
                        0,
                        Values.of(valueFunction.apply(nodeId)),
                        allocatorProvider.allocator(PROPERTY_STRING),
                        allocatorProvider.allocator(PROPERTY_ARRAY),
                        NULL_CONTEXT,
                        INSTANCE);
                record.addPropertyBlock(block);
                record.setId(idGenerator.nextId(NULL_CONTEXT));
                record.setInUse(true);
                propertyStore.updateRecord(record, IdUpdateListener.DIRECT, cursor, NULL_CONTEXT, cursors);
            }
        }
    }
}
