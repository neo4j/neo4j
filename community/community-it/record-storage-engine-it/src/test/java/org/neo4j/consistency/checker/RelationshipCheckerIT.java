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
package org.neo4j.consistency.checker;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.consistency.checker.ParallelExecution.NOOP_EXCEPTION_HANDLER;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;

@DbmsExtension(configurationCallback = "configure")
class RelationshipCheckerIT {
    private static final String INDEX_NAME = "index";
    private final ParallelExecution execution = new ParallelExecution(10, NOOP_EXCEPTION_HANDLER, 100);

    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private RecordStorageEngine storageEngine;

    @Inject
    private IndexProviderMap providerMap;

    @Inject
    private Config config;

    @Inject
    private PageCache pageCache;

    @Inject
    private TokenHolders tokenHolders;

    private long relationshipId;
    private CheckerContext context;
    private DefaultPageCacheTracer pageCacheTracer;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(db_format, RecordFormatSelector.defaultFormat().name());
    }

    @BeforeEach
    void setUp() throws Exception {
        pageCacheTracer = new DefaultPageCacheTracer();
        var label = label("any");
        var type = withName("type");
        var propertyName = "property";
        try (var tx = database.beginTx()) {
            var start = tx.createNode(label);
            var end = tx.createNode();
            var rel = start.createRelationshipTo(end, type);
            rel.setProperty(propertyName, "value");
            relationshipId = rel.getId();
            tx.commit();
        }
        try (var tx = database.beginTx()) {
            tx.schema()
                    .indexFor(type)
                    .on(propertyName)
                    .withName(INDEX_NAME)
                    .withIndexType(IndexType.FULLTEXT)
                    .create();
            tx.commit();
        }
        try (var transaction = database.beginTx()) {
            transaction.schema().awaitIndexesOnline(10, MINUTES);
        }

        prepareContext();
    }

    @Test
    void tracePageCacheAccessOnRelationshipCheck() throws Exception {
        prepareContext();
        var relationshipChecker = new RelationshipChecker(context, new IntObjectHashMap<>(), new IntObjectHashMap<>());

        long initialPins = pageCacheTracer.pins();
        long initialUnpins = pageCacheTracer.unpins();
        long initialHits = pageCacheTracer.hits();

        relationshipChecker.check(LongRange.range(0, relationshipId + 1), true, false, EmptyMemoryTracker.INSTANCE);

        assertThat(pageCacheTracer.pins() - initialPins).isEqualTo(6);
        assertThat(pageCacheTracer.unpins() - initialUnpins).isEqualTo(6);
        assertThat(pageCacheTracer.hits() - initialHits).isEqualTo(6);
    }

    private void prepareContext() throws Exception {
        var neoStores = storageEngine.testAccessNeoStores();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (var storeCursors = new CachedStoreCursors(neoStores, CursorContext.NULL_CONTEXT)) {
            Iterable<IndexDescriptor> indexDescriptors =
                    () -> SchemaRuleAccess.getSchemaRuleAccess(neoStores.getSchemaStore(), tokenHolders)
                            .indexesGetAll(storeCursors, EmptyMemoryTracker.INSTANCE);
            var indexAccessors = new IndexAccessors(
                    providerMap,
                    (c, m) -> asResourceIterator(indexDescriptors.iterator()),
                    new IndexSamplingConfig(config),
                    tokenHolders,
                    contextFactory,
                    storageEngine.getOpenOptions(),
                    storageEngine.indexingBehaviour(),
                    EmptyMemoryTracker.INSTANCE);
            context = new CheckerContext(
                    neoStores,
                    indexAccessors,
                    execution,
                    mock(ConsistencyReport.Reporter.class, RETURNS_MOCKS),
                    CacheAccess.EMPTY,
                    tokenHolders,
                    mock(RecordLoading.class),
                    mock(CountsState.class),
                    mock(EntityBasedMemoryLimiter.class),
                    ProgressMonitorFactory.NONE.multipleParts("test"),
                    pageCache,
                    INSTANCE,
                    NullLog.getInstance(),
                    false,
                    ConsistencyFlags.ALL,
                    contextFactory);
            context.initialize();
        }
    }
}
