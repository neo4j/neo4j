/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class DynamicIndexStoreViewTracingIT {
    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private LockService lockService;

    @Inject
    private Locks locks;

    @Inject
    private IndexingService indexingService;

    @Inject
    private StorageEngine storageEngine;

    @Inject
    private JobScheduler jobScheduler;

    @Test
    void tracePageCacheAccess() {
        int nodeCount = 1000;
        var label = Label.label("marker");
        try (var tx = database.beginTx()) {
            for (int i = 0; i < nodeCount; i++) {
                var node = tx.createNode(label);
                node.setProperty("a", randomAscii(10));
            }
            tx.commit();
        }

        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EmptyVersionContextSupplier.EMPTY);
        var neoStoreStoreView = new FullScanStoreView(
                lockService,
                storageEngine::newReader,
                storageEngine::createStorageCursors,
                Config.defaults(),
                jobScheduler);
        var indexStoreView = new DynamicIndexStoreView(
                neoStoreStoreView,
                locks,
                lockService,
                Config.defaults(),
                indexDescriptor -> indexingService.getIndexProxy(indexDescriptor),
                storageEngine::newReader,
                storageEngine::createStorageCursors,
                NullLogProvider.getInstance());
        var storeScan = indexStoreView.visitNodes(
                new int[] {0, 1, 2},
                ALWAYS_TRUE_INT,
                null,
                new TestTokenScanConsumer(),
                false,
                true,
                contextFactory,
                INSTANCE);
        storeScan.run(StoreScan.NO_EXTERNAL_UPDATES);

        assertThat(pageCacheTracer.pins()).isEqualTo(104);
        assertThat(pageCacheTracer.unpins()).isEqualTo(104);
        assertThat(pageCacheTracer.hits()).isEqualTo(104);
    }
}
