/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checking.cache;

import org.junit.jupiter.api.Test;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.full.Stage;
import org.neo4j.consistency.checking.full.StoreProcessor;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@DbmsExtension
class CacheTaskTrackingIT
{
    private static final long NODE_COUNT = 1000;

    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;

    @Test
    void trackPageCacheAccessOnProcessCache()
    {

        try ( var tx = database.beginTx() )
        {
            for ( int i = 0; i < NODE_COUNT; i++ )
            {
                tx.createNode();
            }
            tx.commit();
        }

        var pageCacheTracer = new DefaultPageCacheTracer();
        var storeAccess = new StoreAccess( storageEngine.testAccessNeoStores() );
        storeAccess.initialize();
        var cacheAccess = CacheAccess.EMPTY;
        final Stage stage = Stage.SEQUENTIAL_FORWARD;

        assertZeroTracer( pageCacheTracer );

        var storeProcessor = new StoreProcessor( CheckDecorator.NONE, ConsistencyReport.NO_REPORT, stage, cacheAccess );
        var task = new CacheTask.CheckNextRel( stage, cacheAccess, storeAccess, storeProcessor, pageCacheTracer );
        task.run();

        assertThat( pageCacheTracer.pins() ).isEqualTo( NODE_COUNT );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( NODE_COUNT );
        assertThat( pageCacheTracer.hits() ).isEqualTo( NODE_COUNT );
    }

    private void assertZeroTracer( DefaultPageCacheTracer pageCacheTracer )
    {
        assertThat( pageCacheTracer.pins() ).isZero();
        assertThat( pageCacheTracer.unpins() ).isZero();
        assertThat( pageCacheTracer.hits() ).isZero();
    }
}
