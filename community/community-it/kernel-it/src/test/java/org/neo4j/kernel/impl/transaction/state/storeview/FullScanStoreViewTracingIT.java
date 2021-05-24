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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension
class FullScanStoreViewTracingIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private LockService lockService;
    @Inject
    private RecordStorageEngine storageEngine;
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    void tracePageCacheAccess() throws Exception
    {
        int nodeCount = 1000;
        var label = Label.label( "marker" );
        try ( var tx = database.beginTx() )
        {
            for ( int i = 0; i < nodeCount; i++ )
            {
                var node = tx.createNode( label );
                node.setProperty( "a", randomAscii( 10 ) );
            }
            tx.commit();
        }

        var pageCacheTracer = new DefaultPageCacheTracer();
        var indexStoreView =
                new FullScanStoreView( lockService, storageEngine::newReader, storageEngine::createStorageCursors, Config.defaults(), jobScheduler );
        var storeScan = indexStoreView.visitNodes( EMPTY_INT_ARRAY, ALWAYS_TRUE_INT, null,
                new TestTokenScanConsumer(), true, true, pageCacheTracer, INSTANCE );
        storeScan.run( StoreScan.NO_EXTERNAL_UPDATES );

        assertThat( pageCacheTracer.pins() ).isEqualTo( 4 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 4 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 4 );
    }
}
