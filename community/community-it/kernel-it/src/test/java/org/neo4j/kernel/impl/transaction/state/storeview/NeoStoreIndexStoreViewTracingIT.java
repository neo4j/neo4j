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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension
class NeoStoreIndexStoreViewTracingIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private LockService lockService;
    @Inject
    private RecordStorageEngine storageEngine;

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
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccess" ) )
        {
            var indexStoreView = new NeoStoreIndexStoreView( lockService, storageEngine::newReader );
            var storeScan = indexStoreView.visitNodes( EMPTY_INT_ARRAY, ALWAYS_TRUE_INT, null,
                    (Visitor<EntityTokenUpdate,Exception>) element -> false, true, cursorTracer, INSTANCE );
            storeScan.run();
        }

        assertThat( pageCacheTracer.pins() ).isEqualTo( 2 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 2 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 2 );
    }
}
