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
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension
class LabelScanNodeViewTracingIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    @Inject
    private LockService lockService;
    @Inject
    private LabelScanStore labelScanStore;

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

        var labelId = getLabelId( label );

        var cacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = cacheTracer.createPageCursorTracer( "tracePageCacheAccess" ) )
        {
            var scan = new LabelViewNodeStoreScan<>( storageEngine.newReader(), lockService, labelScanStore,
                    (Visitor<EntityTokenUpdate,Exception>) element -> false, null, new int[]{labelId}, any -> false, cursorTracer, INSTANCE );
            scan.run();
        }

        assertThat( cacheTracer.pins() ).isEqualTo( 3 );
        assertThat( cacheTracer.unpins() ).isEqualTo( 3 );
        assertThat( cacheTracer.hits() ).isEqualTo( 3 );
    }

    private int getLabelId( Label label )
    {
        try ( var tx = database.beginTx() )
        {
            return ((InternalTransaction)tx).kernelTransaction().tokenRead().nodeLabel( label.name() );
        }
    }
}
