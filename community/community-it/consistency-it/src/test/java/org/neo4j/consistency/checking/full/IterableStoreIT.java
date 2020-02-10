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
package org.neo4j.consistency.checking.full;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DbmsExtension
class IterableStoreIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    private DefaultPageCacheTracer pageCacheTracer;

    @BeforeEach
    void setUp()
    {
        pageCacheTracer = new DefaultPageCacheTracer();

        prepareTestData();
    }

    @Test
    void tracePageCacheAccessOnWarmup()
    {
        try ( var iterableStore = new IterableStore<>( storageEngine.testAccessNeoStores().getNodeStore(), true, pageCacheTracer ) )
        {
            iterableStore.warmUpCache();
        }

        assertThat( pageCacheTracer.pins() ).isEqualTo( 10 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 10 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 10 );
    }

    @Test
    void tracePageCacheAccessOnIterations()
    {
        try ( var iterableStore = new IterableStore<>( storageEngine.testAccessNeoStores().getNodeStore(), true, pageCacheTracer ) )
        {
            assertEquals( 10_000, Iterators.count( iterableStore.iterator() ) );
        }

        assertThat( pageCacheTracer.pins() ).isEqualTo( 19 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 19 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 19 );
    }

    private void prepareTestData()
    {
        for ( int i = 0; i < 100; i++ )
        {
            try ( var tx = database.beginTx() )
            {
                for ( int j = 0; j < 100; j++ )
                {
                    tx.createNode();
                }
                tx.commit();
            }
        }
    }
}
