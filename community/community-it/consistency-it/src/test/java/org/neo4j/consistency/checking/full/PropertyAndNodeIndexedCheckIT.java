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

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
class PropertyAndNodeIndexedCheckIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    @Inject
    private IndexProviderMap providerMap;
    @Inject
    private Config config;

    private PropertyReader propertyReader;
    private IndexAccessors indexAccessors;
    private NeoStores neoStores;
    private DefaultPageCacheTracer pageCacheTracer;
    private DirectRecordAccess recordAccess;

    @BeforeEach
    void setUp() throws IOException
    {
        prepareTestData();
        pageCacheTracer = new DefaultPageCacheTracer();
        neoStores = storageEngine.testAccessNeoStores();
        var storeAccess = new StoreAccess( neoStores );
        storeAccess.initialize();
        propertyReader = new PropertyReader( storeAccess );
        recordAccess = new DirectRecordAccess( storeAccess, CacheAccess.EMPTY );
        indexAccessors = new IndexAccessors( providerMap, neoStores, new IndexSamplingConfig( config ), pageCacheTracer );
    }

    @Test
    void tracePageCacheOnNodePropertyCheck() throws Exception
    {
        var indexedCheck = new PropertyAndNodeIndexedCheck( indexAccessors, propertyReader, CacheAccess.EMPTY );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheOnNodePropertyCheck" ) )
        {
            neoStores.getNodeStore().scanAllRecords( (Visitor<NodeRecord,Exception>) record ->
            {
                indexedCheck.check( record, mock( CheckerEngine.class ), recordAccess, cursorTracer );
                return false;
            }, NULL );

            assertThat( cursorTracer.pins() ).isEqualTo( 4 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 4 );
            assertThat( cursorTracer.hits() ).isEqualTo( 4 );
        }
    }

    private void prepareTestData()
    {
        try ( var tx = database.beginTx() )
        {
            var node = tx.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "a" + i, "i" );
                node.addLabel( Label.label( "label" + i ) );
            }
            tx.commit();
        }

        try ( var tx = database.beginTx() )
        {
            var schema = tx.schema();
            for ( int i = 0; i < 10; i++ )
            {
                schema.indexFor( Label.label( "label" + i ) ).on( "a" + i ).create();
            }
            tx.commit();
        }

        try ( var tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, MINUTES );
        }
    }
}
