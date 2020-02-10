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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
class RelationshipIndexedCheckIT
{
    private static final String INDEX_NAME = "relIndex";
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    @Inject
    private IndexProviderMap providerMap;
    @Inject
    private IndexingService indexingService;
    @Inject
    private Config config;

    private PropertyReader propertyReader;
    private IndexAccessors indexAccessors;
    private NeoStores neoStores;
    private DefaultPageCacheTracer pageCacheTracer;
    private DirectRecordAccess recordAccess;

    @BeforeEach
    void setUp() throws Exception
    {
        prepareTestData();
        pageCacheTracer = new DefaultPageCacheTracer();
        neoStores = storageEngine.testAccessNeoStores();
        var storeAccess = new StoreAccess( neoStores );
        storeAccess.initialize();
        propertyReader = new PropertyReader( storeAccess );
        recordAccess = new DirectRecordAccess( storeAccess, CacheAccess.EMPTY );
        allowIndexToBeAccessedByIndexAccessors();
        indexAccessors = new IndexAccessors( providerMap, neoStores, new IndexSamplingConfig( config ), PageCacheTracer.NULL );
    }

    @AfterEach
    void tearDown()
    {
        indexAccessors.close();
    }

    @Test
    void tracePageCacheOnRelationshipPropertyCheck() throws Exception
    {
        var indexList = List.of( getIndexDescriptor( INDEX_NAME ) );
        var indexedCheck = new RelationshipToIndexCheck( indexList, indexAccessors, propertyReader );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheOnRelationshipPropertyCheck" ) )
        {
            neoStores.getRelationshipStore().scanAllRecords( (Visitor<RelationshipRecord,Exception>) record ->
            {
                indexedCheck.check( record, mock( CheckerEngine.class ), recordAccess, cursorTracer );
                return false;
            }, NULL );

            assertThat( cursorTracer.pins() ).isEqualTo( 1 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
            assertThat( cursorTracer.hits() ).isEqualTo( 1 );
        }
    }

    private void allowIndexToBeAccessedByIndexAccessors() throws Exception
    {
        indexingService.stop();
        indexingService.shutdown();
    }

    private void prepareTestData()
    {
        var relType = RelationshipType.withName( "connection" );
        var propertyName = "property";
        try ( var tx = database.beginTx() )
        {
            var start = tx.createNode();
            var end = tx.createNode();
            var relationship = start.createRelationshipTo( end, relType );
            relationship.setProperty( propertyName, "value" );
            tx.commit();
        }

        try ( var tx = database.beginTx() )
        {
            tx.schema().indexFor( relType ).on( propertyName ).withIndexType( IndexType.FULLTEXT ).withName( INDEX_NAME ).create();
            tx.commit();
        }

        try ( var tx = database.beginTx() )
        {
            tx.schema().awaitIndexOnline( INDEX_NAME, 10, MINUTES );
        }
    }

    private IndexDescriptor getIndexDescriptor( String indexName )
    {
        try ( var transaction = database.beginTx() )
        {
            var schemaRead = ((InternalTransaction) transaction).kernelTransaction().schemaRead();
            return schemaRead.indexGetForName( indexName );
        }
    }
}
