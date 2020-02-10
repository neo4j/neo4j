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

import org.neo4j.common.TokenNameLookup;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;

@DbmsExtension
class IndexCheckIT
{
    private static final String NODE_INDEX_NAME = "nodeIndex";
    private static final String RELATIONSHIP_INDEX_NAME = "relationshipIndex";
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    @Inject
    private TokenNameLookup tokenNameLookup;
    private DirectRecordAccess recordAccess;
    private DefaultPageCacheTracer pageCacheTracer;
    private long nodeId;
    private long relationshipId;

    @BeforeEach
    void setUp()
    {
        var label = label( "any" );
        var propertyName = "property";
        var relationshipType = withName( "type" );
        try ( var tx = database.beginTx() )
        {
            tx.schema().indexFor( label ).on( propertyName ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( var tx = database.beginTx() )
        {
            tx.schema().indexFor( relationshipType ).withIndexType( FULLTEXT ).on( propertyName ).withName( RELATIONSHIP_INDEX_NAME ).create();
            tx.commit();
        }
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 10, MINUTES );
        }

        try ( var tx = database.beginTx() )
        {
            var start = tx.createNode( label );
            start.setProperty( propertyName, "nodeValue" );
            nodeId = start.getId();
            var end = tx.createNode( label );
            var relationship = start.createRelationshipTo( end, relationshipType );
            relationshipId = relationship.getId();
            relationship.setProperty( propertyName, "relationshipValue" );
            tx.commit();
        }

        var neoStores = storageEngine.testAccessNeoStores();
        var storeAccess = new StoreAccess( neoStores );
        storeAccess.initialize();
        recordAccess = new DirectRecordAccess( storeAccess, CacheAccess.EMPTY );
        pageCacheTracer = new DefaultPageCacheTracer();
    }

    @Test
    void tracePageCacheAccessOnNodeIndexCheck()
    {
        checkAssessOnIndexCheck( NODE_INDEX_NAME, nodeId );
    }

    @Test
    void tracePageCacheAccessOnRelationshipNodeIndexCheck()
    {
        checkAssessOnIndexCheck( RELATIONSHIP_INDEX_NAME, relationshipId );
    }

    private void checkAssessOnIndexCheck( String indexName, long entityId )
    {
        var indexDescriptor = getIndexDescriptor( indexName );
        var indexCheck = new IndexCheck( indexDescriptor );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "checkAssessOnIndexCheck" ) )
        {
            var indexEntry = new IndexEntry( indexDescriptor, tokenNameLookup, entityId );
            indexCheck.check( indexEntry, mock( CheckerEngine.class ), recordAccess, cursorTracer );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
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
