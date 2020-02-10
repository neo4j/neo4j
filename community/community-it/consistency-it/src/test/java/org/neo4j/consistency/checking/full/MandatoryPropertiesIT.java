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

import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
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
class MandatoryPropertiesIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    private DefaultPageCacheTracer pageCacheTracer;
    private StoreAccess storeAccess;
    private NeoStores neoStores;

    @BeforeEach
    void setUp()
    {
        prepareData();

        pageCacheTracer = new DefaultPageCacheTracer();
        neoStores = storageEngine.testAccessNeoStores();
        storeAccess = new StoreAccess( neoStores );
        storeAccess.initialize();
    }

    @Test
    void trackPageCacheAccessOnCreation()
    {
        new MandatoryProperties( storeAccess, pageCacheTracer );

        assertThat( pageCacheTracer.pins() ).isEqualTo( 256 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 256 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 256 );
    }

    @Test
    void trackPageCacheAccessOnMandatoryPropertiesForNodesCheck() throws Exception
    {
        var mandatoryProperties = new MandatoryProperties( storeAccess, pageCacheTracer );
        var nodesCheck = mandatoryProperties.forNodes( mock( ConsistencyReporter.class ) );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnMandatoryPropertiesForNodesCheck" ) )
        {
            neoStores.getNodeStore().scanAllRecords( (Visitor<NodeRecord,Exception>) record ->
            {
                nodesCheck.apply( record, cursorTracer );
                return false;
            }, NULL );

            assertThat( cursorTracer.pins() ).isEqualTo( 2 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
            assertThat( cursorTracer.hits() ).isEqualTo( 2 );
        }
    }

    private void prepareData()
    {
        var startLabel = Label.label( "start" );
        var endLabel = Label.label( "end" );
        var relationshipType = RelationshipType.withName( "connection" );
        var propertyName = "property";
        try ( var tx = database.beginTx() )
        {
            var start = tx.createNode( startLabel );
            for ( int i = 0; i < 100; i++ )
            {
                start.addLabel( Label.label( "label" + i ) );
            }
            var end = tx.createNode( endLabel );
            start.createRelationshipTo( end, relationshipType );
            tx.commit();
        }
        try ( var tx = database.beginTx() )
        {
            tx.schema().indexFor( startLabel ).on( propertyName ).create();
            tx.commit();
        }

        try ( var tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, MINUTES );
        }
    }
}
