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

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.NodeRecordCheck;
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
class CountsBuilderDecoratorIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    private NeoStores neoStores;
    private CountsBuilderDecorator countsDecorator;
    private ConsistencyReporter consistencyReporter;
    private DefaultPageCacheTracer pageCacheTracer;
    private DirectRecordAccess recordAccess;

    @BeforeEach
    void setUp()
    {
        prepareData();
        neoStores = storageEngine.testAccessNeoStores();
        var storeAccess = new StoreAccess( neoStores );
        storeAccess.initialize();
        countsDecorator = new CountsBuilderDecorator( storeAccess );
        recordAccess = new DirectRecordAccess( storeAccess, CacheAccess.EMPTY );
        pageCacheTracer = new DefaultPageCacheTracer();
        consistencyReporter = new ConsistencyReporter( recordAccess, mock( InconsistencyReport.class ), pageCacheTracer );
    }

    @Test
    void trackPageCacheAccessOnCountsCheck() throws Exception
    {
        prepareDecorator( neoStores, countsDecorator, recordAccess );

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnCountsCheck" ) )
        {
            countsDecorator.checkCounts( storageEngine.countsAccessor(), consistencyReporter, NONE, cursorTracer );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    @Test
    void tracePageCacheAccessOnNodeCheck() throws Exception
    {
        countsDecorator.prepare();
        var nodeCheck = countsDecorator.decorateNodeChecker( new NodeRecordCheck() );
        var fakeEngine = mock( CheckerEngine.class );

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnNodeCheck" ) )
        {
            neoStores.getNodeStore().scanAllRecords( (Visitor<NodeRecord,Exception>) node ->
            {
                nodeCheck.check( node, fakeEngine, recordAccess, cursorTracer );
                return false;
            }, NULL );

            assertThat( cursorTracer.pins() ).isEqualTo( 600 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 600 );
            assertThat( cursorTracer.hits() ).isEqualTo( 600 );
        }
    }

    @Test
    void tracePageCacheAccessOnRelationshipCheck() throws Exception
    {
        countsDecorator.prepare();
        countsDecorator.prepare();
        var relCheck = countsDecorator.decorateRelationshipChecker( new RelationshipRecordCheck() );
        var fakeEngine = mock( CheckerEngine.class );

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnRelationshipCheck" ) )
        {
            neoStores.getRelationshipStore().scanAllRecords( (Visitor<RelationshipRecord,Exception>) record ->
            {
                relCheck.check( record, fakeEngine, recordAccess, cursorTracer );
                return false;
            }, NULL );

            assertThat( cursorTracer.pins() ).isEqualTo( 100 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 100 );
            assertThat( cursorTracer.hits() ).isEqualTo( 100 );
        }
    }

    private void prepareData()
    {
        try ( var tx = database.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                var start = tx.createNode( Label.label( "marker" + i ) );
                var end = tx.createNode( Label.label( "endMarker" + i ) );
                start.createRelationshipTo( end, withName( "type" + i ) );
            }
            tx.commit();
        }
    }

    private void prepareDecorator( NeoStores neoStores, CountsBuilderDecorator countsDecorator, DirectRecordAccess recordAccess ) throws Exception
    {
        countsDecorator.prepare();
        var nodeCheck = countsDecorator.decorateNodeChecker( new NodeRecordCheck() );
        var relCheck = countsDecorator.decorateRelationshipChecker( new RelationshipRecordCheck() );
        var fakeEngine = mock( CheckerEngine.class );

        neoStores.getNodeStore().scanAllRecords( (Visitor<NodeRecord,Exception>) node ->
        {
            nodeCheck.check( node, fakeEngine, recordAccess, NULL );
            return false;
        }, NULL );
        countsDecorator.prepare();
        neoStores.getRelationshipStore().scanAllRecords( (Visitor<RelationshipRecord,Exception>) record ->
        {
            relCheck.check( record, fakeEngine, recordAccess, NULL );
            return false;
        }, NULL );
    }
}
