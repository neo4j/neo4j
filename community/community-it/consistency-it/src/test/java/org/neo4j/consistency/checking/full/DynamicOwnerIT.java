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
import org.neo4j.consistency.checking.LabelTokenRecordCheck;
import org.neo4j.consistency.checking.NodeRecordCheck;
import org.neo4j.consistency.checking.PropertyKeyTokenRecordCheck;
import org.neo4j.consistency.checking.PropertyRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
class DynamicOwnerIT
{
    private static final int TEST_TRANSACTIONS = 100;

    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    private NeoStores neoStores;
    private DirectRecordAccess recordAccess;
    private DefaultPageCacheTracer pageCacheTracer;
    private final OwnerCheck ownerCheck = new OwnerCheck( true );

    @BeforeEach
    void setUp()
    {
        neoStores = storageEngine.testAccessNeoStores();
        var storeAccess = new StoreAccess( neoStores );
        storeAccess.initialize();
        recordAccess = new DirectRecordAccess( storeAccess, CacheAccess.EMPTY );
        pageCacheTracer = new DefaultPageCacheTracer();
    }

    @Test
    void tracePageCacheAccessOnNodeOwnerCheck() throws Exception
    {
        checkOwner( neoStores.getNodeStore(), ownerCheck.decorateNodeChecker( new NodeRecordCheck() ), 400 );
    }

    @Test
    void tracePageCacheAccessOnPropertyOwnerCheck() throws Exception
    {
        checkOwner( neoStores.getPropertyStore(), ownerCheck.decoratePropertyChecker( new PropertyRecordCheck() ), 200 );
    }

    @Test
    void tracePageCacheAccessOnRelationshipOwnerCheck() throws Exception
    {
        checkOwner( neoStores.getRelationshipStore(), ownerCheck.decorateRelationshipChecker( new RelationshipRecordCheck() ), 200 );
    }

    @Test
    void tracePageCacheAccessOnKeyTokenOwnerCheck() throws Exception
    {
        checkOwner( neoStores.getPropertyKeyTokenStore(), ownerCheck.decoratePropertyKeyTokenChecker( new PropertyKeyTokenRecordCheck() ), 200 );
    }

    @Test
    void tracePageCacheAccessOnLabelOwnerCheck() throws Exception
    {
        checkOwner( neoStores.getLabelTokenStore(), ownerCheck.decorateLabelTokenChecker( new LabelTokenRecordCheck() ), 200 );
    }

    private <T extends AbstractBaseRecord> void checkOwner( CommonAbstractStore<T,? extends StoreHeader> store,
            RecordCheck<T,? extends ConsistencyReport> check, int expected ) throws Exception
    {
        prepareData();

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "checkOwner" ) )
        {
            checkAllRecords( store, check, cursorTracer );

            assertThat( cursorTracer.pins() ).isEqualTo( expected );
            assertThat( cursorTracer.unpins() ).isEqualTo( expected );
            assertThat( cursorTracer.hits() ).isEqualTo( expected );
        }
    }

    private <T extends AbstractBaseRecord> void checkAllRecords( CommonAbstractStore<T,? extends StoreHeader> store,
            RecordCheck<T,? extends ConsistencyReport> check,
            PageCursorTracer cursorTracer )
            throws Exception
    {
        store.scanAllRecords( (Visitor<T,Exception>) record ->
        {
            check.check( record, mock( CheckerEngine.class ), recordAccess, cursorTracer );
            return false;
        }, NULL );
    }

    private void prepareData()
    {
        var type = RelationshipType.withName( "any" );
        for ( int i = 0; i < TEST_TRANSACTIONS; i++ )
        {
            try ( var tx = database.beginTx() )
            {
                var start = tx.createNode( Label.label( "start" + i ) );
                var end = tx.createNode( Label.label( "end" + i ) );
                start.setProperty( "a" + i, "b" );
                end.setProperty( "c" + i, "d" );
                start.createRelationshipTo( end, type );
                end.createRelationshipTo( start, type );
                tx.commit();
            }
        }
    }
}
