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

import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension
class PropertyReaderIT
{
    @Inject
    private GraphDatabaseAPI databaseAPI;
    @Inject
    private RecordStorageEngine storageEngine;
    private PropertyReader reader;
    private NeoStores neoStores;
    private DefaultPageCacheTracer pageCacheTracer;

    @BeforeEach
    void setUp()
    {
        neoStores = storageEngine.testAccessNeoStores();
        reader = new PropertyReader( new StoreAccess( neoStores ) );
        pageCacheTracer = new DefaultPageCacheTracer();
    }

    @Test
    void shouldDetectAndAbortPropertyChainLoadingOnCircularReference()
    {
        // Create property chain 1 --> 2 --> 3 --> 4
        //                             ↑           │
        //                             └───────────┘
        PropertyStore propertyStore = neoStores.getPropertyStore();
        PropertyRecord record = propertyStore.newRecord();
        // 1
        record.setId( 1 );
        record.initialize( true, -1, 2 );
        propertyStore.updateRecord( record, NULL );
        // 2
        record.setId( 2 );
        record.initialize( true, 1, 3 );
        propertyStore.updateRecord( record, NULL );
        // 3
        record.setId( 3 );
        record.initialize( true, 2, 4 );
        propertyStore.updateRecord( record, NULL );
        // 4
        record.setId( 4 );
        record.initialize( true, 3, 2 ); // <-- completing the circle
        propertyStore.updateRecord( record, NULL );

        // when
        var e = assertThrows(PropertyReader.CircularPropertyRecordChainException.class, () -> reader.getPropertyRecordChain( 1, NULL ) );
        assertEquals( 4, e.propertyRecordClosingTheCircle().getId() );
    }

    @Test
    void tracePageCacheAccessOnPropertyChainRead() throws PropertyReader.CircularPropertyRecordChainException
    {
        PropertyStore propertyStore = neoStores.getPropertyStore();
        PropertyRecord record = propertyStore.newRecord();
        record.setId( 1 );
        record.initialize( true, -1, 2 );
        propertyStore.updateRecord( record, NULL );
        record.setId( 2 );
        record.initialize( true, 1, -1 );
        propertyStore.updateRecord( record, NULL );

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnPropertyChainRead" ) )
        {
            reader.getPropertyRecordChain( 1, cursorTracer );

            assertCursorTracer( cursorTracer, 2 );
        }
    }

    @Test
    void tracePageCacheAccessOnPropertyValueRead()
    {
        var propertyStore = neoStores.getPropertyStore();
        var record = propertyStore.newRecord();
        record.setId( 1 );
        record.initialize( true, -1, -1 );

        PropertyBlock block = new PropertyBlock();
        TextValue expectedValue = Values.stringValue( randomAscii( 100 ) );
        propertyStore.encodeValue( block, 1, expectedValue, NULL, INSTANCE );
        record.addPropertyBlock( block );

        propertyStore.updateRecord( record, NULL );

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnPropertyValueRead" ) )
        {
            block.getValueRecords().clear();
            assertTrue( block.isLight() );

            reader.propertyValue( block, cursorTracer );

            assertCursorTracer( cursorTracer, 1 );
        }
    }

    @Test
    void tracePageCacheAccessOnNodePropertyValueRead()
    {
        long nodeId;
        try ( var tx = databaseAPI.beginTx() )
        {
            var node = tx.createNode();
            node.setProperty( "a", randomAscii( 1024 ) );
            nodeId = node.getId();
            tx.commit();
        }

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnPropertyValueRead" ) )
        {
            reader.getNodePropertyValue( nodeId, 1, cursorTracer );

            assertCursorTracer( cursorTracer, 2 );
        }
    }

    private static void assertCursorTracer( PageCursorTracer cursorTracer, int expectedValue )
    {
        assertThat( cursorTracer.pins() ).isEqualTo( expectedValue );
        assertThat( cursorTracer.unpins() ).isEqualTo( expectedValue );
        assertThat( cursorTracer.hits() ).isEqualTo( expectedValue );
    }
}
