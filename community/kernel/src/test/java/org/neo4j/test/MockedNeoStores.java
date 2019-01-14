/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.test;

import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockedNeoStores
{
    private MockedNeoStores()
    {
    }

    @SuppressWarnings( {"unchecked", "rawtypes"} )
    public static NeoStores basicMockedNeoStores()
    {
        NeoStores neoStores = mock( NeoStores.class );

        // Cursor, absolutely mocked and cannot be used at all as it is
        RecordCursor cursor = mockedRecordCursor();

        // NodeStore - DynamicLabelStore
        NodeStore nodeStore = mock( NodeStore.class );
        when( nodeStore.newRecordCursor( any() ) ).thenReturn( cursor );
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );

        // NodeStore - DynamicLabelStore
        DynamicArrayStore dynamicLabelStore = mock( DynamicArrayStore.class );
        when( dynamicLabelStore.newRecordCursor( any() ) ).thenReturn( cursor );
        when( nodeStore.getDynamicLabelStore() ).thenReturn( dynamicLabelStore );

        // RelationshipStore
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( relationshipStore.newRecordCursor( any() ) ).thenReturn( cursor );
        when( neoStores.getRelationshipStore() ).thenReturn( relationshipStore );

        // RelationshipGroupStore
        RelationshipGroupStore relationshipGroupStore = mock( RelationshipGroupStore.class );
        when( relationshipGroupStore.newRecordCursor( any() ) ).thenReturn( cursor );
        when( neoStores.getRelationshipGroupStore() ).thenReturn( relationshipGroupStore );

        // PropertyStore
        PropertyStore propertyStore = mock( PropertyStore.class );
        when( propertyStore.newRecordCursor( any() ) ).thenReturn( cursor );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );

        // PropertyStore -- DynamicStringStore
        DynamicStringStore propertyStringStore = mock( DynamicStringStore.class );
        when( propertyStringStore.newRecordCursor( any() ) ).thenReturn( cursor );
        when( propertyStore.getStringStore() ).thenReturn( propertyStringStore );

        // PropertyStore -- DynamicArrayStore
        DynamicArrayStore propertyArrayStore = mock( DynamicArrayStore.class );
        when( propertyArrayStore.newRecordCursor( any() ) ).thenReturn( cursor );
        when( propertyStore.getArrayStore() ).thenReturn( propertyArrayStore );

        return neoStores;
    }

    @SuppressWarnings( "unchecked" )
    public static <R extends AbstractBaseRecord> RecordCursor<R> mockedRecordCursor()
    {
        RecordCursor<R> cursor = mock( RecordCursor.class );
        when( cursor.acquire( anyLong(), any( RecordLoad.class ) ) ).thenReturn( cursor );
        return cursor;
    }
}
