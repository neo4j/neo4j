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
package org.neo4j.kernel.impl.store;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordCursorsTest
{
    @Test
    public void closesCursors()
    {
        RecordCursors cursors = newRecordCursorsWithMockedNeoStores();

        cursors.close();

        verifyAllCursorsClosed( cursors );
    }

    @Test
    public void closesCursorsEvenIfSomeCursorFailsToClose()
    {
        RecordCursors cursors = newRecordCursorsWithMockedNeoStores();
        RecordCursor<RelationshipGroupRecord> relGroupCursor = cursors.relationshipGroup();
        RuntimeException exception = new RuntimeException( "Close failure" );
        doThrow( exception ).when( relGroupCursor ).close();

        try
        {
            cursors.close();
        }
        catch ( Exception e )
        {
            assertSame( exception, e.getCause() );
        }

        verifyAllCursorsClosed( cursors );
    }

    private static void verifyAllCursorsClosed( RecordCursors recordCursors )
    {
        verify( recordCursors.node() ).close();
        verify( recordCursors.relationship() ).close();
        verify( recordCursors.relationshipGroup() ).close();
        verify( recordCursors.property() ).close();
        verify( recordCursors.propertyString() ).close();
        verify( recordCursors.propertyArray() ).close();
        verify( recordCursors.label() ).close();
    }

    private static RecordCursors newRecordCursorsWithMockedNeoStores()
    {
        NeoStores neoStores = mock( NeoStores.class );
        NodeStore nodeStore = newStoreMockWithRecordCursor( NodeStore.class );
        RelationshipStore relStore = newStoreMockWithRecordCursor( RelationshipStore.class );
        RelationshipGroupStore relGroupStore = newStoreMockWithRecordCursor( RelationshipGroupStore.class );
        PropertyStore propertyStore = newStoreMockWithRecordCursor( PropertyStore.class );
        DynamicStringStore dynamicStringStore = newStoreMockWithRecordCursor( DynamicStringStore.class );
        DynamicArrayStore dynamicArrayStore = newStoreMockWithRecordCursor( DynamicArrayStore.class );
        DynamicArrayStore dynamicLabelStore = newStoreMockWithRecordCursor( DynamicArrayStore.class );

        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        when( neoStores.getRelationshipStore() ).thenReturn( relStore );
        when( neoStores.getRelationshipGroupStore() ).thenReturn( relGroupStore );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );
        when( propertyStore.getStringStore() ).thenReturn( dynamicStringStore );
        when( propertyStore.getArrayStore() ).thenReturn( dynamicArrayStore );
        when( nodeStore.getDynamicLabelStore() ).thenReturn( dynamicLabelStore );

        return new RecordCursors( neoStores );
    }

    private static <S extends RecordStore<R>, R extends AbstractBaseRecord> S newStoreMockWithRecordCursor(
            Class<S> storeClass )
    {
        S storeMock = mock( storeClass );
        RecordCursor<R> cursor = newCursorMock();
        when( storeMock.newRecordCursor( any() ) ).thenReturn( cursor );
        return storeMock;
    }

    @SuppressWarnings( "unchecked" )
    private static <T extends AbstractBaseRecord> RecordCursor<T> newCursorMock()
    {
        RecordCursor<T> cursor = mock( RecordCursor.class );
        when( cursor.acquire( anyLong(), any() ) ).thenReturn( cursor );
        return cursor;
    }
}
