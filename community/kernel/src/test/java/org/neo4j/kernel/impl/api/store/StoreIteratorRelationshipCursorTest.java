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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StoreIteratorRelationshipCursorTest
{

    private static final long RELATIONSHIP_ID = 1L;

    @Test
    public void retrieveUsedRelationship()
    {
        final RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        RecordCursor recordCursor = mock( RecordCursor.class );
        RelationshipStore relationshipStore = getRelationshipStore( relationshipRecord, recordCursor );
        when( recordCursor.next( RELATIONSHIP_ID, relationshipRecord, RecordLoad.CHECK  ) )
                .thenAnswer( new RelationshipAnswer( relationshipRecord, true ) );

        try ( StoreIteratorRelationshipCursor cursor = createRelationshipCursor( relationshipRecord,
                relationshipStore ) )
        {
            cursor.init( PrimitiveLongCollections.iterator( RELATIONSHIP_ID ) );
            assertTrue( cursor.next() );
            assertEquals( RELATIONSHIP_ID, cursor.get().id() );
        }
    }

    @Test
    public void retrieveUnusedRelationship()
    {
        final RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        RecordCursor recordCursor = mock( RecordCursor.class );
        RelationshipStore relationshipStore = getRelationshipStore( relationshipRecord, recordCursor );
        when( recordCursor.next( RELATIONSHIP_ID, relationshipRecord, RecordLoad.CHECK  ) )
                .thenAnswer( new RelationshipAnswer( relationshipRecord, false ) );

        try ( StoreIteratorRelationshipCursor cursor = createRelationshipCursor( relationshipRecord,
                relationshipStore ) )
        {
            cursor.init( PrimitiveLongCollections.iterator( RELATIONSHIP_ID ) );
            assertTrue( cursor.next() );
            assertEquals( RELATIONSHIP_ID, cursor.get().id() );
        }
    }

    private StoreIteratorRelationshipCursor createRelationshipCursor( RelationshipRecord relationshipRecord,
            RelationshipStore relationshipStore )
    {
        RecordCursors recordCursors = newRecordCursorsWithMockedNeoStores(relationshipStore);
        InstanceCache<StoreIteratorRelationshipCursor> instanceCache = new TestCursorCache();
        return new StoreIteratorRelationshipCursor( relationshipRecord, instanceCache, recordCursors,
                LockService.NO_LOCK_SERVICE );
    }

    private RelationshipStore getRelationshipStore( RelationshipRecord relationshipRecord, RecordCursor recordCursor )
    {
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( recordCursor.acquire( anyLong(), any() ) ).thenReturn( recordCursor );
        when( relationshipStore.newRecord() ).thenReturn( relationshipRecord );
        when( relationshipStore.newRecordCursor( relationshipRecord ) ).thenReturn( recordCursor );
        return relationshipStore;
    }

    private static RecordCursors newRecordCursorsWithMockedNeoStores( RelationshipStore relationshipStore )
    {
        NeoStores neoStores = mock( NeoStores.class );
        NodeStore nodeStore = newStoreMockWithRecordCursor( NodeStore.class );
        RelationshipGroupStore relGroupStore = newStoreMockWithRecordCursor( RelationshipGroupStore.class );
        PropertyStore propertyStore = newStoreMockWithRecordCursor( PropertyStore.class );
        DynamicStringStore dynamicStringStore = newStoreMockWithRecordCursor( DynamicStringStore.class );
        DynamicArrayStore dynamicArrayStore = newStoreMockWithRecordCursor( DynamicArrayStore.class );
        DynamicArrayStore dynamicLabelStore = newStoreMockWithRecordCursor( DynamicArrayStore.class );

        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        when( neoStores.getRelationshipStore() ).thenReturn( relationshipStore );
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

    private static class RelationshipAnswer implements Answer<Boolean>
    {
        private final RelationshipRecord relationshipRecord;
        private boolean used;

        RelationshipAnswer( RelationshipRecord relationshipRecord, boolean used )
        {
            this.relationshipRecord = relationshipRecord;
            this.used = used;
        }

        @Override
        public Boolean answer( InvocationOnMock invocationOnMock )
        {
            relationshipRecord.setInUse( used );
            relationshipRecord.setId( RELATIONSHIP_ID );
            return true;
        }
    }

    private class TestCursorCache extends InstanceCache<StoreIteratorRelationshipCursor>
    {
        @Override
        protected StoreIteratorRelationshipCursor create()
        {
            return null;
        }
    }
}
