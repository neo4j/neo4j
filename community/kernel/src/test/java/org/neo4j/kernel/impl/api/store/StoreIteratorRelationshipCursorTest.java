/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StoreIteratorRelationshipCursorTest
{

    private static final long RELATIONSHIP_ID = 1L;

    @Test
    public void retrieveUsedRelationship() throws Exception
    {
        final RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( relationshipStore.fillRecord( eq( RELATIONSHIP_ID ), eq( relationshipRecord ), any( RecordLoad.class ) ) )
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
    public void impossibleToRetrieveUnusedRelationship()
    {
        final RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( relationshipStore.fillRecord( eq( RELATIONSHIP_ID ), eq( relationshipRecord ), any( RecordLoad.class ) ) )
                .thenAnswer( new RelationshipAnswer( relationshipRecord, false ) );

        try ( StoreIteratorRelationshipCursor cursor = createRelationshipCursor( relationshipRecord,
                relationshipStore ) )
        {
            cursor.init( PrimitiveLongCollections.iterator( RELATIONSHIP_ID ) );
            assertFalse( cursor.next() );
        }
    }

    private StoreIteratorRelationshipCursor createRelationshipCursor( RelationshipRecord relationshipRecord,
            RelationshipStore relationshipStore )
    {
        NeoStores neoStores = mock( NeoStores.class );
        StoreStatement storeStatement = mock( StoreStatement.class );
        when( neoStores.getRelationshipStore() ).thenReturn( relationshipStore );
        InstanceCache<StoreIteratorRelationshipCursor> instanceCache = new TestCursorCache();
        return new StoreIteratorRelationshipCursor( relationshipRecord, neoStores, storeStatement, instanceCache,
                LockService.NO_LOCK_SERVICE );
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
        public Boolean answer( InvocationOnMock invocationOnMock ) throws Throwable
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