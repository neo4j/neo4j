/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

public class StoreIteratorRelationshipCursorTest
{
    private static final long RELATIONSHIP_ID = 1L;

    @Test
    public void retrieveUsedRelationship() throws Exception
    {
        try ( StoreIteratorRelationshipCursor cursor = createRelationshipCursor( true ) )
        {
            cursor.init( PrimitiveLongCollections.iterator( RELATIONSHIP_ID ), null );
            assertTrue( cursor.next() );
            assertEquals( RELATIONSHIP_ID, cursor.get().id() );
        }
    }

    @Test
    public void retrieveUnusedRelationship() throws IOException
    {
        try ( StoreIteratorRelationshipCursor cursor = createRelationshipCursor( false ) )
        {
            cursor.init( PrimitiveLongCollections.iterator( RELATIONSHIP_ID ), null );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldCloseThePageCursorWhenDisposed() throws IOException
    {
        StoreIteratorRelationshipCursor cursor = createRelationshipCursor( false );
        cursor.close();
        cursor.dispose();

        verify( pageCursor ).close();
    }

    private final PageCursor pageCursor = mock( PageCursor.class );

    private StoreIteratorRelationshipCursor createRelationshipCursor( boolean relationshipInUse ) throws IOException
    {
        final RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( relationshipStore.newRecord() ).thenReturn( relationshipRecord );
        when( relationshipStore.newPageCursor() ).thenReturn( pageCursor );
        when( relationshipStore.readRecord( RELATIONSHIP_ID, relationshipRecord, CHECK, pageCursor ) ).thenAnswer( i ->
        {
            relationshipRecord.setInUse( relationshipInUse );
            relationshipRecord.setId( RELATIONSHIP_ID );
            return relationshipRecord;
        });
        return new StoreIteratorRelationshipCursor( relationshipStore, new TestCursorCache(), NO_LOCK_SERVICE );
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
