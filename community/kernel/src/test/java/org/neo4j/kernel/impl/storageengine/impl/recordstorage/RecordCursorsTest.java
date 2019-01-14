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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

class RecordCursorsTest
{
    @Test
    void nodeCursorShouldClosePageCursor()
    {
        NodeStore store = mock( NodeStore.class );
        PageCursor pageCursor = mock( PageCursor.class );
        when( store.openPageCursorForReading( anyLong() ) ).thenReturn( pageCursor );

        try ( RecordNodeCursor cursor = new RecordNodeCursor( store ) )
        {
            cursor.single( 0 );
        }
        verify( pageCursor ).close();
    }

    @Test
    void relationshipScanCursorShouldClosePageCursor()
    {
        RelationshipStore store = mock( RelationshipStore.class );
        PageCursor pageCursor = mock( PageCursor.class );
        when( store.openPageCursorForReading( anyLong() ) ).thenReturn( pageCursor );

        try ( RecordRelationshipScanCursor cursor = new RecordRelationshipScanCursor( store ) )
        {
            cursor.single( 0 );
        }
        verify( pageCursor ).close();
    }

    @Test
    void relationshipTraversalCursorShouldClosePageCursor()
    {
        RelationshipStore store = mock( RelationshipStore.class );
        PageCursor pageCursor = mock( PageCursor.class );
        when( store.openPageCursorForReading( anyLong() ) ).thenReturn( pageCursor );
        RelationshipGroupStore groupStore = mock( RelationshipGroupStore.class );
        PageCursor groupPageCursor = mock( PageCursor.class );
        when( store.openPageCursorForReading( anyLong() ) ).thenReturn( pageCursor );

        try ( RecordRelationshipTraversalCursor cursor = new RecordRelationshipTraversalCursor( store, groupStore ) )
        {
            cursor.init( 0, 0 );
        }
        verify( pageCursor ).close();
        verifyZeroInteractions( groupPageCursor, groupStore );
    }

    @Test
    void relationshipGroupCursorShouldClosePageCursor()
    {
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        PageCursor relationshipPageCursor = mock( PageCursor.class );
        when( relationshipStore.openPageCursorForReading( anyLong() ) ).thenReturn( relationshipPageCursor );
        RelationshipGroupStore store = mock( RelationshipGroupStore.class );
        PageCursor pageCursor = mock( PageCursor.class );
        when( store.openPageCursorForReading( anyLong() ) ).thenReturn( pageCursor );

        try ( RecordRelationshipGroupCursor cursor = new RecordRelationshipGroupCursor( relationshipStore, store ) )
        {
            cursor.init( 0, 0 );
        }
        verify( pageCursor ).close();
        verifyZeroInteractions( relationshipStore, relationshipPageCursor );
    }

    @Test
    void propertyCursorShouldClosePageCursor()
    {
        PropertyStore store = mock( PropertyStore.class );
        PageCursor pageCursor = mock( PageCursor.class );
        when( store.openPageCursorForReading( anyLong() ) ).thenReturn( pageCursor );

        try ( RecordPropertyCursor cursor = new RecordPropertyCursor( store ) )
        {
            cursor.init( 0 );
        }
        verify( pageCursor ).close();
    }
}
