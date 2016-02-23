/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.function.Consumer;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.test.MockedNeoStores;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class StoreNodeRelationshipCursorTest
{
    @Test
    public void shouldHandleDenseNodeWithNoRelationships() throws Exception
    {
        // This can actually happen, since we upgrade sparse node --> dense node when creating relationships,
        // but we don't downgrade dense --> sparse when we delete relationships. So if we have a dense node
        // which no longer has relationships, there was this assumption that we could just call getRecord
        // on the NodeRecord#getNextRel() value. Although that value could actually be -1

        // GIVEN
        NeoStores stores = MockedNeoStores.basicMockedNeoStores();

        @SuppressWarnings( "unchecked" )
        StoreNodeRelationshipCursor cursor = new StoreNodeRelationshipCursor(
                new RelationshipRecord( -1 ),
                stores,
                new RelationshipGroupRecord( -1 ),
                mock( StoreStatement.class ),
                mock( Consumer.class ),
                NO_LOCK_SERVICE, new RecordCursors( stores ) );
        reset( stores.getRelationshipGroupStore() );

        // WHEN
        cursor.init( true, NO_NEXT_RELATIONSHIP.intValue(), 0, Direction.BOTH );

        // THEN
        verifyNoMoreInteractions( stores.getRelationshipGroupStore() );
        assertFalse( cursor.next() );
    }
}
