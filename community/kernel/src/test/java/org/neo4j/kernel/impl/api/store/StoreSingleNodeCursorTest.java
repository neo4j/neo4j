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

import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;

public class StoreSingleNodeCursorTest
{
    private final ReadableTransactionState state = mock( ReadableTransactionState.class );
    private final RecordCursors recordCursors = mock( RecordCursors.class );
    private final StoreSingleNodeCursor cursor =
            new StoreSingleNodeCursor( new NodeRecord(-1), i -> {}, recordCursors, NO_LOCK_SERVICE );

    @Test
    public void shouldNotLoopForeverWhenNodesAreAddedToTheTxState() throws Exception
    {
        // given
        int nodeId = 42;
        when( state.nodeIsDeletedInThisTx( nodeId ) ).thenReturn( false );
        when( state.nodeIsAddedInThisTx( nodeId ) ).thenReturn( true );
        @SuppressWarnings( "unchecked" )
        RecordCursor<NodeRecord> recordCursor = mock( RecordCursor.class );
        when( recordCursor.next( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) ) ).thenReturn( false );
        when( recordCursors.node() ).thenReturn( recordCursor );

        // when
        cursor.init( nodeId, state );

        // then
        assertTrue( cursor.next() );
        assertFalse( cursor.next() );
    }
}
