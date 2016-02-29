/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import java.util.Stack;

import org.neo4j.coreedge.raft.ReplicatedString;

public class PhysicalRaftLogEntryCursorTest
{
    private final RaftLogEntry entryA = new RaftLogEntry( 0, ReplicatedString.valueOf( "A" ) );
    private final RaftLogEntry entryB = new RaftLogEntry( 0, ReplicatedString.valueOf( "B" ) );
    private final RaftLogEntry entryC = new RaftLogEntry( 0, ReplicatedString.valueOf( "C" ) );
    private final RaftLogEntry entryD = new RaftLogEntry( 0, ReplicatedString.valueOf( "D" ) );

    @Test
    public void shouldReturnAppendedRecords() throws Exception
    {
        // given
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );

        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 0, entryA ) )
                .thenReturn( new RaftLogAppendRecord( 1, entryB ) )
                .thenReturn( new RaftLogAppendRecord( 2, entryC ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor, new Stack<>(), 0 );

        // when/then
        assertTrue( entryCursor.next() );
        assertEquals( entryA, entryCursor.get().getLogEntry() );
        assertTrue( entryCursor.next() );
        assertEquals( entryB, entryCursor.get().getLogEntry() );
        assertTrue( entryCursor.next() );
        assertEquals( entryC, entryCursor.get().getLogEntry() );
        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
        assertEquals( null, entryCursor.get() );
    }

    @Test
    public void shouldSkipUntilContinuation() throws Exception
    {
        // given
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );

        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 0, entryA ) )
                .thenReturn( new RaftLogAppendRecord( 1, entryB ) ) // truncated
                .thenReturn( new RaftLogAppendRecord( 2, entryC ) ) // truncated
                .thenReturn( new RaftLogContinuationRecord( 1 ) )
                .thenReturn( new RaftLogAppendRecord( 1, entryD ) )
                .thenReturn( null );

        Stack<Long> skipStack = new Stack<>();
        // this represents the state after truncating entryB, we skip from index 1 until the next continuation
        // the algorithm for generating the skip-stack lives in PhysicalRaftEntryStore
        skipStack.push( 1L );
        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor, skipStack, 0 );

        // when - then
        assertTrue( entryCursor.next() );
        assertEquals( entryA, entryCursor.get().getLogEntry() );
        assertTrue( entryCursor.next() );
        assertEquals( entryD, entryCursor.get().getLogEntry() );
        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
        assertEquals( null, entryCursor.get() );
    }
}
