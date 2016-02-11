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
import org.neo4j.coreedge.raft.ReplicatedInteger;

public class PhysicalRaftLogEntryCursorTest
{
    @Test
    public void shouldReturnAllRecordsIfAllAreAppended() throws Exception
    {
        // Given
        ReplicatedInteger content = ReplicatedInteger.valueOf( 1 );
        RaftLogEntry payload = new RaftLogEntry( 0, content );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        // return 3 records that are just appended, then be done with it
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 1, payload ) )
                .thenReturn( new RaftLogAppendRecord( 2, payload ) )
                .thenReturn( new RaftLogAppendRecord( 3, payload ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertTrue( entryCursor.next() );
        assertEquals( payload, entryCursor.get().getLogEntry() );
        assertTrue( entryCursor.next() );
        assertEquals( payload, entryCursor.get().getLogEntry() );
        assertTrue( entryCursor.next() );
        assertEquals( payload, entryCursor.get().getLogEntry() );

        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
    }

    @Test
    public void shouldReturnNonTruncatedRecords() throws Exception
    {
        // Given
        ReplicatedInteger content = ReplicatedInteger.valueOf( 1 );
        RaftLogEntry payload = new RaftLogEntry( 0, content );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        // return 3 records that are just appended, then be done with it
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 1, payload ) )
                .thenReturn( new RaftLogAppendRecord( 2, payload ) )
                .thenReturn( new RaftLogTruncateRecord( 2 ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertTrue( entryCursor.next() );
        assertEquals( payload, entryCursor.get().getLogEntry() );

        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
    }
    @Test
    public void shouldNotReturnAnyRecordsIfTheyAreAllTruncated() throws Exception
    {
        // Given
        ReplicatedInteger content = ReplicatedInteger.valueOf( 1 );
        RaftLogEntry payload = new RaftLogEntry( 0, content );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        // return 3 records that are just appended, then be done with it
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 1, payload ) )
                .thenReturn( new RaftLogAppendRecord( 2, payload ) )
                .thenReturn( new RaftLogTruncateRecord( 2 ) )
                .thenReturn( new RaftLogTruncateRecord( 1 ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertFalse( entryCursor.next() );
    }

    @Test
    public void shouldReturnCommittedRecords() throws Exception
    {
        // Given
        ReplicatedInteger content = ReplicatedInteger.valueOf( 1 );
        RaftLogEntry payload = new RaftLogEntry( 0, content );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        // return 3 records that are just appended, then be done with it
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 1, payload ) )
                .thenReturn( new RaftLogAppendRecord( 2, payload ) )
                .thenReturn( new RaftLogCommitRecord( 2 ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertTrue( entryCursor.next() );
        assertEquals( payload, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload, entryCursor.get().getLogEntry() );

        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
    }

    @Test
    public void shouldSkipTruncatedAndReturnCommittedRecords() throws Exception
    {
        // Given
        RaftLogEntry payloadTruncated = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 4 ) );
        RaftLogEntry payload5 = new RaftLogEntry( 5, ReplicatedInteger.valueOf( 5 ) );
        RaftLogEntry payload6 = new RaftLogEntry( 6, ReplicatedInteger.valueOf( 6 ) );
        RaftLogEntry payload7 = new RaftLogEntry( 7, ReplicatedInteger.valueOf( 7 ) );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        // return 3 records that are just appended, then be done with it
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 5, payload5 ) )
                .thenReturn( new RaftLogAppendRecord( 6, payloadTruncated ) )
                .thenReturn( new RaftLogAppendRecord( 7, payloadTruncated ) )
                .thenReturn( new RaftLogTruncateRecord( 6 ) )
                .thenReturn( new RaftLogAppendRecord( 6, payload6 ) )
                .thenReturn( new RaftLogAppendRecord( 7, payload7 ) )
                .thenReturn( new RaftLogCommitRecord( 7 ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertTrue( entryCursor.next() );
        assertEquals( payload5, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload6, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload7, entryCursor.get().getLogEntry() );

        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
    }

    @Test
    public void shouldSkipTruncatedAndReturnOnEndOfFile() throws Exception
    {
        // Given
        RaftLogEntry payloadTruncated = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 4 ) );
        RaftLogEntry payload5 = new RaftLogEntry( 5, ReplicatedInteger.valueOf( 5 ) );
        RaftLogEntry payload6 = new RaftLogEntry( 6, ReplicatedInteger.valueOf( 6 ) );
        RaftLogEntry payload7 = new RaftLogEntry( 7, ReplicatedInteger.valueOf( 7 ) );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        // return 3 records that are just appended, then be done with it
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 5, payload5 ) )
                .thenReturn( new RaftLogAppendRecord( 6, payloadTruncated ) )
                .thenReturn( new RaftLogAppendRecord( 7, payloadTruncated ) )
                .thenReturn( new RaftLogTruncateRecord( 6 ) )
                .thenReturn( new RaftLogAppendRecord( 6, payload6 ) )
                .thenReturn( new RaftLogAppendRecord( 7, payload7 ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertTrue( entryCursor.next() );
        assertEquals( payload5, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload6, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload7, entryCursor.get().getLogEntry() );

        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
    }

    @Test
    public void shouldSkipTruncatedAndReturnCommittedRecordsAndRecordsAtEndOfFile() throws Exception
    {
        // Given
        RaftLogEntry payloadTruncated = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 4 ) );
        RaftLogEntry payload5 = new RaftLogEntry( 5, ReplicatedInteger.valueOf( 5 ) );
        RaftLogEntry payload6 = new RaftLogEntry( 6, ReplicatedInteger.valueOf( 6 ) );
        RaftLogEntry payload7 = new RaftLogEntry( 7, ReplicatedInteger.valueOf( 7 ) );
        RaftLogEntry payload8 = new RaftLogEntry( 7, ReplicatedInteger.valueOf( 8 ) );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        // return 3 records that are just appended, then be done with it
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 5, payload5 ) )
                .thenReturn( new RaftLogAppendRecord( 6, payloadTruncated ) )
                .thenReturn( new RaftLogAppendRecord( 7, payloadTruncated ) )
                .thenReturn( new RaftLogTruncateRecord( 6 ) )
                .thenReturn( new RaftLogAppendRecord( 6, payload6 ) )
                .thenReturn( new RaftLogAppendRecord( 7, payload7 ) )
                .thenReturn( new RaftLogCommitRecord( 7 ) )
                .thenReturn( new RaftLogAppendRecord( 8, payload8 ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertTrue( entryCursor.next() );
        assertEquals( payload5, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload6, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload7, entryCursor.get().getLogEntry() );

        assertTrue( entryCursor.next() );
        assertEquals( payload8, entryCursor.get().getLogEntry() );

        assertFalse( entryCursor.next() ); // record cursor is done, there should be no more entries/
    }

    @Test
    public void shouldReturnUncommittedEntriesAtEndOfFileDespiteCommitEntryForNonEncounteredRecords() throws Exception
    {
        // Given
        RaftLogEntry payload5 = new RaftLogEntry( 5, ReplicatedInteger.valueOf( 5 ) );
        RaftRecordCursor recordCursor = mock( RaftRecordCursor.class );
        when( recordCursor.next() )
                .thenReturn( true )
                .thenReturn( true )
                .thenReturn( false );
        when( recordCursor.get() )
                .thenReturn( new RaftLogAppendRecord( 5, payload5 ) )
                .thenReturn( new RaftLogCommitRecord( 2 ) )
                .thenReturn( null );

        PhysicalRaftLogEntryCursor entryCursor = new PhysicalRaftLogEntryCursor( recordCursor );

        // When - Then
        assertTrue( entryCursor.next() );
        assertEquals( payload5, entryCursor.get().getLogEntry() );
    }
}
