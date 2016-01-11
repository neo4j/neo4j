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
package org.neo4j.coreedge.raft.state.id_allocation;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.coreedge.raft.state.id_allocation.InMemoryIdAllocationState.InMemoryIdAllocationStateMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.raft.state.id_allocation.InMemoryIdAllocationState.InMemoryIdAllocationStateMarshal.NUMBER_OF_BYTES_PER_WRITE;

public class IdAllocationStateRecoveryManagerTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private final int NUMBER_OF_RECORDS_PER_FILE = 100;
    private final int NUMBER_OF_BYTES_PER_RECORD = 10;

    @Before
    public void checkArgs()
    {
        assertEquals( 0, NUMBER_OF_RECORDS_PER_FILE % NUMBER_OF_BYTES_PER_RECORD );
    }

    @Test
    public void shouldReturnCorrectLogIndex() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        File file = new File( testDir.directory(), "file" );
        StoreChannel channel = fsa.create( file );

        ByteBuffer buffer = partiallyFillWithWholeRecord( 42 );
        channel.writeAll( buffer );
        channel.force( false );

        IdAllocationStateRecoveryManager manager = new IdAllocationStateRecoveryManager( fsa );

        // when
        final long logIndex = manager.getLogIndex( file );

        // then
        assertEquals( 42, logIndex );
    }

    @Test
    public void shouldToleratePartiallyWrittenEntry() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        File fileA = new File( testDir.directory(), "fileA" );
        StoreChannel channel = fsa.create( fileA );

        ByteBuffer buffer = partiallyFillWithWholeRecord( 42 );
        channel.writeAll( buffer );
        channel.force( false );

        buffer.clear();
        buffer.putLong( 101 ); // extraneous bytes
        buffer.flip();
        channel.writeAll( buffer );
        channel.force( false );

        File fileB = new File( testDir.directory(), "fileB" );
        channel = fsa.create( fileB );

        fillUpAndForce( channel );
        channel.close();

        StateRecoveryManager manager = new IdAllocationStateRecoveryManager( fsa );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        // fileB goes up to 100 while A is at 42.
        assertEquals( fileA, recoveryStatus.previouslyInactive() );
        assertEquals( fileB, recoveryStatus.previouslyActive() );
    }

    private void fillUpAndForce( StoreChannel channel ) throws IOException
    {
        for ( int i = 0; i < NUMBER_OF_RECORDS_PER_FILE; i++ )
        {
            ByteBuffer buffer = partiallyFillWithWholeRecord( i );
            channel.writeAll( buffer );
            channel.force( false );
        }
    }

    private ByteBuffer partiallyFillWithWholeRecord( long logIndex )
    {
        ByteBuffer buffer = ByteBuffer.allocate( NUMBER_OF_BYTES_PER_WRITE );
        InMemoryIdAllocationState state = new InMemoryIdAllocationState();
        state.logIndex( logIndex );
        new InMemoryIdAllocationStateMarshal().marshal( state, buffer );
        buffer.flip();
        return buffer;
    }
}
