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

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.coreedge.raft.state.id_allocation.InMemoryIdAllocationState.InMemoryIdAllocationStateChannelMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

public class IdAllocationStateRecoveryManagerTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private static final int NUMBER_OF_RECORDS_PER_FILE = 100;
    private static final int NUMBER_OF_BYTES_PER_RECORD = 10;

    @BeforeClass
    public static void checkArgs()
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
        FlushableChannel channel = new PhysicalFlushableChannel( fsa.open( file, "rw" ) );

        putRecordWithIndex( 42, channel );
        channel.close();

        IdAllocationStateRecoveryManager manager = new IdAllocationStateRecoveryManager( fsa,
                new InMemoryIdAllocationStateChannelMarshal() );

        // when
        final long logIndex = manager.getOrdinalOfLastRecord( file );

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
        FlushableChannel channelAWrite = new PhysicalFlushableChannel( fsa.create( fileA ) );

        putRecordWithIndex( 42, channelAWrite );

        channelAWrite.putLong( 101 ); // extraneous bytes
        channelAWrite.close();

        File fileB = new File( testDir.directory(), "fileB" );
        FlushableChannel channelBWrite = new PhysicalFlushableChannel( fsa.create( fileB ) );

        filUpWithRecords( channelBWrite );
        channelBWrite.close();

        IdAllocationStateRecoveryManager manager = new IdAllocationStateRecoveryManager( fsa,
                new InMemoryIdAllocationStateChannelMarshal() );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        // fileB goes up to 100 while A is at 42.
        assertEquals( fileA, recoveryStatus.previouslyInactive() );
        assertEquals( fileB, recoveryStatus.previouslyActive() );
    }

    private void filUpWithRecords( WritableChannel channel ) throws IOException
    {
        for ( int i = 0; i < NUMBER_OF_RECORDS_PER_FILE; i++ )
        {
             putRecordWithIndex( i, channel );
        }
    }

    private void putRecordWithIndex( long logIndex, WritableChannel channel ) throws IOException
    {
        InMemoryIdAllocationState state = new InMemoryIdAllocationState();
        state.logIndex( logIndex );
        new InMemoryIdAllocationStateChannelMarshal().marshal( state, channel );
    }
}
