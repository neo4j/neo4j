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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.causalclustering.core.state.StateRecoveryManager;
import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StateRecoveryManagerTest
{

    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( testDir );

    private final int NUMBER_OF_RECORDS_PER_FILE = 100;
    private final int NUMBER_OF_BYTES_PER_RECORD = 10;

    @Before
    public void checkArgs()
    {
        assertEquals( 0, NUMBER_OF_RECORDS_PER_FILE % NUMBER_OF_BYTES_PER_RECORD );
    }

    @Test
    public void shouldFailIfBothFilesAreEmpty() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        File fileA = fileA();
        fsa.create( fileA );

        File fileB = fileB();
        fsa.create( fileB );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal() );

        try
        {
            // when
            StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );
            fail();
        }
        catch ( IllegalStateException ex )
        {
            // then
            // expected
        }
    }

    @Test
    public void shouldReturnPreviouslyInactiveWhenOneFileFullAndOneEmpty() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        File fileA = fileA();
        StoreChannel channel = fsa.create( fileA );

        fillUpAndForce( channel );

        File fileB = fileB();
        fsa.create( fileB );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal() );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        assertEquals( fileB, recoveryStatus.activeFile() );
    }

    @Test
    public void shouldReturnTheEmptyFileAsPreviouslyInactiveWhenActiveContainsCorruptEntry() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        File fileA = fileA();
        StoreChannel channel = fsa.create( fileA );

        ByteBuffer buffer = writeLong( 999 );
        channel.writeAll( buffer );
        channel.force( false );

        File fileB = fileB();
        channel = fsa.create( fileB );
        channel.close();

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal() );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        assertEquals( 999L, recoveryStatus.recoveredState() );
        assertEquals( fileB, recoveryStatus.activeFile() );
    }

    @Test
    public void shouldReturnTheFullFileAsPreviouslyInactiveWhenActiveContainsCorruptEntry()
            throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        File fileA = fileA();
        StoreChannel channel = fsa.create( fileA );

        ByteBuffer buffer = writeLong( 42 );
        channel.writeAll( buffer );
        channel.force( false );

        buffer.clear();
        buffer.putLong( 101 ); // extraneous bytes
        buffer.flip();
        channel.writeAll( buffer );
        channel.force( false );

        File fileB = fileB();
        fsa.create( fileB );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal() );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        assertEquals( fileB, recoveryStatus.activeFile() );
    }

    @Test
    public void shouldRecoverFromPartiallyWrittenEntriesInBothFiles() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal() );

        writeSomeLongsIn( fsa, fileA(), 3, 4 );
        writeSomeLongsIn( fsa, fileB(), 5, 6 );
        writeSomeGarbage( fsa, fileA() );
        writeSomeGarbage( fsa, fileB() );

        // when
        final StateRecoveryManager.RecoveryStatus recovered = manager.recover( fileA(), fileB() );

        // then
        assertEquals( fileA(), recovered.activeFile() );
        assertEquals( 6L, recovered.recoveredState() );
    }

    private File fileA()
    {
        return new File( testDir.directory(), "file.A" );
    }

    private File fileB()
    {
        return new File( testDir.directory(), "file.B" );
    }

    private void writeSomeGarbage( EphemeralFileSystemAbstraction fsa, File file ) throws IOException
    {
        final StoreChannel channel = fsa.open( file, "rw" );
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        buffer.putInt( 9876 );
        buffer.flip();
        channel.writeAll( buffer );
        channel.force( false );
        channel.close();
    }

    private void writeSomeLongsIn( EphemeralFileSystemAbstraction fsa, File file, long... longs ) throws IOException
    {
        final StoreChannel channel = fsa.open( file, "rw" );
        ByteBuffer buffer = ByteBuffer.allocate( longs.length * 8 );

        for ( long aLong : longs )
        {
            buffer.putLong( aLong );
        }

        buffer.flip();
        channel.writeAll( buffer );
        channel.force( false );
        channel.close();
    }

    private void fillUpAndForce( StoreChannel channel ) throws IOException
    {
        for ( int i = 0; i < NUMBER_OF_RECORDS_PER_FILE; i++ )
        {
            ByteBuffer buffer = writeLong( i );
            channel.writeAll( buffer );
            channel.force( false );
        }
    }

    private ByteBuffer writeLong( long logIndex )
    {
        ByteBuffer buffer = ByteBuffer.allocate( 8 );
        buffer.putLong( logIndex );
        buffer.flip();
        return buffer;
    }

    private static class LongMarshal extends SafeStateMarshal<Long>
    {
        @Override
        public Long startState()
        {
            return 0L;
        }

        @Override
        public long ordinal( Long aLong )
        {
            return aLong;
        }

        @Override
        public void marshal( Long aLong, WritableChannel channel ) throws IOException
        {
            channel.putLong( aLong );
        }

        @Override
        protected Long unmarshal0( ReadableChannel channel ) throws IOException
        {
            return channel.getLong();
        }
    }
}
