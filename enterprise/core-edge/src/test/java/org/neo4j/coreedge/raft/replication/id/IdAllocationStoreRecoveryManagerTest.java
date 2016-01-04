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
package org.neo4j.coreedge.raft.replication.id;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.raft.replication.id.InMemoryIdAllocationState.Serializer;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.raft.replication.id.InMemoryIdAllocationState.Serializer
        .NUMBER_OF_BYTES_PER_WRITE;

public class IdAllocationStoreRecoveryManagerTest
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
    public void shouldReturnNewWhenBothFilesAreEmpty() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        File fileA = new File( testDir.directory(), "fileA" );
        fsa.create( fileA );

        File fileB = new File( testDir.directory(), "fileB" );
        fsa.create( fileB );

        IdAllocationStoreRecoveryManager manager = new IdAllocationStoreRecoveryManager( fsa );

        // when
        File fileToUse = manager.recover( fileA, fileB );

        // then
        assertEquals( fileA, fileToUse );
    }

    @Test
    public void shouldReturnRecoverableWhenOneFileFullAndOneEmpty() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        File fileA = new File( testDir.directory(), "fileA" );
        StoreChannel channel = fsa.create( fileA );

        fillUpAndForce( channel );

        File fileB = new File( testDir.directory(), "fileB" );
        fsa.create( fileB );

        IdAllocationStoreRecoveryManager manager = new IdAllocationStoreRecoveryManager( fsa );

        // when
        File fileToUse = manager.recover( fileA, fileB );

        // then
        assertEquals( fileA, fileToUse );
    }

    @Test
    public void shouldReturnRecoverableWhenOneFileEmptyAndOnePartiallyFilledWithWholeRecords() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        File fileA = new File( testDir.directory(), "fileA" );
        StoreChannel channel = fsa.create( fileA );

        ByteBuffer buffer = partiallyFillWithWholeRecord( 999 );
        channel.writeAll( buffer );
        channel.force( false );

        File fileB = new File( testDir.directory(), "fileB" );
        channel = fsa.create( fileA );

        fillUpAndForce( channel );
        channel.close();

        IdAllocationStoreRecoveryManager manager = new IdAllocationStoreRecoveryManager( fsa );

        // when
        File fileToUse = manager.recover( fileA, fileB );

        // then
        assertEquals( fileA, fileToUse );
    }

    @Test
    public void shouldReturnRecoverableWhenOneFileEmptyAndOneContainsOneFullAndOnePartiallyWrittenRecord()
            throws Exception
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
        fsa.create( fileB );

        IdAllocationStoreRecoveryManager manager = new IdAllocationStoreRecoveryManager( fsa );

        // when
        File fileToUse = manager.recover( fileA, fileB );

        // then
        assertEquals( fileA, fileToUse );
    }

    @Test
    public void shouldReturnRecoverableWhenOneFileFullAndOneContainsPartiallyWrittenRecord() throws Exception
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

        IdAllocationStoreRecoveryManager manager = new IdAllocationStoreRecoveryManager( fsa );

        // when
        File fileToUse = manager.recover( fileA, fileB );

        // then
        // fileB goes up to 100 while A is at 42.
        assertEquals( fileB, fileToUse );
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
        InMemoryIdAllocationState store = new InMemoryIdAllocationState();
        store.logIndex( logIndex );
        new Serializer().serialize( store, buffer );
        buffer.flip();
        return buffer;
    }
}
