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

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.SelectiveFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class RaftLogAdversarialTest
{
    public RaftLog createRaftLog( FileSystemAbstraction fileSystem )
    {
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );
        return new NaiveDurableRaftLog( fileSystem, directory, new DummyRaftableContentSerializer(),
                NullLogProvider.getInstance());
    }

    @Test
    public void shouldDiscardEntryIfEntryChannelFails() throws Exception
    {
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, false ),
                NaiveDurableRaftLog.class );
        adversary.disable();

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        FileSystemAbstraction fileSystem = new SelectiveFileSystemAbstraction(
                new File( "raft-log/entries.log" ), new AdversarialFileSystemAbstraction( adversary, fs ), fs );
        RaftLog log = createRaftLog( fileSystem );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        adversary.enable();
        try
        {
            log.append( logEntry );
            fail( "Should have thrown exception" );
        }
        catch ( Exception e )
        {
            // expected
        }

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, l -> {
            assertThat( l.appendIndex(), is( -1L ) );
            assertThat( l.commitIndex(), is( -1L ) );
            assertThat( l.entryExists( 0 ), is( false ) );
        } );
    }

    @Test
    public void shouldDiscardEntryIfContentChannelFails() throws Exception
    {
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, false ),
                NaiveDurableRaftLog.class );
        adversary.disable();

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        FileSystemAbstraction fileSystem = new SelectiveFileSystemAbstraction(
                new File( "raft-log/content.log" ), new AdversarialFileSystemAbstraction( adversary, fs ), fs );
        RaftLog log = createRaftLog( fileSystem );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        adversary.enable();
        try
        {
            log.append( logEntry );
            fail( "Should have thrown exception" );
        }
        catch ( Exception e )
        {
            // expected
        }

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, l -> {
            assertThat( l.appendIndex(), is( -1L ) );
            assertThat( l.commitIndex(), is( -1L ) );
            assertThat( l.entryExists( 0 ), is( false ) );
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            ReadableRaftLog log, FileSystemAbstraction fileSystem, LogVerifier logVerifier ) throws IOException
    {
        logVerifier.verifyLog( log );
        logVerifier.verifyLog( createRaftLog( fileSystem ) );
    }

    private interface LogVerifier
    {
        void verifyLog( ReadableRaftLog log ) throws IOException;
    }
}
