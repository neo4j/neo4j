/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Test;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RaftLogDurabilityTest
{
    public RaftLog createRaftLog( EphemeralFileSystemAbstraction fileSystem )
    {
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );
        return new NaiveDurableRaftLog( fileSystem, directory, new DummyRaftableContentSerializer(), new Monitors() );
    }

    @Test
    public void shouldAppendDataAndNotCommitImmediately() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        RaftLog log = createRaftLog( fileSystem );

        final RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, myLog -> {
            assertThat( myLog.appendIndex(), is( 0L ) );
            assertThat( myLog.commitIndex(), is( -1L ) );
            assertThat( myLog.entryExists( 0 ), is( true ) );
            assertThat( myLog.readLogEntry( 0 ), equalTo( logEntry ) );
        } );
    }

    @Test
    public void shouldAppendAndCommit() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        RaftLog log = createRaftLog( fileSystem );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );
        log.commit( 0 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, myLog -> {
            assertThat( myLog.appendIndex(), is( 0L ) );
            assertThat( myLog.commitIndex(), is( 0L ) );
            assertThat( myLog.entryExists( 0 ), is( true ) );
        } );
    }

    @Test
    public void shouldAppendAfterReloadingFromFileSystem() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        RaftLog log = createRaftLog( fileSystem );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntryA );

        fileSystem.crash();
        log = createRaftLog( fileSystem );

        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );

        assertThat( log.entryExists( 0 ), is( true ) );
        assertThat( log.readLogEntry( 0 ), is( logEntryA ) );

        assertThat( log.entryExists( 1 ), is( true ) );
        assertThat( log.readLogEntry( 1 ), is( logEntryB ) );

        assertThat( log.entryExists( 2 ), is( false ) );
    }

    @Test
    public void shouldTruncatePreviouslyAppendedEntries() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        RaftLog log = createRaftLog( fileSystem );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.truncate( 1 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, myLog -> {
            assertThat( myLog.appendIndex(), is( 0L ) );
            assertThat( myLog.entryExists( 0 ), is( true ) );
            assertThat( myLog.entryExists( 1 ), is( false ) );
        } );
    }

    @Test
    public void shouldReplacePreviouslyAppendedEntries() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        RaftLog log = createRaftLog( fileSystem );

        final RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        final RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        final RaftLogEntry logEntryC = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 3 ) );
        final RaftLogEntry logEntryD = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 4 ) );
        final RaftLogEntry logEntryE = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 5 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.append( logEntryC );

        log.truncate( 1 );

        log.append( logEntryD );
        log.append( logEntryE );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, myLog -> {
            assertThat( myLog.appendIndex(), is( 2L ) );
            assertThat( myLog.readLogEntry( 0 ), equalTo( logEntryA ) );
            assertThat( myLog.readLogEntry( 1 ), equalTo( logEntryD ) );
            assertThat( myLog.readLogEntry( 2 ), equalTo( logEntryE ) );
            assertThat( myLog.entryExists( 0 ), is( true ) );
            assertThat( myLog.entryExists( 1 ), is( true ) );
            assertThat( myLog.entryExists( 2 ), is( true ) );
            assertThat( myLog.entryExists( 3 ), is( false ) );
        } );
    }

    @Test
    public void shouldLogDifferentContentTypes() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        RaftLog log = createRaftLog( fileSystem );

        final RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        final RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "hejzxcjkzhxcjkxz" ) );

        log.append( logEntryA );
        log.append( logEntryB );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, myLog -> {
            assertThat( myLog.appendIndex(), is( 1L ) );
            assertThat( myLog.readLogEntry( 0 ), equalTo( logEntryA ) );
            assertThat( myLog.readLogEntry( 1 ), equalTo( logEntryB ) );
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            RaftLog log, EphemeralFileSystemAbstraction fileSystem, LogVerifier logVerifier ) throws RaftStorageException
    {
        logVerifier.verifyLog( log );
        logVerifier.verifyLog( createRaftLog( fileSystem ) );
        fileSystem.crash();
        logVerifier.verifyLog( createRaftLog( fileSystem ) );
    }

    private interface LogVerifier
    {
        void verifyLog( RaftLog log ) throws RaftStorageException;
    }
}
