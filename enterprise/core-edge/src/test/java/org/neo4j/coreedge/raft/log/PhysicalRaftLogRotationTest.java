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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Test;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

public class PhysicalRaftLogRotationTest
{
    private LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

    @After
    public void tearDown() throws Throwable
    {
        life.stop();
        life.shutdown();
    }

    private PhysicalRaftLog createRaftLog( long rotateAtSize, PhysicalLogFile.Monitor logFileMonitor )
    {
        if ( fileSystem == null )
        {
            fileSystem = new EphemeralFileSystemAbstraction();
        }
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );

        PhysicalRaftLog newRaftLog = new PhysicalRaftLog( fileSystem, directory, rotateAtSize, 100, 10,
                logFileMonitor, new DummyRaftableContentSerializer(),  () -> mock( DatabaseHealth.class ),
                NullLogProvider.getInstance() );
        life.add( newRaftLog );
        life.init();
        life.start();
        return newRaftLog;
    }

    @Test
    public void shouldRotateOnAppendWhenRotateSizeIsReached() throws Exception
    {
        // Given
        AtomicLong currentVersion = new AtomicLong();
        PhysicalLogFile.Monitor logFileMonitor =
                ( logFile, logVersion, lastTransactionId, clean ) -> currentVersion.set( logVersion );
        int rotateAtSize = 100;
        PhysicalRaftLog log = createRaftLog( rotateAtSize, logFileMonitor );

        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < rotateAtSize; i++ )
        {
            builder.append( "i" );
        }

        // When
        ReplicatedString stringThatIsMoreThan100Bytes = new ReplicatedString( builder.toString() );
        log.append( new RaftLogEntry( 0, stringThatIsMoreThan100Bytes ) );

        // Then
        assertEquals( 1, currentVersion.get() );
    }

    @Test
    public void shouldRotateOnCommitWhenRotateSizeIsReached() throws Exception
    {
        // Given
        AtomicLong currentVersion = new AtomicLong();
        PhysicalLogFile.Monitor logFileMonitor =
                ( logFile, logVersion, lastTransactionId, clean ) -> currentVersion.set( logVersion );
        int rotateAtSize = 100;
        PhysicalRaftLog log = createRaftLog( rotateAtSize, logFileMonitor );

        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < rotateAtSize - 40; i++ )
        {
            builder.append( "i" );
        }

        // When
        ReplicatedString stringThatGetsTheSizeToAlmost100Bytes = new ReplicatedString( builder.toString() );
        log.append( new RaftLogEntry( 0, stringThatGetsTheSizeToAlmost100Bytes ) );
        assertEquals( 0, currentVersion.get() );
        log.commit( 1 );

        // Then
        assertEquals( 1, currentVersion.get() );
    }

    @Test
    public void shouldRotateOnTruncateWhenRotateSizeIsReached() throws Exception
    {
        // Given
        AtomicLong currentVersion = new AtomicLong();
        PhysicalLogFile.Monitor logFileMonitor =
                ( logFile, logVersion, lastTransactionId, clean ) -> currentVersion.set( logVersion );
        int rotateAtSize = 100;
        PhysicalRaftLog log = createRaftLog( rotateAtSize, logFileMonitor );

        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < rotateAtSize - 40; i++ )
        {
            builder.append( "i" );
        }

        // When
        ReplicatedString stringThatGetsTheSizeToAlmost100Bytes = new ReplicatedString( builder.toString() );
        long indexToTruncate = log.append( new RaftLogEntry( 0, stringThatGetsTheSizeToAlmost100Bytes ) );
        assertEquals( 0, currentVersion.get() );
        log.truncate( indexToTruncate );

        // Then
        assertEquals( 1, currentVersion.get() );
    }

    @Test
    public void shouldBeAbleToRecoverToLatestStateAfterRotation() throws Throwable
    {
        int rotateAtSize = 100;
        PhysicalRaftLog log = createRaftLog( rotateAtSize, new PhysicalLogFile.Monitor.Adapter() );

        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < rotateAtSize - 40; i++ )
        {
            builder.append( "i" );
        }

        ReplicatedString stringThatGetsTheSizeToAlmost100Bytes = new ReplicatedString( builder.toString() );
        int term = 0;
        long indexToCommit = log.append( new RaftLogEntry( term, stringThatGetsTheSizeToAlmost100Bytes ) );
        log.commit( indexToCommit );
        indexToCommit = log.append( new RaftLogEntry( term, ReplicatedInteger.valueOf( 1 ) ) );
        log.commit( indexToCommit );

        // When
        life.remove( log );
        log = createRaftLog( rotateAtSize, new PhysicalLogFile.Monitor.Adapter() );

        // Then
        assertEquals( indexToCommit, log.commitIndex() );
        assertEquals( indexToCommit, log.appendIndex() );
        assertTrue( log.entryExists( indexToCommit ) );
        assertEquals( term, log.readEntryTerm( indexToCommit ) );
    }


    @Test
    public void shouldBeAbleToRecoverToLatestStateAfterRotationWhenEarlierFilesArePruned() throws Throwable
    {
        int rotateAtSize = 100;
        PhysicalRaftLog log = createRaftLog( rotateAtSize, new PhysicalLogFile.Monitor.Adapter() );

        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < rotateAtSize - 40; i++ )
        {
            builder.append( "i" );
        }

        ReplicatedString stringThatGetsTheSizeToAlmost100Bytes = new ReplicatedString( builder.toString() );
        int term = 0;
        long indexToCommit = log.append( new RaftLogEntry( term, stringThatGetsTheSizeToAlmost100Bytes ) );
        log.commit( indexToCommit );
        indexToCommit = log.append( new RaftLogEntry( term, ReplicatedInteger.valueOf( 1 ) ) );
        log.commit( indexToCommit );

        // When
        life.remove( log );

        fileSystem.deleteFile( new File( new File("raft-log"), "raft.log.0" ) ); // hackish, decorate it my great Tech Liege

        log = createRaftLog( rotateAtSize, new PhysicalLogFile.Monitor.Adapter() );

        // Then
        assertEquals( indexToCommit, log.commitIndex() );
        assertEquals( indexToCommit, log.appendIndex() );
        assertTrue( log.entryExists( indexToCommit ) );
        assertEquals( term, log.readEntryTerm( indexToCommit ) );
    }
}
