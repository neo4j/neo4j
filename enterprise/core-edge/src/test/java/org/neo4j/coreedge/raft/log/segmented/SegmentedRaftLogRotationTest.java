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
package org.neo4j.coreedge.raft.log.segmented;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_log_pruning_strategy;

public class SegmentedRaftLogRotationTest
{
    private LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

    @After
    public void tearDown() throws Throwable
    {
        life.stop();
        life.shutdown();
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        if ( fileSystem == null )
        {
            fileSystem = new EphemeralFileSystemAbstraction();
        }
        File directory = new File( SEGMENTED_LOG_DIRECTORY_NAME );
        fileSystem.mkdir( directory );

        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( fileSystem, directory, rotateAtSize,
                new DummyRaftableContentSerializer(),
                NullLogProvider.getInstance(), 1000, raft_log_pruning_strategy.getDefaultValue() );
        life.add( newRaftLog );
        life.init();
        life.start();
        return newRaftLog;
    }

    @Ignore( "This test is stupid - rewrite" )
    public void shouldRotateOnAppendWhenRotateSizeIsReached() throws Exception
    {
        // Given
        AtomicLong currentVersion = new AtomicLong();
        int rotateAtSize = 100;
        SegmentedRaftLog log = createRaftLog( rotateAtSize );

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
    public void shouldBeAbleToRecoverToLatestStateAfterRotation() throws Throwable
    {
        int rotateAtSize = 100;
        SegmentedRaftLog log = createRaftLog( rotateAtSize );

        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < rotateAtSize - 40; i++ )
        {
            builder.append( "i" );
        }

        ReplicatedString stringThatGetsTheSizeToAlmost100Bytes = new ReplicatedString( builder.toString() );
        int term = 0;
        log.append( new RaftLogEntry( term, stringThatGetsTheSizeToAlmost100Bytes ) );
        long indexToRestoreTo = log.append( new RaftLogEntry( term, ReplicatedInteger.valueOf( 1 ) ) );

        // When
        life.remove( log );
        log = createRaftLog( rotateAtSize );

        // Then
        assertEquals( indexToRestoreTo, log.appendIndex() );
        assertEquals( term, log.readEntryTerm( indexToRestoreTo ) );
    }
}
