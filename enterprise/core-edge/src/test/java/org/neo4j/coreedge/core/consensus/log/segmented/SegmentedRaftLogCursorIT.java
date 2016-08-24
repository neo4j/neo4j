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
package org.neo4j.coreedge.core.consensus.log.segmented;

import org.junit.After;
import org.junit.Test;

import java.io.File;

import org.neo4j.coreedge.core.consensus.ReplicatedInteger;
import org.neo4j.coreedge.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.core.consensus.log.RaftLogCursor;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.raft_log_pruning_strategy;
import static org.neo4j.coreedge.core.consensus.log.RaftLog.PHYSICAL_LOG_DIRECTORY_NAME;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogCursorIT
{
    private LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

    @After
    public void tearDown() throws Throwable
    {
        life.stop();
        life.shutdown();
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize, String pruneStrategy )
    {
        if ( fileSystem == null )
        {
            fileSystem = new EphemeralFileSystemAbstraction();
        }

        File directory = new File( PHYSICAL_LOG_DIRECTORY_NAME );
        fileSystem.mkdir( directory );

        SegmentedRaftLog newRaftLog =
                new SegmentedRaftLog( fileSystem, directory, rotateAtSize, new DummyRaftableContentSerializer(),
                        getInstance(), pruneStrategy, 8, Clocks.systemClock(),
                        new OnDemandJobScheduler() );

        life.add( newRaftLog );
        life.init();
        life.start();

        return newRaftLog;
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        return createRaftLog( rotateAtSize, raft_log_pruning_strategy.getDefaultValue() );
    }

    @Test
    public void shouldReturnFalseOnCursorForEntryThatDoesntExist() throws Exception
    {
        //given
        SegmentedRaftLog segmentedRaftLog = createRaftLog( 1 );
        segmentedRaftLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        segmentedRaftLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 2 ) ) );
        long lastIndex = segmentedRaftLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 3 ) ) );

        //when
        boolean next;
        try ( RaftLogCursor entryCursor = segmentedRaftLog.getEntryCursor( lastIndex + 1 ) )
        {
            next = entryCursor.next();
        }

        //then
        assertFalse( next );
    }

    @Test
    public void shouldReturnTrueOnEntryThatExists() throws Exception
    {
        //given
        SegmentedRaftLog segmentedRaftLog = createRaftLog( 1 );
        segmentedRaftLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        segmentedRaftLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 2 ) ) );
        long lastIndex = segmentedRaftLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 3 ) ) );

        //when
        boolean next;
        try ( RaftLogCursor entryCursor = segmentedRaftLog.getEntryCursor( lastIndex ) )
        {
            next = entryCursor.next();
        }

        //then
        assertTrue( next );
    }

    @Test
    public void shouldReturnFalseOnCursorForEntryThatWasPruned() throws Exception
    {
        //given
        SegmentedRaftLog segmentedRaftLog = createRaftLog( 1, "keep_none" );
        long firstIndex = segmentedRaftLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        segmentedRaftLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 2 ) ) );
        long lastIndex = segmentedRaftLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 3 ) ) );

        //when
        segmentedRaftLog.prune( firstIndex );
        RaftLogCursor entryCursor = segmentedRaftLog.getEntryCursor( firstIndex );
        boolean next = entryCursor.next();

        //then
        assertFalse( next );
    }
}
