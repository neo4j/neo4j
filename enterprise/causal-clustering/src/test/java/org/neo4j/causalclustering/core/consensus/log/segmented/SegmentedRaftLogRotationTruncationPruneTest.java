/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.After;
import org.junit.Test;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogRotationTruncationPruneTest
{

    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();

    @After
    public void tearDown() throws Exception
    {
        fileSystem.close();
    }

    @Test
    public void shouldPruneAwaySingleEntriesIfRotationHappenedEveryEntry() throws Exception
    {
        /**
         * If you have a raft log which rotates after every append, therefore having a single entry in every segment,
         * we assert that every sequential prune attempt will result in the prevIndex incrementing by one.
         */

        // given
        RaftLog log = createRaftLog();

        long term = 0;
        for ( int i = 0; i < 10; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }

        assertEquals( -1, log.prevIndex() );
        for ( int i = 0; i < 9; i++ )
        {
            log.prune( i );
            assertEquals( i, log.prevIndex() );
        }
        log.prune( 9 );
        assertEquals( 8, log.prevIndex() );
    }

    @Test
    public void shouldPruneAwaySingleEntriesAfterTruncationIfRotationHappenedEveryEntry() throws Exception
    {
        /**
         * Given a log with many single-entry segments, a series of truncations at decending values followed by
         * pruning at more previous segments will maintain the correct prevIndex for the log.
         *
         * Initial Scenario:    [0][1][2][3][4][5][6][7][8][9]              prevIndex = 0
         * Truncate segment 9 : [0][1][2][3][4][5][6][7][8]   (9)           prevIndex = 0
         * Truncate segment 8 : [0][1][2][3][4][5][6][7]      (8)(9)        prevIndex = 0
         * Truncate segment 7 : [0][1][2][3][4][5][6]         (7)(8)(9)     prevIndex = 0
         * Prune segment 0    :    [1][2][3][4][5][6]         (7)(8)(9)     prevIndex = 1
         * Prune segment 1    :       [2][3][4][5][6]         (7)(8)(9)     prevIndex = 2
         * Prune segment 2    :          [3][4][5][6]         (7)(8)(9)     prevIndex = 3
         * Prune segment 3    :             [4][5][6]         (7)(8)(9)     prevIndex = 4
         * Prune segment 4    :                [5][6]         (7)(8)(9)     prevIndex = 5
         * Prune segment 5    :                [5][6]         (7)(8)(9)     prevIndex = 5
         * Prune segment 6    :                [5][6]         (7)(8)(9)     prevIndex = 5
         * Etc...
         *
         * The prevIndex should not become corrupt and become greater than 5 in this example.
         */

        // given
        RaftLog log = createRaftLog();

        long term = 0;
        for ( int i = 0; i < 10; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }

        log.truncate( 9 );
        log.truncate( 8 );
        log.truncate( 7 );

        assertEquals( -1, log.prevIndex() );
        for ( int i = 0; i <= 5; i++ )
        {
            log.prune( i );
            assertEquals( i, log.prevIndex() );
        }
        for ( int i = 5; i < 10; i++ )
        {
            log.prune( i );
            assertEquals( 5, log.prevIndex() );
        }
    }

    private RaftLog createRaftLog() throws Exception
    {
        File directory = new File( RAFT_LOG_DIRECTORY_NAME );
        FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        fileSystem.mkdir( directory );

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( "1 entries", logProvider ).newInstance();
        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( fileSystem, directory, 1,
                new DummyRaftableContentSerializer(), logProvider, 8, Clocks.fakeClock(), new OnDemandJobScheduler(),
                pruningStrategy );

        newRaftLog.start();
        return newRaftLog;
    }
}
