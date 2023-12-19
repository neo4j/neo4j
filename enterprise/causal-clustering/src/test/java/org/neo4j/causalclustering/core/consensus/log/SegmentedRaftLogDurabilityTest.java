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
package org.neo4j.causalclustering.core.consensus.log;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import org.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.Clocks;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;
import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.hasNoContent;
import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogDurabilityTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private final RaftLogFactory logFactory = fileSystem ->
    {
        File directory = new File( RAFT_LOG_DIRECTORY_NAME );
        fileSystem.mkdir( directory );

        long rotateAtSizeBytes = 128;
        int readerPoolSize = 8;

        NullLogProvider logProvider = getInstance();
        SegmentedRaftLog log =
                new SegmentedRaftLog( fileSystem, directory, rotateAtSizeBytes, new DummyRaftableContentSerializer(),
                        logProvider, readerPoolSize, Clocks.fakeClock(), new OnDemandJobScheduler(),
                        new CoreLogPruningStrategyFactory( "1 size", logProvider ).newInstance() );
        log.start();

        return log;
    };

    @Test
    public void shouldAppendDataAndNotCommitImmediately() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        final RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog ->
        {
            assertThat( myLog.appendIndex(), is( 0L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntry ) );
        } );
    }

    @Test
    public void shouldAppendAndCommit() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(),
                myLog -> assertThat( myLog.appendIndex(), is( 0L ) ) );
    }

    @Test
    public void shouldAppendAfterReloadingFromFileSystem() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntryA );

        fsRule.get().crash();
        log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );
        assertThat( readLogEntry( log, 0 ), is( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), is( logEntryB ) );
    }

    @Test
    public void shouldTruncatePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.truncate( 1 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(),
                myLog -> assertThat( myLog.appendIndex(), is( 0L ) ) );
    }

    @Test
    public void shouldReplacePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

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

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog ->
        {
            assertThat( myLog.appendIndex(), is( 2L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntryA ) );
            assertThat( readLogEntry( myLog, 1 ), equalTo( logEntryD ) );
            assertThat( readLogEntry( myLog, 2 ), equalTo( logEntryE ) );
        } );
    }

    @Test
    public void shouldLogDifferentContentTypes() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        final RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        final RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "hejzxcjkzhxcjkxz" ) );

        log.append( logEntryA );
        log.append( logEntryB );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog ->
        {
            assertThat( myLog.appendIndex(), is( 1L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntryA ) );
            assertThat( readLogEntry( myLog, 1 ), equalTo( logEntryB ) );
        } );
    }

    @Test
    public void shouldRecoverAfterEventuallyPruning() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        long term = 0L;

        long safeIndex;
        long prunedIndex = -1;
        int val = 0;

        // this loop should eventually be able to prune something
        while ( prunedIndex == -1 )
        {
            for ( int i = 0; i < 100; i++ )
            {
                log.append( new RaftLogEntry( term, valueOf( val++ ) ) );
            }
            safeIndex = log.appendIndex() - 50;
            // when
            prunedIndex = log.prune( safeIndex );
        }

        final long finalAppendIndex = log.appendIndex();
        final long finalPrunedIndex = prunedIndex;
        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog ->
        {
            assertThat( log, hasNoContent( 0 ) );
            assertThat( log, hasNoContent( finalPrunedIndex ) );
            assertThat( myLog.prevIndex(), equalTo( finalPrunedIndex ) );
            assertThat( myLog.appendIndex(), is( finalAppendIndex ) );
            assertThat( readLogEntry( myLog, finalPrunedIndex + 1 ).content(), equalTo( valueOf( (int) finalPrunedIndex + 1 ) ) );
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            RaftLog log, EphemeralFileSystemAbstraction fileSystem, LogVerifier logVerifier ) throws Exception
    {
        logVerifier.verifyLog( log );
        logVerifier.verifyLog( logFactory.createBasedOn( fsRule.get() ) );
        fileSystem.crash();
        logVerifier.verifyLog( logFactory.createBasedOn( fsRule.get() ) );
    }

    private interface RaftLogFactory
    {
        RaftLog createBasedOn( FileSystemAbstraction fileSystem ) throws Exception;
    }

    private interface LogVerifier
    {
        void verifyLog( RaftLog log ) throws IOException;
    }
}
