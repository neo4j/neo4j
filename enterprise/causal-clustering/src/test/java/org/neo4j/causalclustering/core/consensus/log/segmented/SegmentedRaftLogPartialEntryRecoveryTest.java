/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.TokenType;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;
import static org.neo4j.logging.NullLogProvider.getInstance;

/**
 * This class tests that partially written entries at the end of the last raft log file (also known as Segment)
 * do not cause a problem. This is guaranteed by rotating after recovery and making sure that half written
 * entries at the end do not stop recovery from proceeding.
 */
public class SegmentedRaftLogPartialEntryRecoveryTest
{
    @Rule
    public final DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory( fsRule.get() );
    private File logDirectory;

    @Rule
    public RuleChain chain = RuleChain.outerRule( fsRule ).around( dir );

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        File directory = new File( RAFT_LOG_DIRECTORY_NAME );
        logDirectory = dir.directory( directory.getName() );

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( "100 entries", logProvider ).newInstance();
        return new SegmentedRaftLog( fsRule.get(), logDirectory, rotateAtSize, new CoreReplicatedContentMarshal(),
                logProvider, 8, Clocks.fakeClock(), new OnDemandJobScheduler(), pruningStrategy );
    }

    private RecoveryProtocol createRecoveryProtocol()
    {
        FileNames fileNames = new FileNames( logDirectory );
        return new RecoveryProtocol( fsRule.get(), fileNames,
                new ReaderPool( 8, getInstance(), fileNames, fsRule.get(), Clocks.fakeClock() ),
                new CoreReplicatedContentMarshal(), getInstance() );
    }

    @Test
    public void incompleteEntriesAtTheEndShouldNotCauseFailures() throws Throwable
    {
        // Given
        // we use a RaftLog to create a raft log file and then we will start chopping bits off from the end
        SegmentedRaftLog raftLog = createRaftLog( 100_000 );

        raftLog.start();

        // Add a bunch of entries, preferably one of each available kind.
        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 4, new ReplicatedIdAllocationRequest( new MemberId( UUID.randomUUID() ),
                IdType.RELATIONSHIP, 1, 1024 ) ) );
        raftLog.append( new RaftLogEntry( 4, new ReplicatedIdAllocationRequest( new MemberId( UUID.randomUUID() ),
                IdType.RELATIONSHIP, 1025, 1024 ) ) );
        raftLog.append( new RaftLogEntry( 4, new ReplicatedLockTokenRequest( new MemberId( UUID.randomUUID() ), 1 ) ) );
        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 5, new ReplicatedTokenRequest( TokenType.LABEL,
                "labelToken", new byte[]{ 1, 2, 3 } ) ) );
        raftLog.append( new RaftLogEntry( 5,
                new ReplicatedTransaction( new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9 , 10 } ) ) );

        raftLog.stop();

        // We use a temporary RecoveryProtocol to get the file to chop
        RecoveryProtocol recovery = createRecoveryProtocol();
        State recoveryState = recovery.run();
        String logFilename = recoveryState.segments.last().getFilename();
        recoveryState.segments.close();
        File logFile = new File( logDirectory, logFilename );

        // When
        // We remove any number of bytes from the end (up to but not including the header) and try to recover
        // Then
        // No exceptions should be thrown
        truncateAndRecover( logFile, SegmentHeader.SIZE );
    }

    @Test
    public void incompleteHeaderOfLastOfMoreThanOneLogFilesShouldNotCauseFailure() throws Throwable
    {
        // Given
        // we use a RaftLog to create two log files, in order to chop the header of the second
        SegmentedRaftLog raftLog = createRaftLog(1 );

        raftLog.start();

        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) ); // will cause rotation

        raftLog.stop();

        // We use a temporary RecoveryProtocol to get the file to chop
        RecoveryProtocol recovery = createRecoveryProtocol();
        State recoveryState = recovery.run();
        String logFilename = recoveryState.segments.last().getFilename();
        recoveryState.segments.close();
        File logFile = new File( logDirectory, logFilename );

        // When
        // We remove any number of bytes from the end of the second file and try to recover
        // Then
        // No exceptions should be thrown
        truncateAndRecover( logFile, 0 );
    }

    @Test
    public void shouldNotAppendAtTheEndOfLogFileWithIncompleteEntries() throws Throwable
    {
        // Given
        // we use a RaftLog to create a raft log file and then we will chop some bits off the end
        SegmentedRaftLog raftLog = createRaftLog(100_000 );

        raftLog.start();

        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );

        raftLog.stop();

        // We use a temporary RecoveryProtocol to get the file to chop
        RecoveryProtocol recovery = createRecoveryProtocol();
        State recoveryState = recovery.run();
        String logFilename = recoveryState.segments.last().getFilename();
        recoveryState.segments.close();
        File logFile = new File( logDirectory, logFilename );
        StoreChannel lastFile = fsRule.get().open( logFile, OpenMode.READ_WRITE );
        long currentSize = lastFile.size();
        lastFile.close();

        // When
        // We induce an incomplete entry at the end of the last file
        lastFile = fsRule.get().open( logFile, OpenMode.READ_WRITE );
        lastFile.truncate( currentSize - 1 );
        lastFile.close();

        // We start the raft log again, on the previous log file with truncated last entry.
        raftLog = createRaftLog( 100_000 );

        //  Recovery will run here
        raftLog.start();

        // Append an entry
        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );

        // Then
        // The log should report as containing only the last entry we've appended
        try ( RaftLogCursor entryCursor = raftLog.getEntryCursor( 0 ) )
        {
            // There should be exactly one entry, of type NewLeaderBarrier
            assertTrue( entryCursor.next() );
            RaftLogEntry raftLogEntry = entryCursor.get();
            assertEquals( NewLeaderBarrier.class, raftLogEntry.content().getClass() );
            assertFalse( entryCursor.next() );
        }
        raftLog.stop();
    }

    /**
     * Truncates and recovers the log file provided, one byte at a time until it reaches the header.
     * The reason the header is not truncated (and instead has its own test) is that if the log consists of
     * only one file (Segment) and the header is incomplete, that is correctly an exceptional circumstance and
     * is tested elsewhere.
     */
    private void truncateAndRecover( File logFile, long truncateDownToSize )
            throws IOException, DamagedLogStorageException, DisposedException
    {
        StoreChannel lastFile = fsRule.get().open( logFile, OpenMode.READ_WRITE );
        long currentSize = lastFile.size();
        lastFile.close();
        RecoveryProtocol recovery;
        while ( currentSize-- > truncateDownToSize )
        {
            lastFile = fsRule.get().open( logFile, OpenMode.READ_WRITE );
            lastFile.truncate( currentSize );
            lastFile.close();
            recovery = createRecoveryProtocol();
            State state = recovery.run();
            state.segments.close();
        }
    }
}
