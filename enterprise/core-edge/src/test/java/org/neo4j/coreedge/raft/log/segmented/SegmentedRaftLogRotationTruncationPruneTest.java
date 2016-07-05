package org.neo4j.coreedge.raft.log.segmented;

import java.io.File;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;

public class SegmentedRaftLogRotationTruncationPruneTest
{
    @Test
    public void shouldPruneAwaySingleEntriesIfRotationHappenedEveryEntry() throws Exception
    {
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
        FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();

        File directory = new File( SEGMENTED_LOG_DIRECTORY_NAME );
        fileSystem.mkdir( directory );

        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( fileSystem, directory, 1,
                new DummyRaftableContentSerializer(),
                NullLogProvider.getInstance(), "1 entries", 8, new FakeClock() );

        newRaftLog.start();
        return newRaftLog;
    }
}