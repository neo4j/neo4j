package org.neo4j.coreedge.core.consensus.log.segmented;

import org.junit.Test;

import org.neo4j.coreedge.core.consensus.log.InMemoryRaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static org.neo4j.coreedge.core.consensus.ReplicatedInteger.valueOf;

public class CachedSuffixRaftLogTest
{
    @Test
    public void shouldReadEntriesAtTheEndOfTheLogFromTheCacheLog() throws Exception
    {
        // given
        RaftLog cacheLog = spy( new InMemoryRaftLog() );
        RaftLog fullLog = spy( new InMemoryRaftLog() );
        CachedSuffixRaftLog cachedSuffixRaftLog = new CachedSuffixRaftLog( fullLog, cacheLog );

        cachedSuffixRaftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        cachedSuffixRaftLog.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        cacheLog.prune( 0 );

        // when
        cachedSuffixRaftLog.getEntryCursor( 1 );

        // then
        verify( cacheLog ).getEntryCursor( 1 );
        verify( fullLog, never() ).getEntryCursor( 1 );
    }

    @Test
    public void shouldReadEntriesAtTheStartOfTheLogFromTheFullLog() throws Exception
    {
        // given
        RaftLog cacheLog = spy( new InMemoryRaftLog() );
        RaftLog fullLog = spy( new InMemoryRaftLog() );
        CachedSuffixRaftLog cachedSuffixRaftLog = new CachedSuffixRaftLog( fullLog, cacheLog );

        cachedSuffixRaftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        cachedSuffixRaftLog.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        cacheLog.prune( 0 );

        // when
        cachedSuffixRaftLog.getEntryCursor( 0 );

        // then
        verify( fullLog ).getEntryCursor( 0 );
        verify( cacheLog, never() ).getEntryCursor( 0 );
    }
}