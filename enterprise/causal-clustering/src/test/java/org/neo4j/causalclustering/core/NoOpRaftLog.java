package org.neo4j.causalclustering.core;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

class NoOpRaftLog implements RaftLog
{
    AtomicLong counter = new AtomicLong( 0 );

    @Override
    public long append( RaftLogEntry... entry ) throws IOException
    {
        return counter.addAndGet( entry.length );
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        return safeIndex;
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        return index;
    }

    @Override
    public long appendIndex()
    {
        return counter.get();
    }

    @Override
    public long prevIndex()
    {
        return counter.get();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        return -1; // TODO could be wrong as -1 is `doesn't exists`
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        throw new RuntimeException( "Unimplemented" );
    }
}
