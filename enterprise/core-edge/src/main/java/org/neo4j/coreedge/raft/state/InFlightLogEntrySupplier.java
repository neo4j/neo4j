package org.neo4j.coreedge.raft.state;

import java.io.IOException;

import org.neo4j.coreedge.raft.log.RaftLogCursor;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;

public class InFlightLogEntrySupplier implements AutoCloseable
{
    private final ReadableRaftLog raftLog;
    private final InFlightMap<Long, RaftLogEntry> inFlightMap;

    private RaftLogCursor cursor;
    private boolean useInFlightMap = true;

    public InFlightLogEntrySupplier( ReadableRaftLog raftLog, InFlightMap<Long,RaftLogEntry> inFlightMap )
    {
        this.raftLog = raftLog;
        this.inFlightMap = inFlightMap;
    }

    public RaftLogEntry get( long logIndex ) throws IOException
    {
        RaftLogEntry entry = null;

        if ( useInFlightMap )
        {
            entry = inFlightMap.retrieve( logIndex );
        }

        if ( entry == null )
        {
            useInFlightMap = false;
            entry = getUsingCursor( logIndex );
        }

        inFlightMap.unregister( logIndex );

        return entry;
    }

    private RaftLogEntry getUsingCursor( long logIndex ) throws IOException
    {
        if ( cursor == null )
        {
            cursor = raftLog.getEntryCursor( logIndex );
        }

        if ( cursor.next() )
        {
            assert cursor.index() == logIndex;
            return cursor.get();
        }
        else
        {
            return null;
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( cursor != null )
        {
            cursor.close();
        }
    }
}
