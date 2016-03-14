package org.neo4j.coreedge.raft.log;

import java.io.IOException;

import org.neo4j.cursor.RawCursor;

public interface RaftLogCursor extends RawCursor<RaftLogEntry,Exception>
{
    @Override
    boolean next() throws IOException, RaftLogCompactedException;

    @Override
    void close() throws IOException;

    static RaftLogCursor empty()
    {
        return new RaftLogCursor()
        {
            @Override
            public boolean next() throws IOException
            {
                return false;
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public RaftLogEntry get()
            {
                throw new IllegalStateException();
            }
        };
    }
}
