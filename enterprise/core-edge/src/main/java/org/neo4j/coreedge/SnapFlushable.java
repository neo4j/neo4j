package org.neo4j.coreedge;

import java.io.IOException;

import org.neo4j.coreedge.raft.state.CoreSnapshot;

public interface SnapFlushable
{
    void flush() throws IOException;

    void addSnapshots( CoreSnapshot coreSnapshot );

    long getLastAppliedIndex();

    void installSnapshots( CoreSnapshot coreSnapshot );
}
