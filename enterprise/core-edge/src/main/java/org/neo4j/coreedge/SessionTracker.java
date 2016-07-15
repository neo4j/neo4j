package org.neo4j.coreedge;

import java.io.IOException;

import org.neo4j.coreedge.catchup.storecopy.core.CoreStateType;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.state.CoreSnapshot;
import org.neo4j.coreedge.raft.state.StateStorage;

public class SessionTracker implements SnapFlushable
{
    private final StateStorage<GlobalSessionTrackerState> sessionTrackerStorage;
    private GlobalSessionTrackerState sessionState = new GlobalSessionTrackerState();

    public SessionTracker( StateStorage<GlobalSessionTrackerState> sessionTrackerStorage )
    {
        this.sessionTrackerStorage = sessionTrackerStorage;
    }

    public void start()
    {
        sessionState = sessionTrackerStorage.getInitialState();
    }

    @Override
    public long getLastAppliedIndex()
    {
        return sessionState.logIndex();
    }

    @Override
    public void flush() throws IOException
    {
        sessionTrackerStorage.persistStoreData( sessionState );
    }

    @Override
    public void addSnapshots( CoreSnapshot coreSnapshot )
    {
        coreSnapshot.add( CoreStateType.SESSION_TRACKER, sessionState.newInstance() );
    }

    @Override
    public void installSnapshots( CoreSnapshot coreSnapshot )
    {
        sessionState = coreSnapshot.get( CoreStateType.SESSION_TRACKER );
    }

    public boolean validateOperation( GlobalSession globalSession, LocalOperationId localOperationId )
    {
        return sessionState.validateOperation( globalSession, localOperationId );
    }

    public void update( GlobalSession globalSession, LocalOperationId localOperationId, long logIndex )
    {
        sessionState.update( globalSession, localOperationId, logIndex );
    }

    public GlobalSessionTrackerState newInstance()
    {
        return sessionState.newInstance();
    }
}
