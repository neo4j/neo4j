/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge;

import java.io.IOException;

import org.neo4j.coreedge.core.state.snapshot.CoreStateType;
import org.neo4j.coreedge.core.replication.session.GlobalSession;
import org.neo4j.coreedge.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.core.replication.session.LocalOperationId;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshot;
import org.neo4j.coreedge.core.state.storage.StateStorage;

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
