/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering;

import java.io.IOException;

import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.storage.StateStorage;

public class SessionTracker
{
    private final StateStorage<GlobalSessionTrackerState> sessionTrackerStorage;
    private GlobalSessionTrackerState sessionState;

    public SessionTracker( StateStorage<GlobalSessionTrackerState> sessionTrackerStorage )
    {
        this.sessionTrackerStorage = sessionTrackerStorage;
    }

    public void start()
    {
        if ( sessionState == null )
        {
            sessionState = sessionTrackerStorage.getInitialState();
        }
    }

    public long getLastAppliedIndex()
    {
        return sessionState.logIndex();
    }

    public void flush() throws IOException
    {
        sessionTrackerStorage.persistStoreData( sessionState );
    }

    public GlobalSessionTrackerState snapshot()
    {
        return sessionState.newInstance();
    }

    public void installSnapshot( GlobalSessionTrackerState sessionState )
    {
        this.sessionState = sessionState;
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
