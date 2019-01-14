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
