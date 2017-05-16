/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import java.io.IOException;

import org.neo4j.causalclustering.SessionTracker;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;
import org.neo4j.causalclustering.core.state.storage.StateStorage;

import static java.lang.Long.max;

public class CoreState
{
    private CoreStateMachines coreStateMachines;
    private final SessionTracker sessionTracker;
    private final StateStorage<Long> lastFlushedStorage;

    public CoreState( CoreStateMachines coreStateMachines, SessionTracker sessionTracker,
            StateStorage<Long> lastFlushedStorage )
    {
        this.coreStateMachines = coreStateMachines;
        this.sessionTracker = sessionTracker;
        this.lastFlushedStorage = lastFlushedStorage;
    }

    synchronized void augmentSnapshot( CoreSnapshot coreSnapshot )
    {
        coreStateMachines.addSnapshots( coreSnapshot );
        coreSnapshot.add( CoreStateType.SESSION_TRACKER, sessionTracker.snapshot() );
    }

    synchronized void installSnapshot( CoreSnapshot coreSnapshot ) throws IOException
    {
        coreStateMachines.installSnapshots( coreSnapshot );
        sessionTracker.installSnapshot( coreSnapshot.get( CoreStateType.SESSION_TRACKER ) );
    }

    synchronized void flush( long lastApplied ) throws IOException
    {
        coreStateMachines.flush();
        sessionTracker.flush();
        lastFlushedStorage.persistStoreData( lastApplied );
    }

    public CommandDispatcher commandDispatcher()
    {
        return coreStateMachines.commandDispatcher();
    }

    long getLastAppliedIndex()
    {
        return max( coreStateMachines.getLastAppliedIndex(), sessionTracker.getLastAppliedIndex() );
    }

    long getLastFlushed()
    {
        return lastFlushedStorage.getInitialState();
    }
}
