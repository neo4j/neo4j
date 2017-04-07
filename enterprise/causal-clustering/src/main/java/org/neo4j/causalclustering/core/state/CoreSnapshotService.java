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

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;

public class CoreSnapshotService
{
    private final CommandApplicationProcess applicationProcess;
    private final CoreState coreState;
    private final RaftLog raftLog;
    private final RaftMachine raftMachine;

    public CoreSnapshotService( CommandApplicationProcess applicationProcess, CoreState coreState, RaftLog raftLog, RaftMachine raftMachine )
    {
        this.applicationProcess = applicationProcess;
        this.coreState = coreState;
        this.raftLog = raftLog;
        this.raftMachine = raftMachine;
    }

    public synchronized CoreSnapshot snapshot() throws Exception
    {
        applicationProcess.pauseApplier();
        try
        {
            long lastApplied = applicationProcess.lastApplied();

            long prevTerm = raftLog.readEntryTerm( lastApplied );
            CoreSnapshot coreSnapshot = new CoreSnapshot( lastApplied, prevTerm );

            coreState.augmentSnapshot( coreSnapshot );
            coreSnapshot.add( CoreStateType.RAFT_CORE_STATE, raftMachine.coreState() );

            return coreSnapshot;
        }
        finally
        {
            applicationProcess.resumeApplier();
        }
    }

    public synchronized void installSnapshot( CoreSnapshot coreSnapshot ) throws Exception
    {
        long snapshotPrevIndex = coreSnapshot.prevIndex();
        raftLog.skip( snapshotPrevIndex, coreSnapshot.prevTerm() );

        coreState.installSnapshot( coreSnapshot );
        raftMachine.installCoreState( coreSnapshot.get( CoreStateType.RAFT_CORE_STATE ) );
        coreState.flush( snapshotPrevIndex );

        applicationProcess.installSnapshot( coreSnapshot );
        notifyAll();
    }

    synchronized void awaitState() throws InterruptedException
    {
        while ( raftMachine.state().appendIndex() < 0 )
        {
            wait();
        }
    }
}
