/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;

public class CoreSnapshotService
{
    private static final String OPERATION_NAME = "snapshot request";

    private final CommandApplicationProcess applicationProcess;
    private final CoreState coreState;
    private final RaftLog raftLog;
    private final RaftMachine raftMachine;

    public CoreSnapshotService( CommandApplicationProcess applicationProcess, CoreState coreState, RaftLog raftLog,
            RaftMachine raftMachine )
    {
        this.applicationProcess = applicationProcess;
        this.coreState = coreState;
        this.raftLog = raftLog;
        this.raftMachine = raftMachine;
    }

    public synchronized CoreSnapshot snapshot() throws Exception
    {
        applicationProcess.pauseApplier( OPERATION_NAME );
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
            applicationProcess.resumeApplier( OPERATION_NAME );
        }
    }

    public synchronized void installSnapshot( CoreSnapshot coreSnapshot ) throws IOException
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
