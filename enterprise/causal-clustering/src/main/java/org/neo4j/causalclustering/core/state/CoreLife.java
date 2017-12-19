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

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.identity.BoundState;
import org.neo4j.causalclustering.identity.ClusterBinder;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class CoreLife implements Lifecycle
{
    private final RaftMachine raftMachine;
    private final LocalDatabase localDatabase;
    private final ClusterBinder clusterBinder;

    private final CommandApplicationProcess applicationProcess;
    private final CoreStateMachines coreStateMachines;
    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;

    public CoreLife( RaftMachine raftMachine,
            LocalDatabase localDatabase,
            ClusterBinder clusterBinder,
            CommandApplicationProcess commandApplicationProcess,
            CoreStateMachines coreStateMachines,
            LifecycleMessageHandler<?> raftMessageHandler,
            CoreSnapshotService snapshotService )
    {
        this.raftMachine = raftMachine;
        this.localDatabase = localDatabase;
        this.clusterBinder = clusterBinder;
        this.applicationProcess = commandApplicationProcess;
        this.coreStateMachines = coreStateMachines;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
    }

    @Override
    public synchronized void init() throws Throwable
    {
        localDatabase.init();
    }

    @Override
    public synchronized void start() throws Throwable
    {
        BoundState boundState = clusterBinder.bindToCluster();
        raftMessageHandler.start( boundState.clusterId() );

        if ( boundState.snapshot().isPresent() )
        {
            // this means that we bootstrapped the cluster
            CoreSnapshot snapshot = boundState.snapshot().get();
            snapshotService.installSnapshot( snapshot );
        }
        else
        {
            snapshotService.awaitState();
        }

        localDatabase.start();
        coreStateMachines.installCommitProcess( localDatabase.getCommitProcess() );
        applicationProcess.start();
        raftMachine.postRecoveryActions();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        raftMachine.stopTimers();
        raftMessageHandler.stop();
        applicationProcess.stop();
        localDatabase.stop();
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        localDatabase.shutdown();
    }
}
