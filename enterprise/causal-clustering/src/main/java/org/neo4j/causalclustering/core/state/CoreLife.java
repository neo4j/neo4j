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
package org.neo4j.causalclustering.core.state;

import java.util.Optional;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloaderService;
import org.neo4j.causalclustering.identity.BoundState;
import org.neo4j.causalclustering.identity.ClusterBinder;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.kernel.impl.util.DebugUtil;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.scheduler.JobScheduler;

public class CoreLife extends SafeLifecycle
{
    private final RaftMachine raftMachine;
    private final LocalDatabase localDatabase;
    private final ClusterBinder clusterBinder;

    private final CommandApplicationProcess applicationProcess;
    private final CoreStateMachines coreStateMachines;
    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreStateDownloaderService downloadService;

    public CoreLife( RaftMachine raftMachine, LocalDatabase localDatabase, ClusterBinder clusterBinder, CommandApplicationProcess commandApplicationProcess,
            CoreStateMachines coreStateMachines, LifecycleMessageHandler<?> raftMessageHandler, CoreSnapshotService snapshotService,
            CoreStateDownloaderService downloadService )
    {
        this.raftMachine = raftMachine;
        this.localDatabase = localDatabase;
        this.clusterBinder = clusterBinder;
        this.applicationProcess = commandApplicationProcess;
        this.coreStateMachines = coreStateMachines;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
    }

    @Override
    public void init0() throws Throwable
    {
        localDatabase.init();
    }

    @Override
    public void start0() throws Throwable
    {
        BoundState boundState = clusterBinder.bindToCluster();
        raftMessageHandler.start( boundState.clusterId() );

        boolean startedByDownloader = false;
        if ( boundState.snapshot().isPresent() )
        {
            // this means that we bootstrapped the cluster
            CoreSnapshot snapshot = boundState.snapshot().get();
            snapshotService.installSnapshot( snapshot );
        }
        else
        {
            snapshotService.awaitState();
            Optional<JobScheduler.JobHandle> downloadJob = downloadService.downloadJob();
            if ( downloadJob.isPresent() )
            {
                downloadJob.get().waitTermination();
                startedByDownloader = true;
            }
        }

        if ( !startedByDownloader )
        {
            localDatabase.start();
            coreStateMachines.installCommitProcess( localDatabase.getCommitProcess() );
        }
        applicationProcess.start();
        raftMachine.postRecoveryActions();
    }

    @Override
    public void stop0() throws Throwable
    {
        raftMachine.stopTimers();
        raftMessageHandler.stop();
        applicationProcess.stop();
        localDatabase.stop();
    }

    @Override
    public void shutdown0() throws Throwable
    {
        localDatabase.shutdown();
    }
}
