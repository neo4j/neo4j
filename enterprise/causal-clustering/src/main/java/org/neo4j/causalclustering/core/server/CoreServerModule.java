/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.server;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.catchup.CheckpointerSupplier;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.IdentityModule;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.pruning.PruningScheduler;
import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiter;
import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiterLifecycle;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreLife;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.CoreState;
import org.neo4j.causalclustering.core.state.LongIndexMarshal;
import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloader;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloaderService;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CoreServerModule
{
    public static final String CLUSTER_ID_NAME = "cluster-id";
    public static final String LAST_FLUSHED_NAME = "last-flushed";

    public final MembershipWaiterLifecycle membershipWaiterLifecycle;
    private final CatchupServer catchupServer;
    private final IdentityModule identityModule;
    private final CoreStateMachinesModule coreStateMachinesModule;
    private final ConsensusModule consensusModule;
    private final ClusteringModule clusteringModule;
    private final LocalDatabase localDatabase;
    private final Supplier<DatabaseHealth> dbHealthSupplier;
    private final CommandApplicationProcess commandApplicationProcess;
    private final CoreSnapshotService snapshotService;
    private final CoreStateDownloaderService downloadService;
    private final Config config;
    private final JobScheduler jobScheduler;
    private final LogProvider logProvider;
    private final PlatformModule platformModule;
    private final SslPolicy sslPolicy;

    public CoreServerModule( IdentityModule identityModule, final PlatformModule platformModule, ConsensusModule consensusModule,
            CoreStateMachinesModule coreStateMachinesModule, ClusteringModule clusteringModule, ReplicationModule replicationModule,
            LocalDatabase localDatabase, Supplier<DatabaseHealth> dbHealthSupplier, SslPolicy sslPolicy, File clusterStateDirectory )
    {
        this.identityModule = identityModule;
        this.coreStateMachinesModule = coreStateMachinesModule;
        this.consensusModule = consensusModule;
        this.clusteringModule = clusteringModule;
        this.localDatabase = localDatabase;
        this.dbHealthSupplier = dbHealthSupplier;
        this.platformModule = platformModule;
        this.sslPolicy = sslPolicy;

        this.config = platformModule.config;
        this.jobScheduler = platformModule.jobScheduler;

        final Dependencies dependencies = platformModule.dependencies;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;

        this.logProvider = logging.getInternalLogProvider();
        LogProvider userLogProvider = logging.getUserLogProvider();

        LifeSupport servicesToStopOnStoreCopy = new LifeSupport();

        if ( platformModule.config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            platformModule.dataSourceManager.addListener( new DataSourceManager.Listener()
            {
                @Override
                public void registered( NeoStoreDataSource dataSource )
                {
                    servicesToStopOnStoreCopy.add( pickBackupExtension( dataSource ) );
                }

                @Override
                public void unregistered( NeoStoreDataSource dataSource )
                {
                    servicesToStopOnStoreCopy.remove( pickBackupExtension( dataSource ) );
                }

                private OnlineBackupKernelExtension pickBackupExtension( NeoStoreDataSource dataSource )
                {
                    return dataSource.getDependencyResolver().resolveDependency( OnlineBackupKernelExtension.class );
                }
            } );
        }

        StateStorage<Long> lastFlushedStorage = platformModule.life.add(
                new DurableStateStorage<>( platformModule.fileSystem, clusterStateDirectory, LAST_FLUSHED_NAME, new LongIndexMarshal(),
                        platformModule.config.get( CausalClusteringSettings.last_flushed_state_size ), logProvider ) );

        consensusModule.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStorage::getInitialState );

        CoreState coreState = new CoreState( coreStateMachinesModule.coreStateMachines,
                replicationModule.getSessionTracker(), lastFlushedStorage );

        final Supplier<DatabaseHealth> databaseHealthSupplier = platformModule.dependencies.provideDependency( DatabaseHealth.class );
        commandApplicationProcess = new CommandApplicationProcess(
                consensusModule.raftLog(),
                platformModule.config.get( CausalClusteringSettings.state_machine_apply_max_batch_size ),
                platformModule.config.get( CausalClusteringSettings.state_machine_flush_window_size ),
                databaseHealthSupplier,
                logProvider,
                replicationModule.getProgressTracker(),
                replicationModule.getSessionTracker(),
                coreState,
                consensusModule.inFlightCache(),
                platformModule.monitors );
        platformModule.dependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        this.snapshotService = new CoreSnapshotService( commandApplicationProcess, coreState, consensusModule.raftLog(), consensusModule.raftMachine() );

        CoreStateDownloader downloader = createCoreStateDownloader( servicesToStopOnStoreCopy );

        this.downloadService = new CoreStateDownloaderService( platformModule.jobScheduler, downloader, commandApplicationProcess, logProvider,
                new ExponentialBackoffStrategy( 1, 30, SECONDS ).newTimeout() );

        this.membershipWaiterLifecycle = createMembershipWaiterLifecycle();

        catchupServer = new CatchupServer( logProvider, userLogProvider, localDatabase::storeId,
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                localDatabase::dataSource, localDatabase::isAvailable, snapshotService, config, platformModule.monitors,
                new CheckpointerSupplier( platformModule.dependencies ), fileSystem, platformModule.pageCache,
                platformModule.storeCopyCheckPointMutex, sslPolicy );

        RaftLogPruner raftLogPruner = new RaftLogPruner( consensusModule.raftMachine(), commandApplicationProcess, platformModule.clock );
        dependencies.satisfyDependency( raftLogPruner );

        life.add( new PruningScheduler( raftLogPruner, jobScheduler,
                config.get( CausalClusteringSettings.raft_log_pruning_frequency ).toMillis(), logProvider ) );

        // Exposes this so that tests can start/stop the catchup server
        dependencies.satisfyDependency( catchupServer );

        servicesToStopOnStoreCopy.add( catchupServer );
    }

    private CoreStateDownloader createCoreStateDownloader( LifeSupport servicesToStopOnStoreCopy )
    {
        long inactivityTimeoutMillis = platformModule.config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ).toMillis();
        CatchUpClient catchUpClient = platformModule.life.add(
                new CatchUpClient( logProvider, Clocks.systemClock(), inactivityTimeoutMillis, platformModule.monitors, sslPolicy ) );

        RemoteStore remoteStore = new RemoteStore(
                logProvider, platformModule.fileSystem, platformModule.pageCache, new StoreCopyClient( catchUpClient, logProvider ),
                new TxPullClient( catchUpClient, platformModule.monitors ), new TransactionLogCatchUpFactory(), platformModule.monitors, localDatabase );

        CopiedStoreRecovery copiedStoreRecovery = platformModule.life.add(
                new CopiedStoreRecovery( platformModule.config, platformModule.kernelExtensions.listFactories(), platformModule.pageCache ) );

        StoreCopyProcess storeCopyProcess = new StoreCopyProcess( platformModule.fileSystem, platformModule.pageCache, localDatabase,
                copiedStoreRecovery, remoteStore, logProvider );

        return new CoreStateDownloader( localDatabase, servicesToStopOnStoreCopy, remoteStore, catchUpClient, logProvider,
                storeCopyProcess, coreStateMachinesModule.coreStateMachines, snapshotService, clusteringModule.topologyService() );
    }

    private MembershipWaiterLifecycle createMembershipWaiterLifecycle()
    {
        long electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout ).toMillis();
        MembershipWaiter membershipWaiter = new MembershipWaiter( identityModule.myself(), jobScheduler,
                dbHealthSupplier, electionTimeout * 4, logProvider );
        long joinCatchupTimeout = config.get( CausalClusteringSettings.join_catch_up_timeout ).toMillis();
        return new MembershipWaiterLifecycle( membershipWaiter, joinCatchupTimeout, consensusModule.raftMachine(), logProvider );
    }

    public CatchupServer catchupServer()
    {
        return catchupServer;
    }

    public CoreLife createCoreLife( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> handler )
    {
        return new CoreLife( consensusModule.raftMachine(),
                localDatabase, clusteringModule.clusterBinder(), commandApplicationProcess, coreStateMachinesModule.coreStateMachines,
                handler, snapshotService, downloadService );
    }

    public CommandApplicationProcess commandApplicationProcess()
    {
        return commandApplicationProcess;
    }

    public CoreStateDownloaderService downloadService()
    {
        return downloadService;
    }
}
