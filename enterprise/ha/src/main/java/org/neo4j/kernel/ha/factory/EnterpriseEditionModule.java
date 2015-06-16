/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.factory;

import org.jboss.netty.logging.InternalLoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.logging.NettyLoggerFactory;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberAvailability;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.com.Server;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.function.Factory;
import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchDetectingTxVerifier;
import org.neo4j.kernel.ha.BranchedDataMigrator;
import org.neo4j.kernel.ha.CommitProcessSwitcher;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighAvailabilityDiagnostics;
import org.neo4j.kernel.ha.HighAvailabilityLogger;
import org.neo4j.kernel.ha.HighAvailabilityMemberInfoProvider;
import org.neo4j.kernel.ha.LabelTokenCreatorModeSwitcher;
import org.neo4j.kernel.ha.LastUpdateTime;
import org.neo4j.kernel.ha.PropertyKeyCreatorModeSwitcher;
import org.neo4j.kernel.ha.RelationshipTypeCreatorModeSwitcher;
import org.neo4j.kernel.ha.TransactionChecksumLookup;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.UpdatePullerClient;
import org.neo4j.kernel.ha.UpdatePullingTransactionObligationFulfiller;
import org.neo4j.kernel.ha.cluster.DefaultElectionCredentialsProvider;
import org.neo4j.kernel.ha.cluster.DefaultMasterImplSPI;
import org.neo4j.kernel.ha.cluster.HANewSnapshotFunction;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.SimpleHighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.SwitchToMaster;
import org.neo4j.kernel.ha.cluster.SwitchToSlave;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilitySlaves;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.DefaultSlaveFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveImpl;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.lock.LockManagerModeSwitcher;
import org.neo4j.kernel.ha.management.ClusterDatabaseInfoProvider;
import org.neo4j.kernel.ha.management.HighlyAvailableKernelData;
import org.neo4j.kernel.ha.transaction.CommitPusher;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.storemigration.UpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByDatabaseModeException;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationControl;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise edition.
 */
public class EnterpriseEditionModule
        extends EditionModule
{
    public HighAvailabilityMemberStateMachine memberStateMachine;
    public ClusterMembers members;

    public EnterpriseEditionModule( final PlatformModule platformModule )
    {
        final LifeSupport life = platformModule.life;
        final FileSystemAbstraction fs = platformModule.fileSystem;
        final File storeDir = platformModule.storeDir;
        final Config config = platformModule.config;
        final Dependencies dependencies = platformModule.dependencies;
        final LogService logging = platformModule.logging;
        final Monitors monitors = platformModule.monitors;

        // Set Netty logger
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( logging.getInternalLogProvider() ) );

        life.add( new BranchedDataMigrator( platformModule.storeDir ) );
        DelegateInvocationHandler<Master> masterDelegateInvocationHandler =
                new DelegateInvocationHandler<>( Master.class );
        Master master = (Master) newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                masterDelegateInvocationHandler );
        InstanceId serverId = config.get( ClusterSettings.server_id );

        RequestContextFactory requestContextFactory = dependencies.satisfyDependency( new RequestContextFactory(
                serverId.toIntegerIndex(),
                dependencies.provideDependency( TransactionIdStore.class ) ) );

        TransactionCommittingResponseUnpacker responseUnpacker = dependencies.satisfyDependency(
                new TransactionCommittingResponseUnpacker( DependenciesProxy.dependencies( dependencies,
                        TransactionCommittingResponseUnpacker.Dependencies.class ) ) );

        Supplier<KernelAPI> kernelProvider = dependencies.provideDependency( KernelAPI.class );

        transactionStartTimeout = config.get( HaSettings.state_switch_timeout );

        DelegateInvocationHandler<ClusterMemberEvents> clusterEventsDelegateInvocationHandler =
                new DelegateInvocationHandler<>( ClusterMemberEvents.class );
        DelegateInvocationHandler<HighAvailabilityMemberContext> memberContextDelegateInvocationHandler =
                new DelegateInvocationHandler<>( HighAvailabilityMemberContext.class );
        DelegateInvocationHandler<ClusterMemberAvailability> clusterMemberAvailabilityDelegateInvocationHandler =
                new DelegateInvocationHandler<>( ClusterMemberAvailability.class );

        ClusterMemberEvents clusterEvents = dependencies.satisfyDependency(
                (ClusterMemberEvents) newProxyInstance(
                        ClusterMemberEvents.class.getClassLoader(),
                        new Class[]{ClusterMemberEvents.class, Lifecycle.class},
                        clusterEventsDelegateInvocationHandler ) );

        HighAvailabilityMemberContext memberContext = (HighAvailabilityMemberContext) newProxyInstance(
                HighAvailabilityMemberContext.class.getClassLoader(),
                new Class[]{HighAvailabilityMemberContext.class}, memberContextDelegateInvocationHandler );
        ClusterMemberAvailability clusterMemberAvailability = dependencies.satisfyDependency(
                (ClusterMemberAvailability) newProxyInstance(
                        ClusterMemberAvailability.class.getClassLoader(),
                        new Class[]{ClusterMemberAvailability.class},
                        clusterMemberAvailabilityDelegateInvocationHandler ) );

        // TODO There's a cyclical dependency here that should be fixed
        final AtomicReference<HighAvailabilityMemberStateMachine> electionProviderRef = new AtomicReference<>();
        ElectionCredentialsProvider electionCredentialsProvider = config.get( HaSettings.slave_only ) ?
                new NotElectableElectionCredentialsProvider() :
                new DefaultElectionCredentialsProvider(
                        config.get( ClusterSettings.server_id ),
                        new OnDiskLastTxIdGetter( platformModule.dependencies.provideDependency( NeoStore.class ) ),
                        new HighAvailabilityMemberInfoProvider()
                        {
                            @Override
                            public HighAvailabilityMemberState getHighAvailabilityMemberState()
                            {
                                return electionProviderRef.get().getCurrentState();
                            }
                        }
                );


        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();


        final ClusterClient clusterClient =
                dependencies.satisfyDependency( new ClusterClient( platformModule.monitors, ClusterClient.adapt(
                        config ), logging,
                        electionCredentialsProvider,
                        objectStreamFactory, objectStreamFactory ) );
        PaxosClusterMemberEvents localClusterEvents = new PaxosClusterMemberEvents( clusterClient, clusterClient,
                clusterClient, clusterClient, logging.getInternalLogProvider(),
                new org.neo4j.function.Predicate<PaxosClusterMemberEvents
                        .ClusterMembersSnapshot>()
                {
                    @Override
                    public boolean test( PaxosClusterMemberEvents.ClusterMembersSnapshot item )
                    {
                        for ( MemberIsAvailable member : item.getCurrentAvailableMembers() )
                        {
                            if ( member.getRoleUri().getScheme().equals( "ha" ) )
                            {
                                if ( HighAvailabilityModeSwitcher.getServerId( member.getRoleUri() ).equals(
                                        platformModule.config.get( ClusterSettings.server_id ) ) )
                                {
                                    logging.getInternalLog( PaxosClusterMemberEvents.class ).error(
                                            String.format( "Instance " +
                                                            "%s has" +
                                                            " the same serverId as ours (%s) - will not " +
                                                            "join this cluster",
                                                    member.getRoleUri(),
                                                    config.get( ClusterSettings.server_id ).toIntegerIndex()
                                            ) );
                                    return true;
                                }
                            }
                        }
                        return true;
                    }
                }, new HANewSnapshotFunction(), objectStreamFactory, objectStreamFactory,
                platformModule.monitors.newMonitor( NamedThreadFactory.Monitor.class )
        );

        // Force a reelection after we enter the cluster
        // and when that election is finished refresh the snapshot
        clusterClient.addClusterListener( new ClusterListener.Adapter()
        {
            boolean hasRequestedElection = true; // This ensures that the election result is (at least) from our
            // request or thereafter

            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                clusterClient.performRoleElections();
            }

            @Override
            public void elected( String role, InstanceId instanceId, URI electedMember )
            {
                if ( hasRequestedElection && role.equals( ClusterConfiguration.COORDINATOR ) )
                {
                    clusterClient.removeClusterListener( this );
                }
            }
        } );

        HighAvailabilityMemberContext localMemberContext = new SimpleHighAvailabilityMemberContext( clusterClient
                .getServerId(), config.get( HaSettings.slave_only ) );
        PaxosClusterMemberAvailability localClusterMemberAvailability = new PaxosClusterMemberAvailability(
                clusterClient.getServerId(), clusterClient, clusterClient, logging.getInternalLogProvider(),
                objectStreamFactory,
                objectStreamFactory );

        memberContextDelegateInvocationHandler.setDelegate( localMemberContext );
        clusterEventsDelegateInvocationHandler.setDelegate( localClusterEvents );
        clusterMemberAvailabilityDelegateInvocationHandler.setDelegate( localClusterMemberAvailability );

        members = dependencies.satisfyDependency( new ClusterMembers( clusterClient, clusterClient,
                clusterEvents,
                config.get( ClusterSettings.server_id ) ) );
        memberStateMachine = new HighAvailabilityMemberStateMachine(
                memberContext, platformModule.availabilityGuard, members,
                clusterEvents,
                clusterClient, logging.getInternalLogProvider() );
        electionProviderRef.set( memberStateMachine );

        HighAvailabilityLogger highAvailabilityLogger = new HighAvailabilityLogger( logging.getUserLogProvider(),
                config.get( ClusterSettings.server_id ) );
        platformModule.availabilityGuard.addListener( highAvailabilityLogger );
        clusterEvents.addClusterMemberListener( highAvailabilityLogger );
        clusterClient.addClusterListener( highAvailabilityLogger );

        LifeSupport paxosLife = new LifeSupport();

        paxosLife.add( clusterClient );
        paxosLife.add( memberStateMachine );
        paxosLife.add( clusterEvents );
        paxosLife.add( localClusterMemberAvailability );

        idGeneratorFactory = dependencies.satisfyDependency(
                createIdGeneratorFactory( masterDelegateInvocationHandler, logging.getInternalLogProvider(),
                        requestContextFactory ) );

        // TODO There's a cyclical dependency here that should be fixed
        final AtomicReference<HighAvailabilityModeSwitcher> exceptionHandlerRef = new AtomicReference<>();
        InvalidEpochExceptionHandler invalidEpochHandler = new InvalidEpochExceptionHandler()
        {
            @Override
            public void handle()
            {
                exceptionHandlerRef.get().forceElections();
            }
        };

        MasterClientResolver masterClientResolver = new MasterClientResolver( logging.getInternalLogProvider(),
                responseUnpacker,
                invalidEpochHandler,
                config.get( HaSettings.read_timeout ).intValue(),
                config.get( HaSettings.lock_read_timeout ).intValue(),
                config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( HaSettings.com_chunk_size ).intValue() );

        LastUpdateTime lastUpdateTime = new LastUpdateTime();

        UpdatePuller updatePuller = dependencies.satisfyDependency( life.add(
                new UpdatePuller( memberStateMachine, requestContextFactory, master, lastUpdateTime,
                        logging.getInternalLogProvider(), serverId, invalidEpochHandler ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullerClient( config.get( HaSettings.pull_interval ),
                platformModule.jobScheduler, logging.getInternalLogProvider(), updatePuller,
                platformModule.availabilityGuard ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullingTransactionObligationFulfiller(
                updatePuller, memberStateMachine, serverId,
                dependencies.provideDependency( TransactionIdStore.class ) ) ) );


        Factory<Slave> slaveFactory = new Factory<Slave>()
        {
            @Override
            public Slave newInstance()
            {
                return new SlaveImpl( dependencies.resolveDependency( TransactionObligationFulfiller.class ) );
            }
        };

        Function<Slave, SlaveServer> slaveServerFactory = new Function<Slave, SlaveServer>()
        {
            @Override
            public SlaveServer apply( Slave slave ) throws RuntimeException
            {
                return new SlaveServer( slave, slaveServerConfig( config ), logging.getInternalLogProvider(),
                        monitors.newMonitor( ByteCounterMonitor.class, SlaveServer.class ),
                        monitors.newMonitor( RequestMonitor.class, SlaveServer.class ) );
            }
        };

        SwitchToSlave switchToSlaveInstance = new SwitchToSlave( platformModule.storeDir, logging,
                platformModule.fileSystem, members,
                config, dependencies, (HaIdGeneratorFactory) idGeneratorFactory,
                masterDelegateInvocationHandler, clusterMemberAvailability,
                requestContextFactory, platformModule.kernelExtensions.listFactories(), masterClientResolver,
                monitors.newMonitor( SwitchToSlave.Monitor.class ),
                monitors.newMonitor( StoreCopyClient.Monitor.class ),
                dependencies.provideDependency( NeoStoreDataSource.class ),
                dependencies.provideDependency( TransactionIdStore.class ),
                slaveFactory, slaveServerFactory, updatePuller, platformModule.pageCache, monitors );

        final Factory<MasterImpl.SPI> masterSPIFactory = new Factory<MasterImpl.SPI>()
        {
            @Override
            public MasterImpl.SPI newInstance()
            {
                return new DefaultMasterImplSPI( platformModule.graphDatabaseFacade, platformModule.fileSystem,
                        platformModule.monitors,
                        labelTokenHolder, propertyKeyTokenHolder, relationshipTypeTokenHolder, idGeneratorFactory,
                        lockManager, platformModule.dependencies.resolveDependency( TransactionCommitProcess.class ),
                        platformModule.dependencies.resolveDependency( LogRotationControl.class ),
                        platformModule.dependencies.resolveDependency( TransactionIdStore.class ),
                        platformModule.dependencies.resolveDependency( LogicalTransactionStore.class ),
                        platformModule.dependencies.resolveDependency( NeoStoreDataSource.class ),
                        platformModule.jobScheduler );
            }
        };

        Factory<Master> masterFactory = new Factory<Master>()
        {
            @Override
            public Master newInstance()
            {
                return new MasterImpl( masterSPIFactory.newInstance(),
                        monitors.newMonitor( MasterImpl.Monitor.class, MasterImpl.class ), config );
            }
        };

        Function<Master, MasterServer> masterServerFactory = new Function<Master, MasterServer>()
        {
            @Override
            public MasterServer apply( Master master ) throws RuntimeException
            {
                TransactionChecksumLookup txChecksumLookup = new TransactionChecksumLookup(
                        platformModule.dependencies.resolveDependency( TransactionIdStore.class ),
                        platformModule.dependencies.resolveDependency( LogicalTransactionStore.class ) );

                MasterServer masterServer = new MasterServer( master, logging.getInternalLogProvider(),
                        masterServerConfig(
                                config ),
                        new BranchDetectingTxVerifier( logging.getInternalLogProvider(), txChecksumLookup ),
                        monitors.newMonitor( ByteCounterMonitor.class, MasterServer.class ),
                        monitors.newMonitor( RequestMonitor.class, MasterServer.class ) );
                return masterServer;
            }
        };

        SwitchToMaster switchToMasterInstance = new SwitchToMaster( logging, (HaIdGeneratorFactory) idGeneratorFactory,
                config, dependencies.provideDependency( SlaveFactory.class ),
                masterFactory,
                masterServerFactory,
                masterDelegateInvocationHandler, clusterMemberAvailability,
                platformModule.dependencies.provideDependency(
                        NeoStoreDataSource.class ) );

        final HighAvailabilityModeSwitcher highAvailabilityModeSwitcher = new HighAvailabilityModeSwitcher(
                switchToSlaveInstance, switchToMasterInstance,
                clusterClient, clusterMemberAvailability, clusterClient, new Supplier<StoreId>()
        {
            @Override
            public StoreId get()
            {
                return dependencies.resolveDependency( NeoStoreDataSource.class ).getStoreId();
            }
        }, config.get( ClusterSettings.server_id ),
                logging );
        exceptionHandlerRef.set( highAvailabilityModeSwitcher );

        clusterClient.addBindingListener( highAvailabilityModeSwitcher );
        memberStateMachine.addHighAvailabilityMemberListener( highAvailabilityModeSwitcher );

        /*
         * We always need the mode switcher and we need it to restart on switchover.
         */
        paxosLife.add( highAvailabilityModeSwitcher );


        life.add( requestContextFactory );

        life.add( responseUnpacker );

        life.add( paxosLife );

        platformModule.diagnosticsManager.appendProvider( new HighAvailabilityDiagnostics( memberStateMachine,
                clusterClient ) );

        // Create HA services
        lockManager = dependencies.satisfyDependency(
                createLockManager( memberStateMachine, config, masterDelegateInvocationHandler, requestContextFactory,
                        platformModule.availabilityGuard, logging ) );

        propertyKeyTokenHolder = life.add( dependencies.satisfyDependency( new PropertyKeyTokenHolder(
                createPropertyKeyCreator( config, memberStateMachine, masterDelegateInvocationHandler,
                        requestContextFactory, kernelProvider ) ) ) );
        labelTokenHolder = life.add( dependencies.satisfyDependency( new LabelTokenHolder( createLabelIdCreator( config,
                memberStateMachine, masterDelegateInvocationHandler, requestContextFactory, kernelProvider ) ) ) );
        relationshipTypeTokenHolder = life.add( dependencies.satisfyDependency( new RelationshipTypeTokenHolder(
                createRelationshipTypeCreator( config, memberStateMachine, masterDelegateInvocationHandler,
                        requestContextFactory, kernelProvider ) ) ) );

        life.add( dependencies.satisfyDependency( createKernelData( config, platformModule.graphDatabaseFacade,
                members, fs, storeDir, lastUpdateTime, dependencies.provideDependency( NeoStore.class ) ) ) );

        commitProcessFactory = createCommitProcessFactory( dependencies, logging, monitors, config, life,
                clusterClient, members, platformModule.jobScheduler, master, requestContextFactory,
                memberStateMachine );

        headerInformationFactory = createHeaderInformationFactory( memberContext );

        schemaWriteGuard = new SchemaWriteGuard()
        {
            @Override
            public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
            {
                if ( !memberStateMachine.isMaster() )
                {
                    throw new InvalidTransactionTypeKernelException(
                            "Modifying the database schema can only be done on the master server, " +
                                    "this server is a slave. Please issue schema modification commands directly to " +
                                    "the master."
                    );
                }
            }
        };

        upgradeConfiguration = new HAUpgradeConfiguration();

        registerRecovery( config.get( GraphDatabaseFacadeFactory.Configuration.editionName ), dependencies, logging );

        publishEditionInfo( config, dependencies.resolveDependency( UsageData.class ) );
    }

    private void publishEditionInfo( Config config, UsageData sysInfo )
    {
        sysInfo.set( UsageDataKeys.edition, UsageDataKeys.Edition.enterprise );
        sysInfo.set( UsageDataKeys.operationalMode, UsageDataKeys.OperationalMode.ha );
        sysInfo.set( UsageDataKeys.serverId, config.get( ClusterSettings.server_id ).toString() );
    }


    protected TransactionHeaderInformationFactory createHeaderInformationFactory(
            final HighAvailabilityMemberContext memberContext )
    {
        return new TransactionHeaderInformationFactory.WithRandomBytes()
        {
            @Override
            protected TransactionHeaderInformation createUsing( byte[] additionalHeader )
            {
                return new TransactionHeaderInformation( memberContext.getElectedMasterId().toIntegerIndex(),
                        memberContext.getMyId().toIntegerIndex(), additionalHeader );
            }
        };
    }

    protected CommitProcessFactory createCommitProcessFactory( Dependencies dependencies, LogService logging,
                                                               Monitors monitors, Config config, LifeSupport life,
                                                               ClusterClient clusterClient, ClusterMembers members,
                                                               JobScheduler jobScheduler, final Master master,
                                                               final RequestContextFactory requestContextFactory,
                                                               final HighAvailabilityMemberStateMachine
                                                                       memberStateMachine )
    {
        final DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate =
                new DelegateInvocationHandler<>( TransactionCommitProcess.class );

        DefaultSlaveFactory slaveFactory = dependencies.satisfyDependency(
                new DefaultSlaveFactory( logging.getInternalLogProvider(), monitors,
                        config.get( HaSettings.com_chunk_size ).intValue() ) );

        Slaves slaves = dependencies.satisfyDependency(
                life.add( new HighAvailabilitySlaves( members, clusterClient, slaveFactory ) ) );

        final TransactionPropagator pusher = life.add( new TransactionPropagator( TransactionPropagator.from( config ),
                logging.getInternalLog( TransactionPropagator.class ), slaves, new CommitPusher( jobScheduler ) ) );

        return new CommitProcessFactory()
        {
            @Override
            public TransactionCommitProcess create( TransactionAppender appender,
                                                    KernelHealth kernelHealth, NeoStore neoStore,
                                                    TransactionRepresentationStoreApplier storeApplier,
                                                    NeoStoreInjectedTransactionValidator txValidator,
                                                    IndexUpdatesValidator indexUpdatesValidator,
                                                    Config config )
            {
                if ( config.get( GraphDatabaseSettings.read_only ) )
                {
                    return new ReadOnlyTransactionCommitProcess();
                }
                else
                {
                    TransactionCommitProcess inner = new TransactionRepresentationCommitProcess( appender, kernelHealth,
                                                neoStore, storeApplier, indexUpdatesValidator );
                    new CommitProcessSwitcher( pusher, master, commitProcessDelegate, requestContextFactory,
                            memberStateMachine, txValidator, inner );

                    return (TransactionCommitProcess)
                            newProxyInstance( TransactionCommitProcess.class.getClassLoader(),
                                    new Class[]{TransactionCommitProcess.class}, commitProcessDelegate );
                }
            }
        };
    }

    protected IdGeneratorFactory createIdGeneratorFactory( DelegateInvocationHandler<Master>
                                                                   masterDelegateInvocationHandler,
                                                           LogProvider logging, RequestContextFactory
            requestContextFactory )
    {
        idGeneratorFactory = new HaIdGeneratorFactory( masterDelegateInvocationHandler, logging,
                requestContextFactory );

        /*
         * We don't really switch to master here. We just need to initialize the idGenerator so the initial store
         * can be started (if required). In any case, the rest of the database is in pending state, so nothing will
         * happen until events start arriving and that will set us to the proper state anyway.
         */
        ((HaIdGeneratorFactory) idGeneratorFactory).switchToMaster();

        return idGeneratorFactory;
    }

    protected Locks createLockManager( HighAvailabilityMemberStateMachine memberStateMachine, final Config config,
                                       DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
                                       RequestContextFactory requestContextFactory,
                                       AvailabilityGuard availabilityGuard, final LogService logging )
    {
        DelegateInvocationHandler<Locks> lockManagerDelegate = new DelegateInvocationHandler<>( Locks.class );
        final Locks lockManager = (Locks) newProxyInstance( Locks.class.getClassLoader(),
                new Class[]{Locks.class},
                lockManagerDelegate );
        new LockManagerModeSwitcher( memberStateMachine, lockManagerDelegate, masterDelegateInvocationHandler,
                requestContextFactory, availabilityGuard, config, new Factory<Locks>()
        {
            @Override
            public Locks newInstance()
            {
                return CommunityEditionModule.createLockManager( config, logging );
            }
        } );
        return lockManager;
    }

    protected TokenCreator createRelationshipTypeCreator( Config config,
                                                          HighAvailabilityMemberStateMachine memberStateMachine,
                                                          DelegateInvocationHandler<Master>
                                                                  masterDelegateInvocationHandler,
                                                          RequestContextFactory requestContextFactory,
                                                          Supplier<KernelAPI> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> relationshipTypeCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator relationshipTypeCreator = (TokenCreator) newProxyInstance( TokenCreator.class.getClassLoader(),
                    new Class[]{TokenCreator.class}, relationshipTypeCreatorDelegate );

            new RelationshipTypeCreatorModeSwitcher( memberStateMachine, relationshipTypeCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );

            return relationshipTypeCreator;
        }
    }

    protected TokenCreator createPropertyKeyCreator( Config config,
                                                     HighAvailabilityMemberStateMachine memberStateMachine,
                                                     DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
                                                     RequestContextFactory requestContextFactory,
                                                     Supplier<KernelAPI> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> propertyKeyCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator propertyTokenCreator = (TokenCreator) newProxyInstance( TokenCreator.class.getClassLoader(),
                    new Class[]{TokenCreator.class}, propertyKeyCreatorDelegate );
            new PropertyKeyCreatorModeSwitcher( memberStateMachine, propertyKeyCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );
            return propertyTokenCreator;
        }
    }

    protected TokenCreator createLabelIdCreator( Config config, HighAvailabilityMemberStateMachine memberStateMachine,
                                                 DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
                                                 RequestContextFactory requestContextFactory,
                                                 Supplier<KernelAPI> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> labelIdCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator labelIdCreator = (TokenCreator) newProxyInstance( TokenCreator.class.getClassLoader(),
                    new Class[]{TokenCreator.class}, labelIdCreatorDelegate );
            new LabelTokenCreatorModeSwitcher( memberStateMachine, labelIdCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );
            return labelIdCreator;
        }
    }

    protected KernelData createKernelData( Config config, GraphDatabaseAPI graphDb, ClusterMembers members,
                                           FileSystemAbstraction fs, File storeDir,
                                           LastUpdateTime lastUpdateTime, Supplier<NeoStore> neoStoreSupplier )
    {
        OnDiskLastTxIdGetter txIdGetter = new OnDiskLastTxIdGetter( neoStoreSupplier );
        ClusterDatabaseInfoProvider databaseInfo = new ClusterDatabaseInfoProvider( members,
                txIdGetter,
                lastUpdateTime );
        return new HighlyAvailableKernelData( graphDb, members, databaseInfo, fs, storeDir, config );
    }

    protected void registerRecovery( final String editionName, final DependencyResolver dependencyResolver,
                                     final LogService logging )
    {
        memberStateMachine.addHighAvailabilityMemberListener( new HighAvailabilityMemberListener()
        {
            @Override
            public void masterIsElected( HighAvailabilityMemberChangeEvent event )
            {
            }

            @Override
            public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_MASTER ) &&
                        event.getNewState().equals( HighAvailabilityMemberState.MASTER ) )
                {
                    doAfterRecoveryAndStartup( true );
                }
            }

            @Override
            public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_SLAVE ) &&
                        event.getNewState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    doAfterRecoveryAndStartup( false );
                }
            }

            @Override
            public void instanceStops( HighAvailabilityMemberChangeEvent event )
            {
            }

            private void doAfterRecoveryAndStartup( boolean isMaster )
            {
                try
                {
                    EnterpriseEditionModule.this.doAfterRecoveryAndStartup( editionName, dependencyResolver, isMaster );
                }
                catch ( Throwable throwable )
                {
                    Log messagesLog = logging.getInternalLog( EnterpriseEditionModule.class );
                    messagesLog.error( "Post recovery error", throwable );
                    try
                    {
                        memberStateMachine.stop();
                    }
                    catch ( Throwable throwable1 )
                    {
                        messagesLog.warn( "Could not stop", throwable1 );
                    }
                    try
                    {
                        memberStateMachine.start();
                    }
                    catch ( Throwable throwable1 )
                    {
                        messagesLog.warn( "Could not start", throwable1 );
                    }
                }
            }
        } );
    }

    protected void doAfterRecoveryAndStartup( String editionName, DependencyResolver resolver, boolean isMaster )
    {
        super.doAfterRecoveryAndStartup( editionName, resolver );

        if ( isMaster )
        {
            new RemoveOrphanConstraintIndexesOnStartup( resolver.resolveDependency( KernelAPI.class ),
                    resolver.resolveDependency( LogService.class ).getInternalLogProvider() ).perform();
        }
    }

    private Server.Configuration masterServerConfig( final Config config )
    {
        return new Server.Configuration()
        {
            @Override
            public long getOldChannelThreshold()
            {
                return config.get( HaSettings.lock_read_timeout );
            }

            @Override
            public int getMaxConcurrentTransactions()
            {
                return config.get( HaSettings.max_concurrent_channels_per_slave );
            }

            @Override
            public int getChunkSize()
            {
                return config.get( HaSettings.com_chunk_size ).intValue();
            }

            @Override
            public HostnamePort getServerAddress()
            {
                return config.get( HaSettings.ha_server );
            }
        };
    }

    private Server.Configuration slaveServerConfig( final Config config )
    {
        return new Server.Configuration()
        {
            @Override
            public long getOldChannelThreshold()
            {
                return config.get( HaSettings.lock_read_timeout );
            }

            @Override
            public int getMaxConcurrentTransactions()
            {
                return config.get( HaSettings.max_concurrent_channels_per_slave );
            }

            @Override
            public int getChunkSize()
            {
                return config.get( HaSettings.com_chunk_size ).intValue();
            }

            @Override
            public HostnamePort getServerAddress()
            {
                return config.get( HaSettings.ha_server );
            }
        };
    }


    private static final class HAUpgradeConfiguration implements UpgradeConfiguration
    {
        @Override
        public void checkConfigurationAllowsAutomaticUpgrade()
        {
            throw new UpgradeNotAllowedByDatabaseModeException();
        }
    }
}
