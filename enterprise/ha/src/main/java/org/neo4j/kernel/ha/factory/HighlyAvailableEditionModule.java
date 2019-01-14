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
package org.neo4j.kernel.ha.factory;

import org.jboss.netty.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.ClusterClientModule;
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
import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.function.Factory;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import org.neo4j.kernel.ha.BranchDetectingTxVerifier;
import org.neo4j.kernel.ha.BranchedDataMigrator;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighAvailabilityDiagnostics;
import org.neo4j.kernel.ha.HighAvailabilityLogger;
import org.neo4j.kernel.ha.LastUpdateTime;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.TransactionChecksumLookup;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.cluster.ConversationSPI;
import org.neo4j.kernel.ha.cluster.DefaultConversationSPI;
import org.neo4j.kernel.ha.cluster.DefaultElectionCredentialsProvider;
import org.neo4j.kernel.ha.cluster.DefaultMasterImplSPI;
import org.neo4j.kernel.ha.cluster.HANewSnapshotFunction;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.SimpleHighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.SwitchToMaster;
import org.neo4j.kernel.ha.cluster.SwitchToSlave;
import org.neo4j.kernel.ha.cluster.SwitchToSlaveBranchThenCopy;
import org.neo4j.kernel.ha.cluster.SwitchToSlaveCopyThenBranch;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilitySlaves;
import org.neo4j.kernel.ha.cluster.member.ObservedClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.CommitProcessSwitcher;
import org.neo4j.kernel.ha.cluster.modeswitch.ComponentSwitcherContainer;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.modeswitch.LabelTokenCreatorSwitcher;
import org.neo4j.kernel.ha.cluster.modeswitch.LockManagerSwitcher;
import org.neo4j.kernel.ha.cluster.modeswitch.PropertyKeyCreatorSwitcher;
import org.neo4j.kernel.ha.cluster.modeswitch.RelationshipTypeCreatorSwitcher;
import org.neo4j.kernel.ha.cluster.modeswitch.StatementLocksFactorySwitcher;
import org.neo4j.kernel.ha.cluster.modeswitch.UpdatePullerSwitcher;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.ConversationManager;
import org.neo4j.kernel.ha.com.master.DefaultSlaveFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.id.HaIdReuseEligibility;
import org.neo4j.kernel.ha.management.ClusterDatabaseInfoProvider;
import org.neo4j.kernel.ha.management.HighlyAvailableKernelData;
import org.neo4j.kernel.ha.transaction.CommitPusher;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.core.DelegatingLabelTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingPropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingRelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.enterprise.StandardBoltConnectionTracker;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static java.lang.reflect.Proxy.newProxyInstance;
import static org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache.TransactionMetadata;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise edition.
 */
public class HighlyAvailableEditionModule
        extends EditionModule
{
    private HighAvailabilityMemberStateMachine memberStateMachine;
    public ClusterMembers members;

    public HighlyAvailableEditionModule( final PlatformModule platformModule )
    {
        ioLimiter = new ConfigurableIOLimiter( platformModule.config );

        final LifeSupport life = platformModule.life;
        life.add( platformModule.dataSourceManager );

        final LifeSupport paxosLife = new LifeSupport();
        final LifeSupport clusteringLife = new LifeSupport();

        final FileSystemAbstraction fs = platformModule.fileSystem;
        final File storeDir = platformModule.storeDir;
        final Config config = platformModule.config;
        final Dependencies dependencies = platformModule.dependencies;
        final LogService logging = platformModule.logging;
        final Monitors monitors = platformModule.monitors;

        this.accessCapability = config.get( GraphDatabaseSettings.read_only ) ? new ReadOnly() : new CanWrite();

        idTypeConfigurationProvider = new EnterpriseIdTypeConfigurationProvider( config );

        watcherService = createFileSystemWatcherService( platformModule.fileSystem, storeDir, logging,
                platformModule.jobScheduler, config, fileWatcherFileNameFilter() );
        dependencies.satisfyDependencies( watcherService );
        life.add( watcherService );

        // Set Netty logger
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( logging.getInternalLogProvider() ) );

        life.add( new BranchedDataMigrator( platformModule.storeDir, platformModule.pageCache ) );
        DelegateInvocationHandler<Master> masterDelegateInvocationHandler =
                new DelegateInvocationHandler<>( Master.class );
        Master master = (Master) newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                masterDelegateInvocationHandler );
        InstanceId serverId = config.get( ClusterSettings.server_id );

        RequestContextFactory requestContextFactory = dependencies.satisfyDependency( new RequestContextFactory(
                serverId.toIntegerIndex(),
                dependencies.provideDependency( TransactionIdStore.class ) ) );

        final long idReuseSafeZone = config.get( HaSettings.id_reuse_safe_zone_time ).toMillis();
        TransactionCommittingResponseUnpacker responseUnpacker = dependencies.satisfyDependency(
                new TransactionCommittingResponseUnpacker( dependencies,
                        config.get( HaSettings.pull_apply_batch_size ), idReuseSafeZone ) );

        Supplier<Kernel> kernelProvider = dependencies.provideDependency( Kernel.class );

        transactionStartTimeout = config.get( HaSettings.state_switch_timeout ).toMillis();

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
        OnDiskLastTxIdGetter lastTxIdGetter = new OnDiskLastTxIdGetter(
                () -> platformModule.dependencies.resolveDependency(
                        TransactionIdStore.class ).getLastCommittedTransactionId() );
        ElectionCredentialsProvider electionCredentialsProvider = config.get( HaSettings.slave_only ) ?
                new NotElectableElectionCredentialsProvider() :
                new DefaultElectionCredentialsProvider(
                        config.get( ClusterSettings.server_id ),
                        lastTxIdGetter, () -> electionProviderRef.get().getCurrentState()
                );

        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();

        ClusterClientModule clusterClientModule = new ClusterClientModule( clusteringLife, dependencies, monitors,
                config, logging, electionCredentialsProvider );
        ClusterClient clusterClient = clusterClientModule.clusterClient;
        PaxosClusterMemberEvents localClusterEvents = new PaxosClusterMemberEvents( clusterClient, clusterClient,
                clusterClient, clusterClient, logging.getInternalLogProvider(),
                item ->
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

        ObservedClusterMembers observedMembers = new ObservedClusterMembers( logging.getInternalLogProvider(),
                clusterClient, clusterClient, clusterEvents, config.get( ClusterSettings.server_id ) );

        memberStateMachine = new HighAvailabilityMemberStateMachine( memberContext,
                platformModule.availabilityGuard, observedMembers, clusterEvents, clusterClient,
                logging.getInternalLogProvider() );

        members = dependencies.satisfyDependency( new ClusterMembers( observedMembers, memberStateMachine ) );

        dependencies.satisfyDependency( memberStateMachine );
        paxosLife.add( memberStateMachine );
        electionProviderRef.set( memberStateMachine );

        HighAvailabilityLogger highAvailabilityLogger = new HighAvailabilityLogger( logging.getUserLogProvider(),
                config.get( ClusterSettings.server_id ) );
        platformModule.availabilityGuard.addListener( highAvailabilityLogger );
        clusterEvents.addClusterMemberListener( highAvailabilityLogger );
        clusterClient.addClusterListener( highAvailabilityLogger );

        paxosLife.add( (Lifecycle)clusterEvents );
        paxosLife.add( localClusterMemberAvailability );

        HaIdGeneratorFactory editionIdGeneratorFactory = (HaIdGeneratorFactory) createIdGeneratorFactory( masterDelegateInvocationHandler,
                logging.getInternalLogProvider(), requestContextFactory, fs );
        eligibleForIdReuse = new HaIdReuseEligibility( members, platformModule.clock, idReuseSafeZone );
        createIdComponents( platformModule, dependencies, editionIdGeneratorFactory );
        dependencies.satisfyDependency( idGeneratorFactory );
        dependencies.satisfyDependency( idController );
        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        // TODO There's a cyclical dependency here that should be fixed
        final AtomicReference<HighAvailabilityModeSwitcher> exceptionHandlerRef = new AtomicReference<>();
        InvalidEpochExceptionHandler invalidEpochHandler = () -> exceptionHandlerRef.get().forceElections();

        // At the point in time the LogEntryReader hasn't been instantiated yet. The StorageEngine is responsible
        // for instantiating the CommandReaderFactory, required by a LogEntryReader. The StorageEngine is
        // created in the DataSourceModule, after this module.
        //   That is OK though because all users of it, instantiated below, will not use it right away,
        // but merely provide a way to get access to it. That's why this is a Supplier and will be asked
        // later, after the data source module and all that have started.
        @SuppressWarnings( {"deprecation", "unchecked"} )
        Supplier<LogEntryReader<ReadableClosablePositionAwareChannel>> logEntryReader =
                (Supplier) dependencies.provideDependency( LogEntryReader.class );

        MasterClientResolver masterClientResolver = new MasterClientResolver( logging.getInternalLogProvider(),
                responseUnpacker,
                invalidEpochHandler,
                (int) config.get( HaSettings.read_timeout ).toMillis(),
                (int) config.get( HaSettings.lock_read_timeout ).toMillis(),
                config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( HaSettings.com_chunk_size ).intValue(),
                logEntryReader );

        LastUpdateTime lastUpdateTime = new LastUpdateTime();

        DelegateInvocationHandler<UpdatePuller> updatePullerDelegate =
                new DelegateInvocationHandler<>( UpdatePuller.class );
        UpdatePuller updatePullerProxy = (UpdatePuller) Proxy.newProxyInstance(
                UpdatePuller.class .getClassLoader(), new Class[]{UpdatePuller.class}, updatePullerDelegate );
        dependencies.satisfyDependency( updatePullerProxy );

        PullerFactory pullerFactory = new PullerFactory( requestContextFactory, master, lastUpdateTime,
                logging.getInternalLogProvider(), serverId, invalidEpochHandler,
                config.get( HaSettings.pull_interval ).toMillis(), platformModule.jobScheduler,
                dependencies, platformModule.availabilityGuard, memberStateMachine, monitors );

        dependencies.satisfyDependency( paxosLife.add( pullerFactory.createObligationFulfiller( updatePullerProxy ) ) );

        Function<Slave, SlaveServer> slaveServerFactory =
                slave -> new SlaveServer( slave, slaveServerConfig( config ), logging.getInternalLogProvider(),
                        monitors.newMonitor( ByteCounterMonitor.class, SlaveServer.class ),
                        monitors.newMonitor( RequestMonitor.class, SlaveServer.class ) );

        SwitchToSlave switchToSlaveInstance = chooseSwitchToSlaveStrategy( platformModule, config, dependencies, logging, monitors,
                masterDelegateInvocationHandler, requestContextFactory, clusterMemberAvailability,
                masterClientResolver, updatePullerProxy, pullerFactory, slaveServerFactory, editionIdGeneratorFactory );

        final Factory<MasterImpl.SPI> masterSPIFactory =
                () -> new DefaultMasterImplSPI( platformModule.graphDatabaseFacade, platformModule.fileSystem,
                        platformModule.monitors,
                        labelTokenHolder, propertyKeyTokenHolder, relationshipTypeTokenHolder, this.idGeneratorFactory,
                        platformModule.dependencies.resolveDependency( TransactionCommitProcess.class ),
                        platformModule.dependencies.resolveDependency( CheckPointer.class ),
                        platformModule.dependencies.resolveDependency( TransactionIdStore.class ),
                        platformModule.dependencies.resolveDependency( LogicalTransactionStore.class ),
                        platformModule.dependencies.resolveDependency( NeoStoreDataSource.class ),
                        platformModule.dependencies.resolveDependency( PageCache.class ),
                        platformModule.storeCopyCheckPointMutex,
                        logging.getInternalLogProvider() );

        final Factory<ConversationSPI> conversationSPIFactory =
                () -> new DefaultConversationSPI( lockManager, platformModule.jobScheduler );
        Factory<ConversationManager> conversationManagerFactory =
                () -> new ConversationManager( conversationSPIFactory.newInstance(), config );

        BiFunction<ConversationManager, LifeSupport, Master> masterFactory = ( conversationManager, life1 ) ->
                life1.add( new MasterImpl( masterSPIFactory.newInstance(),
                conversationManager, monitors.newMonitor( MasterImpl.Monitor.class, MasterImpl.class ), config ) );

        BiFunction<Master, ConversationManager, MasterServer> masterServerFactory =
                ( master1, conversationManager ) ->
                {
                    TransactionChecksumLookup txChecksumLookup = new TransactionChecksumLookup(
                            platformModule.dependencies.resolveDependency( TransactionIdStore.class ),
                            platformModule.dependencies.resolveDependency( LogicalTransactionStore.class ) );

                    return new MasterServer( master1, logging.getInternalLogProvider(),
                            masterServerConfig( config ),
                            new BranchDetectingTxVerifier( logging.getInternalLogProvider(), txChecksumLookup ),
                            monitors.newMonitor( ByteCounterMonitor.class, MasterServer.class ),
                            monitors.newMonitor( RequestMonitor.class, MasterServer.class ), conversationManager,
                            logEntryReader.get() );
                };

        SwitchToMaster switchToMasterInstance = new SwitchToMaster( logging, editionIdGeneratorFactory,
                config, dependencies.provideDependency( SlaveFactory.class ),
                conversationManagerFactory,
                masterFactory,
                masterServerFactory,
                masterDelegateInvocationHandler, clusterMemberAvailability,
                platformModule.dependencies.provideDependency( NeoStoreDataSource.class ) );

        ComponentSwitcherContainer componentSwitcherContainer = new ComponentSwitcherContainer();
        Supplier<StoreId> storeIdSupplier = () -> dependencies.resolveDependency( NeoStoreDataSource.class ).getStoreId();

        HighAvailabilityModeSwitcher highAvailabilityModeSwitcher = new HighAvailabilityModeSwitcher(
                switchToSlaveInstance, switchToMasterInstance, clusterClient, clusterMemberAvailability, clusterClient,
                storeIdSupplier, config.get( ClusterSettings.server_id ), componentSwitcherContainer,
                platformModule.dataSourceManager, logging );

        exceptionHandlerRef.set( highAvailabilityModeSwitcher );

        clusterClient.addBindingListener( highAvailabilityModeSwitcher );
        memberStateMachine.addHighAvailabilityMemberListener( highAvailabilityModeSwitcher );

        paxosLife.add( highAvailabilityModeSwitcher );

        componentSwitcherContainer.add( new UpdatePullerSwitcher( updatePullerDelegate, pullerFactory ) );

        life.add( requestContextFactory );
        life.add( responseUnpacker );

        platformModule.diagnosticsManager.appendProvider( new HighAvailabilityDiagnostics( memberStateMachine,
                clusterClient ) );

        dependencies.satisfyDependency( SslPolicyLoader.create( config, logging.getInternalLogProvider() ) ); // for bolt and web server

        // Create HA services
        lockManager = dependencies.satisfyDependency(
                createLockManager( componentSwitcherContainer, config, masterDelegateInvocationHandler,
                        requestContextFactory, platformModule.availabilityGuard, platformModule.clock, logging ) );

        statementLocksFactory = createStatementLocksFactory( componentSwitcherContainer, config, logging );

        propertyKeyTokenHolder = dependencies.satisfyDependency( new DelegatingPropertyKeyTokenHolder(
                createPropertyKeyCreator( config, componentSwitcherContainer,
                        masterDelegateInvocationHandler, requestContextFactory, kernelProvider ) ) );
        labelTokenHolder = dependencies.satisfyDependency( new DelegatingLabelTokenHolder( createLabelIdCreator( config,
                componentSwitcherContainer, masterDelegateInvocationHandler, requestContextFactory,
                kernelProvider ) ) );
        relationshipTypeTokenHolder = dependencies.satisfyDependency( new DelegatingRelationshipTypeTokenHolder(
                createRelationshipTypeCreator( config, componentSwitcherContainer,
                        masterDelegateInvocationHandler, requestContextFactory, kernelProvider ) ) );

        dependencies.satisfyDependency(
                createKernelData( config, platformModule.graphDatabaseFacade, members, fs, platformModule.pageCache,
                        storeDir, lastUpdateTime, lastTxIdGetter, life ) );

        commitProcessFactory = createCommitProcessFactory( dependencies, logging, monitors, config, paxosLife,
                clusterClient, members, platformModule.jobScheduler, master, requestContextFactory,
                componentSwitcherContainer, logEntryReader );

        headerInformationFactory = createHeaderInformationFactory( memberContext );

        schemaWriteGuard = () ->
        {
            if ( !memberStateMachine.isMaster() )
            {
                throw new InvalidTransactionTypeKernelException(
                        "Modifying the database schema can only be done on the master server, " +
                        "this server is a slave. Please issue schema modification commands directly to the master."
                );
            }
        };

        config.augment( GraphDatabaseSettings.allow_upgrade, Settings.FALSE );

        constraintSemantics = new EnterpriseConstraintSemantics();

        coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( platformModule.availabilityGuard, transactionStartTimeout );

        registerRecovery( platformModule.databaseInfo, dependencies, logging );

        UsageData usageData = dependencies.resolveDependency( UsageData.class );
        publishEditionInfo( usageData, platformModule.databaseInfo, config );
        publishServerId( config, usageData );

        // Ordering of lifecycles is important. Clustering infrastructure should start before paxos components
        life.add( clusteringLife );
        life.add( paxosLife );

        dependencies.satisfyDependency( createSessionTracker() );
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
    }

    private StatementLocksFactory createStatementLocksFactory( ComponentSwitcherContainer componentSwitcherContainer,
            Config config, LogService logging )
    {
        StatementLocksFactory configuredStatementLocks = new StatementLocksFactorySelector( lockManager, config, logging ).select();

        DelegateInvocationHandler<StatementLocksFactory> locksFactoryDelegate =
                new DelegateInvocationHandler<>( StatementLocksFactory.class );
        StatementLocksFactory locksFactory =
                (StatementLocksFactory) newProxyInstance( StatementLocksFactory.class.getClassLoader(),
                        new Class[]{StatementLocksFactory.class}, locksFactoryDelegate );

        StatementLocksFactorySwitcher
                locksSwitcher = new StatementLocksFactorySwitcher( locksFactoryDelegate, configuredStatementLocks );
        componentSwitcherContainer.add( locksSwitcher );

        return locksFactory;
    }

    static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any(
                fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                fileName -> fileName.startsWith( IndexConfigStore.INDEX_DB_FILE_NAME ),
                filename -> filename.startsWith( StoreUtil.BRANCH_SUBDIRECTORY ),
                filename -> filename.startsWith( StoreUtil.TEMP_COPY_DIRECTORY_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF )
        );
    }

    private SwitchToSlave chooseSwitchToSlaveStrategy( PlatformModule platformModule, Config config, Dependencies
            dependencies, LogService logging, Monitors monitors, DelegateInvocationHandler<Master>
            masterDelegateInvocationHandler, RequestContextFactory requestContextFactory, ClusterMemberAvailability
            clusterMemberAvailability, MasterClientResolver masterClientResolver, UpdatePuller updatePullerProxy,
            PullerFactory pullerFactory, Function<Slave, SlaveServer> slaveServerFactory, HaIdGeneratorFactory idGeneratorFactory )
    {
        switch ( config.get( HaSettings.branched_data_copying_strategy ) )
        {
            case branch_then_copy:
                return new SwitchToSlaveBranchThenCopy( platformModule.storeDir, logging,
                        platformModule.fileSystem, config, dependencies, idGeneratorFactory,
                        masterDelegateInvocationHandler, clusterMemberAvailability, requestContextFactory,
                        pullerFactory,
                        platformModule.kernelExtensions.listFactories(), masterClientResolver,
                        monitors.newMonitor( SwitchToSlave.Monitor.class ),
                        monitors.newMonitor( StoreCopyClientMonitor.class ),
                        dependencies.provideDependency( NeoStoreDataSource.class ),
                        dependencies.provideDependency( TransactionIdStore.class ),
                        slaveServerFactory, updatePullerProxy, platformModule.pageCache,
                        monitors, platformModule.transactionMonitor );
            case copy_then_branch:
                return new SwitchToSlaveCopyThenBranch( platformModule.storeDir, logging,
                        platformModule.fileSystem, config, dependencies, idGeneratorFactory,
                        masterDelegateInvocationHandler, clusterMemberAvailability, requestContextFactory,
                        pullerFactory,
                        platformModule.kernelExtensions.listFactories(), masterClientResolver,
                        monitors.newMonitor( SwitchToSlave.Monitor.class ),
                        monitors.newMonitor( StoreCopyClientMonitor.class ),
                        dependencies.provideDependency( NeoStoreDataSource.class ),
                        dependencies.provideDependency( TransactionIdStore.class ),
                        slaveServerFactory, updatePullerProxy, platformModule.pageCache,
                        monitors, platformModule.transactionMonitor );
            default:
                throw new RuntimeException( "Unknown branched data copying strategy" );
        }
    }

    private void publishServerId( Config config, UsageData sysInfo )
    {
        sysInfo.set( UsageDataKeys.serverId, config.get( ClusterSettings.server_id ).toString() );
    }

    private TransactionHeaderInformationFactory createHeaderInformationFactory(
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

    private CommitProcessFactory createCommitProcessFactory( Dependencies dependencies, LogService logging,
            Monitors monitors, Config config, LifeSupport paxosLife, ClusterClient clusterClient,
            ClusterMembers members, JobScheduler jobScheduler, Master master,
            RequestContextFactory requestContextFactory, ComponentSwitcherContainer componentSwitcherContainer,
            Supplier<LogEntryReader<ReadableClosablePositionAwareChannel>> logEntryReader )
    {
        DefaultSlaveFactory slaveFactory = dependencies.satisfyDependency( new DefaultSlaveFactory(
                logging.getInternalLogProvider(), monitors, config.get( HaSettings.com_chunk_size ).intValue(),
                logEntryReader ) );

        HostnamePort me = config.get( ClusterSettings.cluster_server );
        Slaves slaves = dependencies.satisfyDependency( paxosLife.add( new HighAvailabilitySlaves( members,
                clusterClient, slaveFactory, me ) ) );

        TransactionPropagator transactionPropagator = new TransactionPropagator( TransactionPropagator.from( config ),
                logging.getInternalLog( TransactionPropagator.class ), slaves, new CommitPusher( jobScheduler ) );
        paxosLife.add( transactionPropagator );

        DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate = new DelegateInvocationHandler<>(
                TransactionCommitProcess.class );

        CommitProcessSwitcher commitProcessSwitcher = new CommitProcessSwitcher( transactionPropagator,
                master, commitProcessDelegate, requestContextFactory, monitors, dependencies );
        componentSwitcherContainer.add( commitProcessSwitcher );

        return new HighlyAvailableCommitProcessFactory( commitProcessDelegate );
    }

    private IdGeneratorFactory createIdGeneratorFactory(
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
            LogProvider logging,
            RequestContextFactory requestContextFactory,
            FileSystemAbstraction fs )
    {
        HaIdGeneratorFactory idGeneratorFactory = new HaIdGeneratorFactory( masterDelegateInvocationHandler, logging,
                requestContextFactory, fs, idTypeConfigurationProvider );
        /*
         * We don't really switch to master here. We just need to initialize the idGenerator so the initial store
         * can be started (if required). In any case, the rest of the database is in pending state, so nothing will
         * happen until events start arriving and that will set us to the proper state anyway.
         */
        idGeneratorFactory.switchToMaster();
        return idGeneratorFactory;
    }

    private Locks createLockManager( ComponentSwitcherContainer componentSwitcherContainer, Config config,
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
            RequestContextFactory requestContextFactory, AvailabilityGuard availabilityGuard, Clock clock, LogService logService )
    {
        DelegateInvocationHandler<Locks> lockManagerDelegate = new DelegateInvocationHandler<>( Locks.class );
        Locks lockManager = (Locks) newProxyInstance( Locks.class.getClassLoader(), new Class[]{Locks.class},
                lockManagerDelegate );

        Factory<Locks> locksFactory = () -> CommunityEditionModule.createLockManager( config, clock, logService );

        LockManagerSwitcher lockManagerModeSwitcher = new LockManagerSwitcher(
                lockManagerDelegate, masterDelegateInvocationHandler, requestContextFactory, availabilityGuard,
                locksFactory, logService.getInternalLogProvider(), config );

        componentSwitcherContainer.add( lockManagerModeSwitcher );
        return lockManager;
    }

    private TokenCreator createRelationshipTypeCreator( Config config,
            ComponentSwitcherContainer componentSwitcherContainer,
            DelegateInvocationHandler<Master> masterInvocationHandler, RequestContextFactory requestContextFactory,
            Supplier<Kernel> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }

        DelegateInvocationHandler<TokenCreator> relationshipTypeCreatorDelegate =
                new DelegateInvocationHandler<>( TokenCreator.class );
        TokenCreator relationshipTypeCreator = (TokenCreator) newProxyInstance( TokenCreator.class.getClassLoader(),
                new Class[]{TokenCreator.class}, relationshipTypeCreatorDelegate );

        RelationshipTypeCreatorSwitcher typeCreatorModeSwitcher = new RelationshipTypeCreatorSwitcher(
                relationshipTypeCreatorDelegate, masterInvocationHandler, requestContextFactory,
                kernelProvider, idGeneratorFactory );

        componentSwitcherContainer.add( typeCreatorModeSwitcher );
        return relationshipTypeCreator;
    }

    private TokenCreator createPropertyKeyCreator( Config config, ComponentSwitcherContainer componentSwitcherContainer,
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
            RequestContextFactory requestContextFactory, Supplier<Kernel> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }

        DelegateInvocationHandler<TokenCreator> propertyKeyCreatorDelegate =
                new DelegateInvocationHandler<>( TokenCreator.class );
        TokenCreator propertyTokenCreator = (TokenCreator) newProxyInstance( TokenCreator.class.getClassLoader(),
                new Class[]{TokenCreator.class}, propertyKeyCreatorDelegate );

        PropertyKeyCreatorSwitcher propertyKeyCreatorModeSwitcher = new PropertyKeyCreatorSwitcher(
                propertyKeyCreatorDelegate, masterDelegateInvocationHandler,
                requestContextFactory, kernelProvider, idGeneratorFactory );

        componentSwitcherContainer.add( propertyKeyCreatorModeSwitcher );
        return propertyTokenCreator;
    }

    private TokenCreator createLabelIdCreator( Config config, ComponentSwitcherContainer componentSwitcherContainer,
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
            RequestContextFactory requestContextFactory, Supplier<Kernel> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }

        DelegateInvocationHandler<TokenCreator> labelIdCreatorDelegate = new DelegateInvocationHandler<>(
                TokenCreator.class );
        TokenCreator labelIdCreator = (TokenCreator) newProxyInstance( TokenCreator.class.getClassLoader(),
                new Class[]{TokenCreator.class}, labelIdCreatorDelegate );

        LabelTokenCreatorSwitcher modeSwitcher = new LabelTokenCreatorSwitcher(
                labelIdCreatorDelegate, masterDelegateInvocationHandler, requestContextFactory, kernelProvider,
                idGeneratorFactory );

        componentSwitcherContainer.add( modeSwitcher );
        return labelIdCreator;
    }

    private KernelData createKernelData( Config config, GraphDatabaseAPI graphDb, ClusterMembers members,
            FileSystemAbstraction fs, PageCache pageCache, File storeDir,
            LastUpdateTime lastUpdateTime, LastTxIdGetter txIdGetter, LifeSupport life )
    {
        ClusterDatabaseInfoProvider databaseInfo = new ClusterDatabaseInfoProvider( members,
                txIdGetter,
                lastUpdateTime );
        return life.add( new HighlyAvailableKernelData( graphDb, members, databaseInfo, fs, pageCache, storeDir, config ) );
    }

    private void registerRecovery( final DatabaseInfo databaseInfo, final DependencyResolver dependencyResolver,
            final LogService logging )
    {
        memberStateMachine.addHighAvailabilityMemberListener( new HighAvailabilityMemberListener.Adapter()
        {
            @Override
            public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_MASTER ) &&
                        event.getNewState().equals( HighAvailabilityMemberState.MASTER ) )
                {
                    doAfterRecoveryAndStartup();
                }
            }

            @Override
            public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_SLAVE ) &&
                        event.getNewState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    doAfterRecoveryAndStartup();
                }
            }

            private void doAfterRecoveryAndStartup()
            {
                try
                {
                    HighlyAvailableEditionModule.this.doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );
                    assureLastCommitTimestampInitialized( dependencyResolver );
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

    private static void assureLastCommitTimestampInitialized( DependencyResolver resolver )
    {
        MetaDataStore metaDataStore = resolver.resolveDependency( MetaDataStore.class );
        LogicalTransactionStore txStore = resolver.resolveDependency( LogicalTransactionStore.class );

        TransactionId txInfo = metaDataStore.getLastCommittedTransaction();
        long lastCommitTimestampFromStore = txInfo.commitTimestamp();
        if ( txInfo.transactionId() == TransactionIdStore.BASE_TX_ID )
        {
            metaDataStore.setLastTransactionCommitTimestamp( TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP );
            return;
        }
        if ( lastCommitTimestampFromStore == TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP ||
             lastCommitTimestampFromStore == TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP )
        {
            long lastCommitTimestampFromLogs;
            try
            {
                TransactionMetadata metadata = txStore.getMetadataFor( txInfo.transactionId() );
                lastCommitTimestampFromLogs = metadata.getTimeWritten();
            }
            catch ( NoSuchTransactionException e )
            {
                lastCommitTimestampFromLogs = TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "Unable to read transaction logs", e );
            }
            metaDataStore.setLastTransactionCommitTimestamp( lastCommitTimestampFromLogs );
        }
    }

    private Server.Configuration masterServerConfig( final Config config )
    {
        return commonConfig( config );
    }

    private Server.Configuration slaveServerConfig( final Config config )
    {
        return commonConfig( config );
    }

    private Server.Configuration commonConfig( final Config config )
    {
        return new Server.Configuration()
        {
            @Override
            public long getOldChannelThreshold()
            {
                return config.get( HaSettings.lock_read_timeout ).toMillis();
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

    @Override
    protected BoltConnectionTracker createSessionTracker()
    {
        return new StandardBoltConnectionTracker();
    }

    @Override
    public void setupSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.setupEnterpriseSecurityModule( platformModule, procedures );
    }
}
