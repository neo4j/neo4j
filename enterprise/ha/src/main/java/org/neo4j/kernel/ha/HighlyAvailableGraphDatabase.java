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
package org.neo4j.kernel.ha;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Map;

import org.jboss.netty.logging.InternalLoggerFactory;

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
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.cluster.DefaultElectionCredentialsProvider;
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
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.lock.LockManagerModeSwitcher;
import org.neo4j.kernel.ha.management.ClusterDatabaseInfoProvider;
import org.neo4j.kernel.ha.management.HighlyAvailableKernelData;
import org.neo4j.kernel.ha.transaction.CommitPusher;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.UpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByDatabaseModeException;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.LogbackWeakDependency;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.kernel.logging.LogbackWeakDependency.DEFAULT_TO_CLASSIC;
import static org.neo4j.kernel.logging.LogbackWeakDependency.NEW_LOGGER_CONTEXT;

/**
 * This has all the functionality of an embedded database, with the addition of services
 * for handling clustering.
 */
public class HighlyAvailableGraphDatabase extends InternalAbstractGraphDatabase
{
    private final LifeSupport paxosLife = new LifeSupport();
    private final org.neo4j.kernel.impl.util.Dependencies dependencies =
            new org.neo4j.kernel.impl.util.Dependencies( new Provider<DependencyResolver>()
            {
                @Override
                public DependencyResolver instance()
                {
                    return HighlyAvailableGraphDatabase.this.dependencyResolver;
                }
            } );
    private RequestContextFactory requestContextFactory;
    private ClusterMembers members;
    private DelegateInvocationHandler<Master> masterDelegateInvocationHandler;
    private Master master;
    private HighAvailabilityMemberStateMachine memberStateMachine;
    private LastUpdateTime lastUpdateTime;
    private HighAvailabilityMemberContext memberContext;
    private ClusterClient clusterClient;
    private ClusterMemberAvailability clusterMemberAvailability;
    private HighAvailabilityModeSwitcher highAvailabilityModeSwitcher;
    private long stateSwitchTimeoutMillis;
    private TransactionCommittingResponseUnpacker responseUnpacker;
    private Provider<KernelAPI> kernelProvider;
    private InvalidEpochExceptionHandler invalidEpochHandler;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                         Iterable<CacheProvider> cacheProviders, Monitors monitors )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ).cacheProviders( cacheProviders ).monitors( monitors ) );
    }

    public HighlyAvailableGraphDatabase( String storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                         Iterable<CacheProvider> cacheProviders )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ).cacheProviders( cacheProviders ) );
    }

    public HighlyAvailableGraphDatabase( String storeDir, Map<String,String> params, Dependencies dependencies )
    {
        super( storeDir, params, dependencies );
        run();
    }

    @Override
    protected void create()
    {
        life.add( new BranchedDataMigrator( storeDir ) );
        masterDelegateInvocationHandler = new DelegateInvocationHandler<>( Master.class );
        master = (Master) Proxy.newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                masterDelegateInvocationHandler );
        InstanceId serverId = config.get( ClusterSettings.server_id );
        requestContextFactory = dependencies.satisfyDependency( new RequestContextFactory( serverId.toIntegerIndex(),
                getDependencyResolver() ) );

        this.responseUnpacker = dependencies.satisfyDependency(
                new TransactionCommittingResponseUnpacker( getDependencyResolver() ) );

        kernelProvider = new Provider<KernelAPI>()
        {
            @Override
            public KernelAPI instance()
            {
                return neoDataSource.getKernel();
            }
        };

        super.create();

        life.add( requestContextFactory );

        life.add( responseUnpacker );

        UpdatePuller updatePuller = dependencies.satisfyDependency( life.add(
                new UpdatePuller( memberStateMachine, requestContextFactory, master, lastUpdateTime,
                        logging, serverId, invalidEpochHandler ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullerClient( config.get( HaSettings.pull_interval ),
                jobScheduler, logging, updatePuller, availabilityGuard ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullingTransactionObligationFulfiller(
                updatePuller, memberStateMachine, serverId, dependencies ) ) );

        stateSwitchTimeoutMillis = config.get( HaSettings.state_switch_timeout );

        life.add( paxosLife );

        life.add( new DatabaseAvailability( availabilityGuard, transactionMonitor ) );

        life.add( new StartupWaiter() );

        diagnosticsManager.appendProvider( new HighAvailabilityDiagnostics( memberStateMachine, clusterClient ) );
    }

    @Override
    protected UpgradeConfiguration createUpgradeConfiguration()
    {
        return new HAUpgradeConfiguration();
    }

    @Override
    protected void createDatabaseAvailability()
    {
        // Skip this, it's done manually in create() to ensure it is as late as possible
    }

    public void start()
    {
        life.start();
    }

    public void stop()
    {
        life.stop();
    }

    @Override
    public org.neo4j.graphdb.Transaction beginTx()
    {
        availabilityGuard.checkAvailability( stateSwitchTimeoutMillis, TransactionFailureException.class );
        return super.beginTx();
    }

    @Override
    public IndexManager index()
    {
        availabilityGuard.checkAvailability( stateSwitchTimeoutMillis, TransactionFailureException.class );
        return super.index();
    }

    @Override
    protected Logging createLogging()
    {
        Logging loggingService = life.add( LogbackWeakDependency.tryLoadLogbackService( config,
                NEW_LOGGER_CONTEXT,
                DEFAULT_TO_CLASSIC, monitors ) );

        // Set Netty logger
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( loggingService ) );

        return loggingService;
    }

    @Override
    protected void createTxHook()
    {
        DelegateInvocationHandler<ClusterMemberEvents> clusterEventsDelegateInvocationHandler =
                new DelegateInvocationHandler<>( ClusterMemberEvents.class );
        DelegateInvocationHandler<HighAvailabilityMemberContext> memberContextDelegateInvocationHandler =
                new DelegateInvocationHandler<>( HighAvailabilityMemberContext.class );
        DelegateInvocationHandler<ClusterMemberAvailability> clusterMemberAvailabilityDelegateInvocationHandler =
                new DelegateInvocationHandler<>( ClusterMemberAvailability.class );

        ClusterMemberEvents clusterEvents = dependencies.satisfyDependency(
                (ClusterMemberEvents) Proxy.newProxyInstance(
                        ClusterMemberEvents.class.getClassLoader(),
                        new Class[]{ClusterMemberEvents.class, Lifecycle.class},
                        clusterEventsDelegateInvocationHandler ) );

        memberContext = (HighAvailabilityMemberContext) Proxy.newProxyInstance(
                HighAvailabilityMemberContext.class.getClassLoader(),
                new Class[]{HighAvailabilityMemberContext.class}, memberContextDelegateInvocationHandler );
        clusterMemberAvailability = dependencies.satisfyDependency( (ClusterMemberAvailability) Proxy.newProxyInstance(
                ClusterMemberAvailability.class.getClassLoader(),
                new Class[]{ClusterMemberAvailability.class}, clusterMemberAvailabilityDelegateInvocationHandler ) );

        ElectionCredentialsProvider electionCredentialsProvider = config.get( HaSettings.slave_only ) ?
                                                                  new NotElectableElectionCredentialsProvider() :
                                                                  new DefaultElectionCredentialsProvider(
                                                                          config.get( ClusterSettings.server_id ),
                                                                          new OnDiskLastTxIdGetter( this ),
                                                                          new HighAvailabilityMemberInfoProvider()
                                                                          {
                                                                              @Override
                                                                              public HighAvailabilityMemberState
                                                                              getHighAvailabilityMemberState()
                                                                              {
                                                                                  return memberStateMachine
                                                                                          .getCurrentState();
                                                                              }
                                                                          }
                                                                  );


        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();


        clusterClient =
                dependencies.satisfyDependency( new ClusterClient( monitors, ClusterClient.adapt( config ), logging,
                        electionCredentialsProvider,
                        objectStreamFactory, objectStreamFactory ) );
        PaxosClusterMemberEvents localClusterEvents = new PaxosClusterMemberEvents( clusterClient, clusterClient,
                clusterClient, clusterClient, logging, new Predicate<PaxosClusterMemberEvents.ClusterMembersSnapshot>()
        {
            @Override
            public boolean accept( PaxosClusterMemberEvents.ClusterMembersSnapshot item )
            {
                for ( MemberIsAvailable member : item.getCurrentAvailableMembers() )
                {
                    if ( member.getRoleUri().getScheme().equals( "ha" ) )
                    {
                        if ( HighAvailabilityModeSwitcher.getServerId( member.getRoleUri() ).equals(
                                config.get( ClusterSettings.server_id ) ) )
                        {
                            msgLog.error( String.format( "Instance %s has the same serverId as ours (%s) - will not " +
                                                         "join this cluster",
                                    member.getRoleUri(), config.get( ClusterSettings.server_id ).toIntegerIndex()
                            ) );
                            return true;
                        }
                    }
                }
                return true;
            }
        }, new HANewSnapshotFunction(), objectStreamFactory, objectStreamFactory,
                monitors.newMonitor( NamedThreadFactory.Monitor.class )
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
                clusterClient.getServerId(), clusterClient, clusterClient, logging, objectStreamFactory,
                objectStreamFactory );

        memberContextDelegateInvocationHandler.setDelegate( localMemberContext );
        clusterEventsDelegateInvocationHandler.setDelegate( localClusterEvents );
        clusterMemberAvailabilityDelegateInvocationHandler.setDelegate( localClusterMemberAvailability );

        members = dependencies.satisfyDependency( new ClusterMembers( clusterClient, clusterClient, clusterEvents,
                config.get( ClusterSettings.server_id ) ) );
        memberStateMachine = new HighAvailabilityMemberStateMachine( memberContext, availabilityGuard, members,
                clusterEvents,
                clusterClient, logging.getMessagesLog( HighAvailabilityMemberStateMachine.class ) );

        HighAvailabilityConsoleLogger highAvailabilityConsoleLogger = new HighAvailabilityConsoleLogger( logging
                .getConsoleLog( HighAvailabilityConsoleLogger.class ), config.get( ClusterSettings
                .server_id ) );
        availabilityGuard.addListener( highAvailabilityConsoleLogger );
        clusterEvents.addClusterMemberListener( highAvailabilityConsoleLogger );
        clusterClient.addClusterListener( highAvailabilityConsoleLogger );

        paxosLife.add( clusterClient );
        paxosLife.add( memberStateMachine );
        paxosLife.add( clusterEvents );
        paxosLife.add( localClusterMemberAvailability );

    }

    @Override
    public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
    {
        if ( !isMaster() )
        {
            throw new InvalidTransactionTypeKernelException(
                    "Modifying the database schema can only be done on the master server, " +
                    "this server is a slave. Please issue schema modification commands directly to the master."
            );
        }
    }

    @Override
    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
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

    @Override
    protected CommitProcessFactory getCommitProcessFactory()
    {
        final DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate =
                new DelegateInvocationHandler<>( TransactionCommitProcess.class );

        DefaultSlaveFactory slaveFactory = dependencies.satisfyDependency( new DefaultSlaveFactory( logging, monitors,
                config.get( HaSettings.com_chunk_size ).intValue() ) );

        Slaves slaves = dependencies.satisfyDependency(
                life.add( new HighAvailabilitySlaves( members, clusterClient, slaveFactory ) ) );

        final TransactionPropagator pusher = life.add( new TransactionPropagator( TransactionPropagator.from( config ),
                msgLog, slaves, new CommitPusher( jobScheduler ) ) );

        return new CommitProcessFactory()
        {
            @Override
            public TransactionCommitProcess create( LogicalTransactionStore logicalTransactionStore,
                                                    KernelHealth kernelHealth, NeoStore neoStore,
                                                    TransactionRepresentationStoreApplier storeApplier,
                                                    NeoStoreInjectedTransactionValidator txValidator,
                                                    IndexUpdatesValidator indexUpdatesValidator, Config config )
            {
                if ( config.get( GraphDatabaseSettings.read_only ) )
                {
                    return defaultCommitProcessFactory.create( logicalTransactionStore, kernelHealth, neoStore,
                            storeApplier, txValidator, indexUpdatesValidator, config );
                }
                else
                {

                    TransactionCommitProcess inner =
                            defaultCommitProcessFactory.create( logicalTransactionStore, kernelHealth, neoStore,
                                    storeApplier, txValidator, indexUpdatesValidator, config );
                    new CommitProcessSwitcher( pusher, master, commitProcessDelegate, requestContextFactory,
                            memberStateMachine, txValidator, inner );

                    return (TransactionCommitProcess) Proxy
                            .newProxyInstance( TransactionCommitProcess.class.getClassLoader(),
                                    new Class[]{TransactionCommitProcess.class}, commitProcessDelegate );
                }
            }
        };
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        idGeneratorFactory = new HaIdGeneratorFactory( masterDelegateInvocationHandler, logging,
                requestContextFactory );

        ConsoleLogger consoleLog = logging.getConsoleLog( HighAvailabilityModeSwitcher.class );

        invalidEpochHandler = new InvalidEpochExceptionHandler()
        {
            @Override
            public void handle()
            {
                highAvailabilityModeSwitcher.forceElections();
            }
        };

        MasterClientResolver masterClientResolver = new MasterClientResolver( logging, responseUnpacker,
                invalidEpochHandler,
                config.get( HaSettings.read_timeout ).intValue(),
                config.get( HaSettings.lock_read_timeout ).intValue(),
                config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( HaSettings.com_chunk_size ).intValue() );

        SwitchToSlave switchToSlaveInstance = new SwitchToSlave( consoleLog, config, getDependencyResolver(),
                (HaIdGeneratorFactory) idGeneratorFactory,
                logging, masterDelegateInvocationHandler, clusterMemberAvailability,
                requestContextFactory, kernelExtensions.listFactories(), masterClientResolver,
                monitors.newMonitor( ByteCounterMonitor.class, SlaveServer.class ),
                monitors.newMonitor( RequestMonitor.class, SlaveServer.class ),
                monitors.newMonitor( SwitchToSlave.Monitor.class ),
                monitors.newMonitor( StoreCopyClient.Monitor.class ) );

        SwitchToMaster switchToMasterInstance = new SwitchToMaster( logging, consoleLog, this,
                (HaIdGeneratorFactory) idGeneratorFactory, config, dependencies.provideDependency( SlaveFactory.class ),
                masterDelegateInvocationHandler, clusterMemberAvailability, dataSourceManager,
                monitors.newMonitor( ByteCounterMonitor.class, MasterServer.class ),
                monitors.newMonitor( RequestMonitor.class, MasterServer.class ),
                monitors.newMonitor( MasterImpl.Monitor.class, MasterImpl.class ) );

        highAvailabilityModeSwitcher = new HighAvailabilityModeSwitcher( switchToSlaveInstance, switchToMasterInstance,
                clusterClient, clusterMemberAvailability, getDependencyResolver(), config.get( ClusterSettings.server_id ), logging );

        clusterClient.addBindingListener( highAvailabilityModeSwitcher );
        memberStateMachine.addHighAvailabilityMemberListener( highAvailabilityModeSwitcher );

        /*
         * We always need the mode switcher and we need it to restart on switchover.
         */
        paxosLife.add( highAvailabilityModeSwitcher );

        /*
         * We don't really switch to master here. We just need to initialize the idGenerator so the initial store
         * can be started (if required). In any case, the rest of the database is in pending state, so nothing will
         * happen until events start arriving and that will set us to the proper state anyway.
         */
        ((HaIdGeneratorFactory) idGeneratorFactory).switchToMaster();

        return idGeneratorFactory;
    }

    @Override
    protected Locks createLockManager()
    {
        DelegateInvocationHandler<Locks> lockManagerDelegate = new DelegateInvocationHandler<>( Locks.class );
        Locks lockManager = (Locks) Proxy.newProxyInstance(
                Locks.class.getClassLoader(), new Class[]{Locks.class}, lockManagerDelegate );
        new LockManagerModeSwitcher( memberStateMachine, lockManagerDelegate, masterDelegateInvocationHandler,
                requestContextFactory, availabilityGuard, config, new Factory<Locks>()
        {
            @Override
            public Locks newInstance()
            {
                return HighlyAvailableGraphDatabase.super.createLockManager();
            }
        } );
        return lockManager;
    }

    @Override
    protected TokenCreator createRelationshipTypeCreator()
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> relationshipTypeCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator relationshipTypeCreator =
                    (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                            new Class[]{TokenCreator.class}, relationshipTypeCreatorDelegate );

            new RelationshipTypeCreatorModeSwitcher( memberStateMachine, relationshipTypeCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );

            return relationshipTypeCreator;
        }
    }

    @Override
    protected TokenCreator createPropertyKeyCreator()
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> propertyKeyCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator propertyTokenCreator =
                    (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                            new Class[]{TokenCreator.class}, propertyKeyCreatorDelegate );
            new PropertyKeyCreatorModeSwitcher( memberStateMachine, propertyKeyCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );
            return propertyTokenCreator;
        }
    }

    @Override
    protected TokenCreator createLabelIdCreator()
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> labelIdCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator labelIdCreator =
                    (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                            new Class[]{TokenCreator.class}, labelIdCreatorDelegate );
            new LabelTokenCreatorModeSwitcher( memberStateMachine, labelIdCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );
            return labelIdCreator;
        }
    }

    @Override
    protected Caches createCaches()
    {
        return new HaCaches( logging.getMessagesLog( Caches.class ), monitors );
    }

    @Override
    protected KernelData createKernelData()
    {
        this.lastUpdateTime = new LastUpdateTime();
        OnDiskLastTxIdGetter txIdGetter = new OnDiskLastTxIdGetter( this );
        ClusterDatabaseInfoProvider databaseInfo = new ClusterDatabaseInfoProvider(
                members, txIdGetter, lastUpdateTime );
        return new HighlyAvailableKernelData( this, members, databaseInfo );
    }

    @Override
    protected void registerRecovery()
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
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_MASTER ) && event.getNewState().equals(
                        HighAvailabilityMemberState.MASTER ) )
                {
                    doAfterRecoveryAndStartup( true );
                }
            }

            @Override
            public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_SLAVE ) && event.getNewState().equals(
                        HighAvailabilityMemberState.SLAVE ) )
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
                    HighlyAvailableGraphDatabase.this.doAfterRecoveryAndStartup( isMaster );
                }
                catch ( Throwable throwable )
                {
                    msgLog.error( "Post recovery error", throwable );
                    try
                    {
                        memberStateMachine.stop();
                    }
                    catch ( Throwable throwable1 )
                    {
                        msgLog.warn( "Could not stop", throwable1 );
                    }
                    try
                    {
                        memberStateMachine.start();
                    }
                    catch ( Throwable throwable1 )
                    {
                        msgLog.warn( "Could not start", throwable1 );
                    }
                }
            }
        } );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + storeDir + "]";
    }

    public HighAvailabilityMemberState getInstanceState()
    {
        return memberStateMachine.getCurrentState();
    }

    public String role()
    {
        return members.getSelf().getHARole();
    }

    public boolean isMaster()
    {
        return memberStateMachine.getCurrentState() == HighAvailabilityMemberState.MASTER;
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return dependencies;
    }

    private static final class HAUpgradeConfiguration implements UpgradeConfiguration
    {
        @Override
        public void checkConfigurationAllowsAutomaticUpgrade()
        {
            throw new UpgradeNotAllowedByDatabaseModeException();
        }
    }

    /**
     * At end of startup, wait for instance to become either master or slave.
     * <p>
     * This helps users who expect to be able to access the instance after
     * the constructor is run.
     */
    private class StartupWaiter extends LifecycleAdapter
    {
        @Override
        public void start() throws Throwable
        {
            availabilityGuard.isAvailable( stateSwitchTimeoutMillis );
        }
    }
}
