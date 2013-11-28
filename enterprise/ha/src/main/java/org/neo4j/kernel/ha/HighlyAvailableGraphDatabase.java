/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Map;
import javax.transaction.Transaction;

import org.jboss.netty.logging.InternalLoggerFactory;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.com.BindingNotifier;
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
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Factory;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
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
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilitySlaves;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.DefaultSlaveFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.lock.LockManagerModeSwitcher;
import org.neo4j.kernel.ha.management.ClusterDatabaseInfoProvider;
import org.neo4j.kernel.ha.management.HighlyAvailableKernelData;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.ha.transaction.TxHookModeSwitcher;
import org.neo4j.kernel.ha.transaction.TxIdGeneratorModeSwitcher;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.WritableTransactionState;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.LogbackWeakDependency;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.kernel.ha.DelegateInvocationHandler.snapshot;
import static org.neo4j.kernel.impl.transaction.XidImpl.DEFAULT_SEED;
import static org.neo4j.kernel.impl.transaction.XidImpl.getNewGlobalId;
import static org.neo4j.kernel.logging.LogbackWeakDependency.DEFAULT_TO_CLASSIC;
import static org.neo4j.kernel.logging.LogbackWeakDependency.NEW_LOGGER_CONTEXT;

public class HighlyAvailableGraphDatabase extends InternalAbstractGraphDatabase
{
    private RequestContextFactory requestContextFactory;
    private Slaves slaves;
    private ClusterMembers members;
    private DelegateInvocationHandler masterDelegateInvocationHandler;
    private Master master;
    private HighAvailabilityMemberStateMachine memberStateMachine;
    private UpdatePuller updatePuller;
    private LastUpdateTime lastUpdateTime;
    private HighAvailabilityMemberContext memberContext;
    private ClusterClient clusterClient;
    private ClusterMemberEvents clusterEvents;
    private ClusterMemberAvailability clusterMemberAvailability;
    private long stateSwitchTimeoutMillis;

    private final LifeSupport paxosLife = new LifeSupport();

    private DelegateInvocationHandler clusterEventsDelegateInvocationHandler;
    private DelegateInvocationHandler memberContextDelegateInvocationHandler;
    private DelegateInvocationHandler clusterMemberAvailabilityDelegateInvocationHandler;
    private HighAvailabilityModeSwitcher highAvailabilityModeSwitcher;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                         Iterable<CacheProvider> cacheProviders,
                                         Iterable<TransactionInterceptorProvider> txInterceptorProviders )
    {
        super( storeDir, params,
                Iterables.<Class<?>,Class<?>>iterable( GraphDatabaseSettings.class, HaSettings.class,ClusterSettings.class ),
                kernelExtensions,
                cacheProviders, txInterceptorProviders );
        run();
    }

    @Override
    protected void create()
    {
        life.add( new BranchedDataMigrator( storeDir ) );
        masterDelegateInvocationHandler = new DelegateInvocationHandler();
        master = (Master) Proxy.newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                masterDelegateInvocationHandler );

        super.create();

        kernelEventHandlers.registerKernelEventHandler( new HaKernelPanicHandler( xaDataSourceManager,
                (TxManager) txManager, availabilityGuard ) );
        life.add( updatePuller = new UpdatePuller( (HaXaDataSourceManager) xaDataSourceManager, master,
                requestContextFactory, txManager, availabilityGuard, lastUpdateTime, config, msgLog ) );

        stateSwitchTimeoutMillis = config.get( HaSettings.state_switch_timeout );

        life.add( paxosLife );

        life.add( new DatabaseAvailability( txManager, availabilityGuard ) );

        life.add( new StartupWaiter() );

        diagnosticsManager.appendProvider( new HighAvailabilityDiagnostics( memberStateMachine, clusterClient ) );
    }

    @Override
    protected AvailabilityGuard createAvailabilityGuard()
    {
        // 3 conditions: DatabaseAvailability, HighAvailabilityMemberStateMachine, and HA Kernel Panic
        return new AvailabilityGuard( Clock.SYSTEM_CLOCK, 3 );
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
    protected org.neo4j.graphdb.Transaction beginTx( ForceMode forceMode )
    {
        // TODO first startup ever we don't have a proper db, so don't even serve read requests
        // if this is a startup for where we have been a member of this cluster before we
        // can server (possibly quite outdated) read requests.
        if (!availabilityGuard.isAvailable( stateSwitchTimeoutMillis ))
        {
            throw new TransactionFailureException( "Timeout waiting for database to allow new transactions. "
                    + availabilityGuard.describeWhoIsBlocking() );
        }

        return super.beginTx( forceMode );
    }

    @Override
    protected Logging createLogging()
    {
        Logging loggingService = life.add( new LogbackWeakDependency().tryLoadLogbackService( config, NEW_LOGGER_CONTEXT,
                DEFAULT_TO_CLASSIC ) );

        // Set Netty logger
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( loggingService ) );

        return loggingService;
    }

    @Override
    protected TransactionStateFactory createTransactionStateFactory()
    {
        return new TransactionStateFactory( logging )
        {
            @Override
            public TransactionState create( Transaction tx )
            {
                return new WritableTransactionState( snapshot( lockManager ),
                        nodeManager, logging, tx, snapshot( txHook ),
                        snapshot( txIdGenerator ) );
            }
        };
    }

    @Override
    protected XaDataSourceManager createXaDataSourceManager()
    {
        XaDataSourceManager toReturn = new HaXaDataSourceManager( logging.getMessagesLog( HaXaDataSourceManager.class
        ) );
        requestContextFactory = new RequestContextFactory( config.get( ClusterSettings.server_id ), toReturn,
                dependencyResolver );
        return toReturn;
    }

    @Override
    protected RemoteTxHook createTxHook()
    {
        clusterEventsDelegateInvocationHandler = new DelegateInvocationHandler();
        memberContextDelegateInvocationHandler = new DelegateInvocationHandler();
        clusterMemberAvailabilityDelegateInvocationHandler = new DelegateInvocationHandler();

        clusterEvents = (ClusterMemberEvents) Proxy.newProxyInstance( ClusterMemberEvents.class.getClassLoader(),
                new Class[]{ClusterMemberEvents.class, Lifecycle.class}, clusterEventsDelegateInvocationHandler );
        memberContext = (HighAvailabilityMemberContext) Proxy.newProxyInstance(
                HighAvailabilityMemberContext.class.getClassLoader(),
                new Class[]{HighAvailabilityMemberContext.class}, memberContextDelegateInvocationHandler );
        clusterMemberAvailability = (ClusterMemberAvailability) Proxy.newProxyInstance(
                ClusterMemberAvailability.class.getClassLoader(),
                new Class[]{ClusterMemberAvailability.class}, clusterMemberAvailabilityDelegateInvocationHandler );

        ElectionCredentialsProvider electionCredentialsProvider = config.get( HaSettings.slave_only ) ?
                new NotElectableElectionCredentialsProvider() :
                new DefaultElectionCredentialsProvider( config.get( ClusterSettings.server_id ),
                        new OnDiskLastTxIdGetter( new File( getStoreDir() ) ), new HighAvailabilityMemberInfoProvider()
                {
                    @Override
                    public HighAvailabilityMemberState getHighAvailabilityMemberState()
                    {
                        return memberStateMachine.getCurrentState();
                    }
                } );


        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();


        clusterClient = new ClusterClient( ClusterClient.adapt( config ), logging, electionCredentialsProvider,
                objectStreamFactory, objectStreamFactory );
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
                        if ( HighAvailabilityModeSwitcher.getServerId( member.getRoleUri() ) ==
                                config.get( ClusterSettings.server_id ) )
                        {
                            msgLog.error( String.format( "Instance %s has the same serverId as ours (%d) - will not " +
                                    "join this cluster",
                                    member.getRoleUri(), config.get( ClusterSettings.server_id ) ) );
                            return true;
                        }
                    }
                }
                return true;
            }
        }, new HANewSnapshotFunction(), objectStreamFactory, objectStreamFactory );

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
                .getServerId() );
        PaxosClusterMemberAvailability localClusterMemberAvailability = new PaxosClusterMemberAvailability(
                clusterClient.getServerId(), clusterClient, clusterClient, logging, objectStreamFactory,
                objectStreamFactory );

        memberContextDelegateInvocationHandler.setDelegate( localMemberContext );
        clusterEventsDelegateInvocationHandler.setDelegate( localClusterEvents );
        clusterMemberAvailabilityDelegateInvocationHandler.setDelegate( localClusterMemberAvailability );

        members = new ClusterMembers( clusterClient, clusterClient, clusterEvents,
                new InstanceId( config.get( ClusterSettings.server_id ) ) );
        memberStateMachine = new HighAvailabilityMemberStateMachine( memberContext, availabilityGuard, members,
                clusterEvents,
                clusterClient, logging.getMessagesLog( HighAvailabilityMemberStateMachine.class ) );

        HighAvailabilityConsoleLogger highAvailabilityConsoleLogger = new HighAvailabilityConsoleLogger( logging
                .getConsoleLog( HighAvailabilityConsoleLogger.class ), new InstanceId( config.get( ClusterSettings
                .server_id ) ) );
        availabilityGuard.addListener( highAvailabilityConsoleLogger );
        clusterEvents.addClusterMemberListener( highAvailabilityConsoleLogger );
        clusterClient.addClusterListener( highAvailabilityConsoleLogger );

        paxosLife.add( clusterClient );
        paxosLife.add( memberStateMachine );
        paxosLife.add( clusterEvents );
        paxosLife.add( localClusterMemberAvailability );

        DelegateInvocationHandler<RemoteTxHook> txHookDelegate = new DelegateInvocationHandler<>();
        RemoteTxHook txHook = (RemoteTxHook) Proxy.newProxyInstance( RemoteTxHook.class.getClassLoader(), new Class[]{RemoteTxHook.class},
                txHookDelegate );
        new TxHookModeSwitcher( memberStateMachine, txHookDelegate,
                master, new TxHookModeSwitcher.RequestContextFactoryResolver()
        {
            @Override
            public RequestContextFactory get()
            {
                return requestContextFactory;
            }
        }, logging.getMessagesLog( TxHookModeSwitcher.class ), dependencyResolver );
        return txHook;
    }

    @Override
    public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
    {
        if (!isMaster())
        {
            throw new InvalidTransactionTypeKernelException(
                    "Modifying the database schema can only be done on the master server, " +
                            "this server is a slave. Please issue schema modification commands directly to the master." );
        }
    }

    @Override
    protected TxIdGenerator createTxIdGenerator()
    {
        DelegateInvocationHandler<TxIdGenerator> txIdGeneratorDelegate = new DelegateInvocationHandler<>();
        TxIdGenerator txIdGenerator =
                (TxIdGenerator) Proxy.newProxyInstance( TxIdGenerator.class.getClassLoader(),
                        new Class[]{TxIdGenerator.class}, txIdGeneratorDelegate );
        slaves = life.add( new HighAvailabilitySlaves( members, clusterClient, new DefaultSlaveFactory(
                xaDataSourceManager, logging, config.get( HaSettings.com_chunk_size ).intValue() ) ) );

        new TxIdGeneratorModeSwitcher( memberStateMachine, txIdGeneratorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, msgLog, config, slaves,
                txManager );
        return txIdGenerator;
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        idGeneratorFactory = new HaIdGeneratorFactory( master, logging );
        highAvailabilityModeSwitcher = new HighAvailabilityModeSwitcher( clusterClient, masterDelegateInvocationHandler,
                clusterMemberAvailability, memberStateMachine, this, (HaIdGeneratorFactory) idGeneratorFactory,
                config, logging, updateableSchemaState, kernelExtensions.listFactories(), monitors );

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
    protected LockManager createLockManager()
    {
        DelegateInvocationHandler<LockManager> lockManagerDelegate = new DelegateInvocationHandler<>();
        LockManager lockManager =
                (LockManager) Proxy.newProxyInstance( LockManager.class.getClassLoader(),
                        new Class[]{LockManager.class}, lockManagerDelegate );
        new LockManagerModeSwitcher( memberStateMachine, lockManagerDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, txManager, txHook,
                availabilityGuard, config );
        return lockManager;
    }

    @Override
    protected TokenCreator createRelationshipTypeCreator()
    {
        DelegateInvocationHandler<TokenCreator> relationshipTypeCreatorDelegate =
                new DelegateInvocationHandler<>();
        TokenCreator relationshipTypeCreator =
                (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                        new Class[]{TokenCreator.class}, relationshipTypeCreatorDelegate );
        new RelationshipTypeCreatorModeSwitcher( memberStateMachine, relationshipTypeCreatorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, logging );
        return relationshipTypeCreator;
    }

    @Override
    protected TokenCreator createPropertyKeyCreator()
    {
        DelegateInvocationHandler<TokenCreator> propertyKeyCreatorDelegate =
                new DelegateInvocationHandler<>();
        TokenCreator propertyTokenCreator =
                (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                        new Class[]{TokenCreator.class}, propertyKeyCreatorDelegate );
        new PropertyKeyCreatorModeSwitcher( memberStateMachine, propertyKeyCreatorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, logging );
        return propertyTokenCreator;
    }

    @Override
    protected TokenCreator createLabelIdCreator()
    {
        DelegateInvocationHandler<TokenCreator> labelIdCreatorDelegate =
                new DelegateInvocationHandler<>();
        TokenCreator labelIdCreator =
                (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                        new Class[]{TokenCreator.class}, labelIdCreatorDelegate );
        new LabelTokenCreatorModeSwitcher( memberStateMachine, labelIdCreatorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, logging );
        return labelIdCreator;
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
        return new HighlyAvailableKernelData( this, members,
                new ClusterDatabaseInfoProvider( members, new OnDiskLastTxIdGetter( new File( getStoreDir() ) ),
                        lastUpdateTime ) );
    }
    
    @Override
    protected Factory<byte[]> createXidGlobalIdFactory()
    {
        final int serverId = config.get( ClusterSettings.server_id );
        return new Factory<byte[]>()
        {
            @Override
            public byte[] newInstance()
            {
                return getNewGlobalId( DEFAULT_SEED, serverId );
            }
        };
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
                    synchronized ( xaDataSourceManager )
                    {
                        HighlyAvailableGraphDatabase.this.doAfterRecoveryAndStartup( isMaster );
                    }
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

    public String getInstanceState()
    {
        return memberStateMachine.getCurrentState().name();
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
        return new DependencyResolver.Adapter()
        {
            @Override
            public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
            {
                T result;
                try
                {
                    result = dependencyResolver.resolveDependency( type, selector );
                }
                catch ( IllegalArgumentException e )
                {
                    if ( ClusterMemberEvents.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( clusterEvents );
                    }
                    else if ( ClusterMemberAvailability.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( clusterMemberAvailability );
                    }
                    else if ( UpdatePuller.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( updatePuller );
                    }
                    else if ( Slaves.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( slaves );
                    }
                    else if ( ClusterClient.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( clusterClient );
                    }
                    else if ( BindingNotifier.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( clusterClient );
                    }
                    else if ( ClusterMembers.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( members );
                    }
                    else if ( RequestContextFactory.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( requestContextFactory );
                    }
                    else
                    {
                        throw e;
                    }
                }
                return selector.select( type, option( result ) );
            }
        };
    }

    /**
     * At end of startup, wait for instance to become either master or slave.
     * <p/>
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
