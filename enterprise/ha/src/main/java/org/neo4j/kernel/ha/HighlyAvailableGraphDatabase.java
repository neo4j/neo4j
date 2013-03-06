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

import static org.neo4j.kernel.ha.DelegateInvocationHandler.snapshot;

import java.io.File;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberAvailability;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.DefaultElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.SimpleHighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilitySlaves;
import org.neo4j.kernel.ha.cluster.zoo.ZooKeeperHighAvailabilityEvents;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.DefaultSlaveFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.lock.LockManagerModeSwitcher;
import org.neo4j.kernel.ha.management.ClusterDatabaseInfoProvider;
import org.neo4j.kernel.ha.management.HighlyAvailableKernelData;
import org.neo4j.kernel.ha.switchover.Switchover;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.ha.transaction.TxHookModeSwitcher;
import org.neo4j.kernel.ha.transaction.TxIdGeneratorModeSwitcher;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.KeyCreator;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.WritableTransactionState;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Logging;

import ch.qos.logback.classic.LoggerContext;

public class HighlyAvailableGraphDatabase extends InternalAbstractGraphDatabase
{
    private RequestContextFactory requestContextFactory;
    private Slaves slaves;
    private ClusterMembers members;
    private DelegateInvocationHandler masterDelegateInvocationHandler;
    private LoggerContext loggerContext;
    private Master master;
    private InstanceAccessGuard accessGuard;
    private HighAvailabilityMemberStateMachine memberStateMachine;
    private UpdatePuller updatePuller;
    private LastUpdateTime lastUpdateTime;
    private HighAvailabilityMemberContext memberContext;
    private ClusterClient clusterClient;
    private ClusterMemberEvents clusterEvents;
    private ClusterMemberAvailability clusterMemberAvailability;
    private long stateSwitchTimeoutMillis;

    /*
     * TODO the following are in place of a proper abstraction of component dependencies, in which the compatibility
     * layer would be an optional component and the paxos layer would depend on it. Since we currently don't have one,
     * we need to fake it with this life and the accompanying boolean.
     */
    /*
     * paxosLife holds stuff that must be added in global life if we are not in compatibility mode. If in compatibility
     * mode they will be started only on switchover.
     */
     private final LifeSupport paxosLife = new LifeSupport();
    /*
     * compatibilityMode is true if we are in ZK compatibility mode. If false, paxosLife is added to the global life.
     */
    private boolean compatibilityMode = false;
    /*
     * compatibilityLifecycle holds stuff that needs to be shutdown when switching. They can be restarted by adding
      * them to paxosLife too.
     */
    List<Lifecycle> compatibilityLifecycle = new LinkedList<Lifecycle>();
    private DelegateInvocationHandler clusterEventsDelegateInvocationHandler;
    private DelegateInvocationHandler memberContextDelegateInvocationHandler;
    private DelegateInvocationHandler clusterMemberAvailabilityDelegateInvocationHandler;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> params,
                                         Iterable<IndexProvider> indexProviders,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                         Iterable<CacheProvider> cacheProviders,
                                         Iterable<TransactionInterceptorProvider> txInterceptorProviders,
                                         Iterable<SchemaIndexProvider> schemaIndexProvider )
    {
        super( storeDir, params, Iterables.<Class<?>,Class<?>>iterable( GraphDatabaseSettings.class, HaSettings.class,
                NetworkInstance.Configuration.class, ClusterSettings.class ), indexProviders, kernelExtensions,
                cacheProviders, txInterceptorProviders, schemaIndexProvider );
        run();
    }

    @Override
    protected void create()
    {
        life.add( new BranchedDataMigrator( storeDir ) );
        masterDelegateInvocationHandler = new DelegateInvocationHandler();
        master = (Master) Proxy.newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                masterDelegateInvocationHandler );
        accessGuard = new InstanceAccessGuard();

        super.create();

        kernelEventHandlers.registerKernelEventHandler( new HaKernelPanicHandler( xaDataSourceManager,
                (TxManager) txManager ) );
        life.add( updatePuller = new UpdatePuller( (HaXaDataSourceManager) xaDataSourceManager, master,
                requestContextFactory, txManager, accessGuard, lastUpdateTime, config, msgLog ) );

        stateSwitchTimeoutMillis = config.get( HaSettings.state_switch_timeout );
        if ( !compatibilityMode )
        {
            life.add( paxosLife );
        }

        life.add( new StartupWaiter() );

        diagnosticsManager.appendProvider( new HighAvailabilityDiagnostics( memberStateMachine, clusterClient ) );
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
        if (!accessGuard.await( stateSwitchTimeoutMillis ))
        {
            throw new TransactionFailureException( "Timeout waiting for cluster to elect master" );
        }

        return super.beginTx( forceMode );
    }

    @Override
    protected Logging createLogging()
    {
        try
        {
            getClass().getClassLoader().loadClass( "ch.qos.logback.classic.LoggerContext" );
            loggerContext = new LoggerContext();
            return life.add( new LogbackService( config, loggerContext ) );
        }
        catch ( ClassNotFoundException e )
        {
            return life.add( new ClassicLoggingService( config ) );
        }
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
        XaDataSourceManager toReturn = new HaXaDataSourceManager( logging.getLogger( HaXaDataSourceManager.class ) );
        requestContextFactory = new RequestContextFactory( config.get( HaSettings.server_id ), toReturn,
                dependencyResolver );
        return toReturn;
    }

    @Override
    protected TxHook createTxHook()
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

        /*
         *  We need to create these anyway since even in compatibility mode we'll use them for switchover. If it turns
         *  out we are not going to need zookeeper, just assign them to the class fields. The difference is in when
         *  they start().
         */
        ElectionCredentialsProvider electionCredentialsProvider = config.get( HaSettings.slave_only ) ?
                new NotElectableElectionCredentialsProvider() :
                new DefaultElectionCredentialsProvider(config.get( HaSettings.server_id ), new OnDiskLastTxIdGetter( new File( getStoreDir() ) ) );

        clusterClient = new ClusterClient( ClusterClient.adapt( config ), logging, electionCredentialsProvider );
        PaxosClusterMemberEvents localClusterEvents = new PaxosClusterMemberEvents( clusterClient, clusterClient,
            clusterClient, logging, new Predicate<PaxosClusterMemberEvents.ClusterMembersSnapshot>()
        {
            @Override
            public boolean accept( PaxosClusterMemberEvents.ClusterMembersSnapshot item )
            {
                for ( MemberIsAvailable member : item.getCurrentAvailableMembers() )
                {
                    if ( member.getRoleUri().getScheme().equals( "ha" ) )
                    {
                        if ( HighAvailabilityModeSwitcher.getServerId( member.getRoleUri() ) ==
                                config.get( HaSettings.server_id ) )
                        {
                            msgLog.error( String.format( "Instance %s has the same serverId as ours (%d) - will not join this cluster",
                                    member.getRoleUri(), config.get( HaSettings.server_id ) ) );
                            return false;
                        }
                    }
                }
                return true;
            }
        } );

        // Force a reelection after we enter the cluster
        // and when that election is finished refresh the snapshot
        clusterClient.addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                clusterClient.performRoleElections();
            }

            @Override
            public void elected( String role, URI electedMember )
            {
                if (role.equals( ClusterConfiguration.COORDINATOR ))
                {
                    clusterClient.refreshSnapshot();
                    clusterClient.removeClusterListener( this );
                }
            }
        });

        HighAvailabilityMemberContext localMemberContext = new SimpleHighAvailabilityMemberContext( clusterClient );
        PaxosClusterMemberAvailability localClusterMemberAvailability = new PaxosClusterMemberAvailability(
            clusterClient, clusterClient, logging );

        // Here we decide whether to start in compatibility mode or mode or not
        if ( !config.get( HaSettings.coordinators ).isEmpty() &&
            !config.get( HaSettings.coordinators ).get( 0 ).toString().trim().equals( "" ) )
        {
            compatibilityMode = true;
            compatibilityLifecycle = new LinkedList<Lifecycle>();

            Switchover switchover = new ZooToPaxosSwitchover( life, paxosLife, compatibilityLifecycle,
                clusterEventsDelegateInvocationHandler, memberContextDelegateInvocationHandler,
                clusterMemberAvailabilityDelegateInvocationHandler, localClusterEvents,
                localMemberContext, localClusterMemberAvailability );

            ZooKeeperHighAvailabilityEvents zkEvents =
                new ZooKeeperHighAvailabilityEvents( logging, config, switchover );
            compatibilityLifecycle.add( zkEvents );
            memberContextDelegateInvocationHandler.setDelegate(
                    new SimpleHighAvailabilityMemberContext( zkEvents ) );
            clusterEventsDelegateInvocationHandler.setDelegate( zkEvents );
            clusterMemberAvailabilityDelegateInvocationHandler.setDelegate( zkEvents );
            // Paxos Events added to life, won't be stopped because it isn't started yet
            paxosLife.add( localClusterEvents );
        }
        else
        {
            memberContextDelegateInvocationHandler.setDelegate( localMemberContext );
            clusterEventsDelegateInvocationHandler.setDelegate( localClusterEvents );
            clusterMemberAvailabilityDelegateInvocationHandler.setDelegate( localClusterMemberAvailability );
        }

        memberStateMachine = new HighAvailabilityMemberStateMachine( memberContext, accessGuard, clusterEvents,
            logging.getLogger( HighAvailabilityMemberStateMachine.class ) );

        if ( compatibilityMode )
        {
            /*
             * In here goes stuff that needs to stop when switching. If added in paxosLife too they will be restarted.
             * Adding to life starts them when life.start is called - adding them to compatibilityLifeCycle shuts them
             * down on switchover
             */
            compatibilityLifecycle.add( memberStateMachine );
//            compatibilityLifecycle.add( highAvailabilityModeSwitcher );
            compatibilityLifecycle.add( (Lifecycle) clusterEvents );
            life.add( memberStateMachine );
//            life.add( highAvailabilityModeSwitcher );
            life.add( clusterEvents );
        }
        /*
        * Here goes stuff that needs to start when paxos kicks in:
        * In Normal (non compatibility mode): That means they start normally
        * In Compatibility Mode: That means they start when switchover happens. If added to life too they will be
        * restarted
        */
        paxosLife.add( memberStateMachine );
        paxosLife.add( clusterEvents );
        // highAvailabilityModeSwitcher left for reference, has been moved to createTxIdGenerator
//        paxosLife.add( highAvailabilityModeSwitcher );
        paxosLife.add( clusterClient );
        paxosLife.add( localClusterMemberAvailability );

        DelegateInvocationHandler<TxHook> txHookDelegate = new DelegateInvocationHandler<TxHook>();
        TxHook txHook = (TxHook) Proxy.newProxyInstance( TxHook.class.getClassLoader(), new Class[]{TxHook.class},
                txHookDelegate );
        new TxHookModeSwitcher( memberStateMachine, txHookDelegate,
                master, new TxHookModeSwitcher.RequestContextFactoryResolver()
        {
            @Override
            public RequestContextFactory get()
            {
                return requestContextFactory;
            }
        }, dependencyResolver );
        return txHook;
    }

    @Override
    protected TxIdGenerator createTxIdGenerator()
    {
        DelegateInvocationHandler<TxIdGenerator> txIdGeneratorDelegate = new DelegateInvocationHandler<TxIdGenerator>();
        TxIdGenerator txIdGenerator =
                (TxIdGenerator) Proxy.newProxyInstance( TxIdGenerator.class.getClassLoader(),
                        new Class[]{TxIdGenerator.class}, txIdGeneratorDelegate );
        members = new ClusterMembers( clusterClient, clusterClient, clusterClient, clusterEvents );
        slaves = life.add( new HighAvailabilitySlaves( members, clusterClient, new DefaultSlaveFactory(
                xaDataSourceManager, logging, config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( HaSettings.com_chunk_size ).intValue() ) ) );

        new TxIdGeneratorModeSwitcher( memberStateMachine, txIdGeneratorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, msgLog, config, slaves );
        return txIdGenerator;
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {

        idGeneratorFactory = new HaIdGeneratorFactory( master, memberStateMachine, logging );
        HighAvailabilityModeSwitcher highAvailabilityModeSwitcher = new HighAvailabilityModeSwitcher( masterDelegateInvocationHandler,
                clusterMemberAvailability, memberStateMachine, this, (HaIdGeneratorFactory) idGeneratorFactory, config,
                logging );
        /*
        * We always need the mode switcher and we need it to restart on switchover. So:
        * 1) if in compatibility mode, it must be added in all 3 - to start on start and restart on switchover
        * 2) if not in compatibility mode it must be added in paxosLife, which is started anyway.
        */
        paxosLife.add( highAvailabilityModeSwitcher );
        if ( compatibilityMode )
        {
            compatibilityLifecycle.add( 1, highAvailabilityModeSwitcher );
            life.add( highAvailabilityModeSwitcher );
        }

        return idGeneratorFactory;
    }

    @Override
    protected LockManager createLockManager()
    {
        DelegateInvocationHandler<LockManager> lockManagerDelegate = new DelegateInvocationHandler<LockManager>();
        LockManager lockManager =
                (LockManager) Proxy.newProxyInstance( LockManager.class.getClassLoader(),
                        new Class[]{LockManager.class}, lockManagerDelegate );
        new LockManagerModeSwitcher( memberStateMachine, lockManagerDelegate, txManager, txHook,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, accessGuard, config );
        return lockManager;
    }

    @Override
    protected KeyCreator createRelationshipTypeCreator()
    {
        DelegateInvocationHandler<KeyCreator> relationshipTypeCreatorDelegate =
                new DelegateInvocationHandler<KeyCreator>();
        KeyCreator relationshipTypeCreator =
                (KeyCreator) Proxy.newProxyInstance( KeyCreator.class.getClassLoader(),
                        new Class[]{KeyCreator.class}, relationshipTypeCreatorDelegate );
        new RelationshipTypeCreatorModeSwitcher( memberStateMachine, relationshipTypeCreatorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, logging );
        return relationshipTypeCreator;
    }

    @Override
    protected KeyCreator createPropertyKeyCreator()
    {
        DelegateInvocationHandler<KeyCreator> propertyKeyCreatorDelegate =
                new DelegateInvocationHandler<KeyCreator>();
        KeyCreator propertyKeyCreator =
                (KeyCreator) Proxy.newProxyInstance( KeyCreator.class.getClassLoader(),
                        new Class[]{KeyCreator.class}, propertyKeyCreatorDelegate );
        new PropertyKeyCreatorModeSwitcher( memberStateMachine, propertyKeyCreatorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, logging );
        return propertyKeyCreator;
    }
    
    @Override
    protected Caches createCaches()
    {
        return new HaCaches( msgLog );
    }

    @Override
    protected void createNeoDataSource()
    {
        // no op, we must wait to join the cluster to do stuff
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
                    doAfterRecoveryAndStartup();
                }
            }

            @Override
            public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_SLAVE ) && event.getNewState().equals(
                        HighAvailabilityMemberState.SLAVE ) )
                {
                    doAfterRecoveryAndStartup();
                }
            }

            @Override
            public void instanceStops( HighAvailabilityMemberChangeEvent event )
            {
            }

            private void doAfterRecoveryAndStartup()
            {
                try
                {
                    synchronized ( xaDataSourceManager )
                    {
                        HighlyAvailableGraphDatabase.this.doAfterRecoveryAndStartup();
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

    public boolean isMaster()
    {
        return memberStateMachine.getCurrentState() == HighAvailabilityMemberState.MASTER;
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return new DependencyResolver()
        {
            @Override
            public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
            {
                T result;
                try
                {
                    result = dependencyResolver.resolveDependency( type );
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
                    else if ( ClusterMembers.class.isAssignableFrom( type ) )
                    {
                        result = type.cast( members );
                    }
                    else
                    {
                        throw e;
                    }
                }
                return result;
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
            accessGuard.await( stateSwitchTimeoutMillis );
        }
    }

}
