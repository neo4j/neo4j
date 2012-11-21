/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.cluster.protocol.election.DefaultElectionCredentialsProvider;
import org.neo4j.com.ComSettings;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.HighlyAvailableKernelData;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.cluster.HighAvailabilityEvents;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilityMembers;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilitySlaves;
import org.neo4j.kernel.ha.cluster.paxos.PaxosHighAvailabilityEvents;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Logging;

import ch.qos.logback.classic.LoggerContext;

public class HighlyAvailableGraphDatabase extends InternalAbstractGraphDatabase
{
    private RequestContextFactory requestContextFactory;
    private Slaves slaves;
    private HighAvailabilityMembers members;
    private DelegateInvocationHandler delegateInvocationHandler;
    private LoggerContext loggerContext;
    private DefaultTransactionSupport transactionSupport;
    private Master master;
    private HighAvailabilityEvents clusterEvents;
    private InstanceAccessGuard accessGuard;
    private HighAvailabilityMemberStateMachine memberStateMachine;
    private UpdatePuller updatePuller;
    private HighAvailabilityMemberContext memberContext;
    private ClusterClient clusterClient;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> params,
                                         List<IndexProvider> indexProviders, List<KernelExtensionFactory<?>>
            kernelExtensions,
                                         List<CacheProvider> cacheProviders, List<TransactionInterceptorProvider>
            txInterceptorProviders )
    {
        super( storeDir, withDefaults( params ), indexProviders, kernelExtensions, cacheProviders,
                txInterceptorProviders );
        run();
    }

    protected void create()
    {
        life.add( new BranchedDataMigrator( new File( storeDir ) ) );
        delegateInvocationHandler = new DelegateInvocationHandler();
        master = (Master) Proxy.newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                delegateInvocationHandler );
        accessGuard = new InstanceAccessGuard();

        super.create();

        kernelEventHandlers.registerKernelEventHandler( new TxManagerCheckKernelEventHandler( xaDataSourceManager,
                (TxManager) txManager ) );
        life.add( memberStateMachine );
        life.add( updatePuller = new UpdatePuller( (HaXaDataSourceManager) xaDataSourceManager, master,
                requestContextFactory, txManager, accessGuard, config, msgLog ) );

        // Add this just before cluster join to ensure that it is up and running as late as possible
        // and is shut down as early as possible
        life.add( clusterClient );

        life.add( new StartupWaiter() );

        diagnosticsManager.appendProvider( new HighAvailabilityDiagnostics( memberStateMachine, clusterClient ) );
    }

    private static Map<String, String> withDefaults( Map<String, String> params )
    {
        return new ConfigurationDefaults( HaSettings.class, NetworkInstance.Configuration.class, ClusterSettings.class,
                ComSettings.class ).apply( params );
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
        accessGuard.await( 1000 * 60 );
        return super.beginTx( forceMode );
    }

    protected Logging createStringLogger()
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
        DefaultElectionCredentialsProvider electionCredentialsProvider = new DefaultElectionCredentialsProvider(
                config.get( HaSettings.server_id ), new OnDiskLastTxIdGetter( new File( getStoreDir() ) ) );
        // Add if to lifecycle later, as late as possible really
        clusterClient = new ClusterClient( ClusterClient.adapt( config, electionCredentialsProvider ), logging );

        clusterEvents = life.add( new PaxosHighAvailabilityEvents( PaxosHighAvailabilityEvents.adapt( config ),
                clusterClient,
                logging.getLogger( PaxosHighAvailabilityEvents.class ) ) );

        memberContext = new HighAvailabilityMemberContext( clusterClient );

        memberStateMachine = new HighAvailabilityMemberStateMachine( memberContext, accessGuard, clusterEvents,
                logging.getLogger( HighAvailabilityMemberStateMachine.class ) );
        life.add( new HighAvailabilityModeSwitcher( delegateInvocationHandler, clusterEvents, memberStateMachine, this,
                config, logging.getLogger( HighAvailabilityModeSwitcher.class ) ) );

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
        slaves = life.add( new HighAvailabilitySlaves( clusterClient, memberStateMachine, new DefaultSlaveFactory(
                xaDataSourceManager, msgLog, config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( ComSettings.com_chunk_size ) ), logging ) );
        members = new HighAvailabilityMembers( clusterClient, memberStateMachine );
        new TxIdGeneratorModeSwitcher( memberStateMachine, txIdGeneratorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, msgLog, config, slaves );
        return txIdGenerator;
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new HaIdGeneratorFactory( master, memberStateMachine );
    }

    @Override
    protected LockManager createLockManager()
    {
        // TransactionSupport piggy-backing on creating the lock manager
        transactionSupport = new DefaultTransactionSupport( txManager, txHook, accessGuard, config );

        DelegateInvocationHandler<LockManager> lockManagerDelegate = new DelegateInvocationHandler<LockManager>();
        LockManager lockManager =
                (LockManager) Proxy.newProxyInstance( LockManager.class.getClassLoader(),
                        new Class[]{LockManager.class}, lockManagerDelegate );
        new LockManagerModeSwitcher( memberStateMachine, lockManagerDelegate, txManager, txHook,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory, transactionSupport );
        return lockManager;
    }

    @Override
    protected RelationshipTypeCreator createRelationshipTypeCreator()
    {
        DelegateInvocationHandler<RelationshipTypeCreator> relationshipTypeCreatorDelegate =
                new DelegateInvocationHandler<RelationshipTypeCreator>();
        RelationshipTypeCreator relationshipTypeCreator =
                (RelationshipTypeCreator) Proxy.newProxyInstance( RelationshipTypeCreator.class.getClassLoader(),
                        new Class[]{RelationshipTypeCreator.class}, relationshipTypeCreatorDelegate );
        new RelationshipTypeCreatorModeSwitcher( memberStateMachine, relationshipTypeCreatorDelegate,
                (HaXaDataSourceManager) xaDataSourceManager, master, requestContextFactory );
        return relationshipTypeCreator;
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
        return new HighlyAvailableKernelData( this, members,
                new ClusterDatabaseInfoProvider( clusterClient, members ) );
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
                    doRecovery();
                }
            }

            @Override
            public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_SLAVE ) && event.getNewState().equals(
                        HighAvailabilityMemberState.SLAVE ) )
                {
                    doRecovery();
                }
            }

            @Override
            public void instanceStops( HighAvailabilityMemberChangeEvent event )
            {
            }

            private void doRecovery()
            {
                try
                {
                    synchronized ( xaDataSourceManager )
                    {
                        HighlyAvailableGraphDatabase.this.doRecovery();
                    }
                }
                catch ( Throwable throwable )
                {
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
        return getClass().getSimpleName() + "[" + 0 + ", " + storeDir + "]";
    }

    public String getInstanceState()
    {
        return memberStateMachine.getCurrentState().name();
    }

    public long lastUpdateTime()
    {
        //TODO implement this as a transaction interceptor
        return 0;
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
                    if ( HighAvailabilityEvents.class.isAssignableFrom( type ) )
                    {
                        result = (T) clusterEvents;
                    }
                    else if ( UpdatePuller.class.isAssignableFrom( type ) )
                    {
                        result = (T) updatePuller;
                    }
                    else if ( Slaves.class.isAssignableFrom( type ) )
                    {
                        result = (T) slaves;
                    }
                    else if ( ClusterClient.class.isAssignableFrom( type ) )
                    {
                        result = (T) clusterClient;
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
            accessGuard.await( 10000 );
        }
    }
}
