/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.transaction.TransactionManager;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.com.BindingNotifier;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchDetectingTxVerifier;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.SlaveStoreWriter;
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveImpl;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Uris.parameter;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.isStorePresent;

/**
 * Performs the internal switches from pending to slave/master, by listening for
 * ClusterMemberChangeEvents. When finished it will invoke {@link org.neo4j.cluster.member.ClusterMemberAvailability#memberIsAvailable(String, URI)} to announce
 * to the cluster it's new status.
 */
public class HighAvailabilityModeSwitcher implements HighAvailabilityMemberListener, Lifecycle
{
    // TODO solve this with lifecycle instance grouping or something
    @SuppressWarnings( "rawtypes" )
    private static final Class[] SERVICES_TO_RESTART_FOR_STORE_COPY = new Class[] {
            StoreLockerLifecycleAdapter.class,
            XaDataSourceManager.class,
            TransactionManager.class,
            NodeManager.class,
            IndexStore.class
    };

    public static final String MASTER = "master";
    public static final String SLAVE = "slave";
    private URI masterHaURI;
    public static final String INADDR_ANY = "0.0.0.0";
    private URI slaveHaURI;
    private BindingListener bindingListener;

    public static int getServerId( URI haUri )
    {
        // Get serverId parameter, default to -1 if it is missing, and parse to integer
        return INTEGER.apply( withDefaults(
                Functions.<URI, String>constant( "-1" ), parameter( "serverId" ) ).apply( haUri ));
    }

    private URI availableMasterId;

    private final HighAvailabilityMemberStateMachine stateHandler;
    private final BindingNotifier bindingNotifier;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final GraphDatabaseAPI graphDb;
    private final Config config;
    private LifeSupport haCommunicationLife;
    private final StringLogger msgLog;
    private final ConsoleLogger console;

    private final HaIdGeneratorFactory idGeneratorFactory;
    private final Logging logging;

    private final UpdateableSchemaState updateableSchemaState;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final Monitors monitors;
    private final RequestContextFactory requestContextFactory;
    private MasterClientResolver masterClientResolver;

    private ScheduledExecutorService scheduledExecutorService;

    private volatile URI me;

    public HighAvailabilityModeSwitcher( BindingNotifier bindingNotifier,
                                         DelegateInvocationHandler<Master> delegateHandler,
                                         ClusterMemberAvailability clusterMemberAvailability,
                                         HighAvailabilityMemberStateMachine stateHandler, GraphDatabaseAPI graphDb,
                                         HaIdGeneratorFactory idGeneratorFactory, Config config, Logging logging,
                                         UpdateableSchemaState updateableSchemaState,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions, Monitors monitors,
                                         RequestContextFactory requestContextFactory )
    {
        this.bindingNotifier = bindingNotifier;
        this.masterDelegateHandler = delegateHandler;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.graphDb = graphDb;
        this.idGeneratorFactory = idGeneratorFactory;
        this.config = config;
        this.logging = logging;
        this.updateableSchemaState = updateableSchemaState;
        this.kernelExtensions = kernelExtensions;
        this.monitors = monitors;
        this.requestContextFactory = requestContextFactory;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.haCommunicationLife = new LifeSupport();
        this.stateHandler = stateHandler;

        this.console = logging.getConsoleLog( getClass() );
    }

    @Override
    public synchronized void init() throws Throwable
    {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(  );

        stateHandler.addHighAvailabilityMemberListener( this );
        bindingListener = new BindingListener()
        {
            @Override
            public void listeningAt( URI myUri )
            {
                me = myUri;
            }
        };
        bindingNotifier.addBindingListener( bindingListener );
        haCommunicationLife.init();
    }

    @Override
    public synchronized void start() throws Throwable
    {
        haCommunicationLife.start();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        haCommunicationLife.stop();
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        stateHandler.removeHighAvailabilityMemberListener( this );
        bindingNotifier.removeBindingListener( bindingListener );

        scheduledExecutorService.shutdown();

        scheduledExecutorService.awaitTermination( 60, TimeUnit.SECONDS );

        haCommunicationLife.shutdown();
    }

    @Override
    public void masterIsElected( HighAvailabilityMemberChangeEvent event )
    {
        if ( event.getNewState() == event.getOldState() && event.getOldState() == HighAvailabilityMemberState.MASTER )
        {
            clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI );
        }
        else
        {
            stateChanged( event );
        }
    }

    @Override
    public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
    {
        if ( event.getNewState() == event.getOldState() && event.getOldState() == HighAvailabilityMemberState.SLAVE )
        {
            clusterMemberAvailability.memberIsAvailable( SLAVE, slaveHaURI );
        }
        else
        {
            stateChanged( event );
        }
    }

    @Override
    public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
    {
        // ignored, we don't do any mode switching in slave available events
    }

    @Override
    public void instanceStops( HighAvailabilityMemberChangeEvent event )
    {
        stateChanged( event );
    }

    private void stateChanged( HighAvailabilityMemberChangeEvent event )
    {
        availableMasterId = event.getServerHaUri();
        if ( event.getNewState() == event.getOldState() )
        {
            return;
        }
        switch ( event.getNewState() )
        {
            case TO_MASTER:
                haCommunicationLife.shutdown();
                haCommunicationLife = new LifeSupport();

                if ( event.getOldState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                }

                switchToMaster();
                break;
            case TO_SLAVE:
                haCommunicationLife.shutdown();
                switchToSlave();
                break;
            case PENDING:
                if ( event.getOldState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                }
                else if ( event.getOldState().equals( HighAvailabilityMemberState.MASTER ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( MASTER );
                }

                haCommunicationLife.shutdown();
                haCommunicationLife = new LifeSupport();
                break;
            default:
                // do nothing
        }
    }

    private void switchToMaster()
    {
        msgLog.logMessage( "I am " + config.get( ClusterSettings.server_id ) + ", moving to master" );
        try
        {
            DependencyResolver resolver = graphDb.getDependencyResolver();
            HaXaDataSourceManager xaDataSourceManager = resolver.resolveDependency( HaXaDataSourceManager.class );
            synchronized ( xaDataSourceManager )
            {
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                final TransactionManager txManager = graphDb.getDependencyResolver()
                        .resolveDependency( TransactionManager.class );

                idGeneratorFactory.switchToMaster();

                MasterImpl.SPI spi = new DefaultMasterImplSPI( graphDb, logging, txManager );

                MasterImpl masterImpl = new MasterImpl( spi, monitors.newMonitor( MasterImpl.Monitor.class ),
                        logging, config );

                MasterServer masterServer = new MasterServer( masterImpl, logging, serverConfig(),
                        new BranchDetectingTxVerifier( graphDb ),
                        graphDb.getDependencyResolver().resolveDependency( Monitors.class ));
                haCommunicationLife.add( masterImpl );
                haCommunicationLife.add( masterServer );
                assignMaster( masterImpl );
                idGeneratorFactory.switchToMaster();

                haCommunicationLife.start();

                masterHaURI = URI.create( "ha://" + (ServerUtil.getHostString( masterServer.getSocketAddress() ).contains
                        ( "0.0.0.0" ) ? me.getHost() : ServerUtil.getHostString( masterServer.getSocketAddress() )) + ":" +
                        masterServer.getSocketAddress().getPort() + "?serverId=" +
                        config.get( ClusterSettings.server_id ) );
                clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI );
                msgLog.logMessage( "I am " + config.get( ClusterSettings.server_id ) +
                        ", successfully moved to master" );
            }
        }
        catch ( Throwable e )
        {
            msgLog.logMessage( "Failed to switch to master", e );
        }
    }

    private void assignMaster( Master master )
    {
        masterDelegateHandler.setDelegate( master );
    }

    private URI createHaURI( Server<?,?> server )
    {
        String hostString = ServerUtil.getHostString( server.getSocketAddress() );
        int port = server.getSocketAddress().getPort();
        Integer serverId = config.get( ClusterSettings.server_id );
        String host = hostString.contains( INADDR_ANY ) ? me.getHost() : hostString;
        return URI.create( "ha://" + host + ":" + port + "?serverId=" + serverId );
    }

    private void switchToSlave()
    {
        this.masterClientResolver = new MasterClientResolver( logging,
                config.get( HaSettings.read_timeout ).intValue(),
                config.get( HaSettings.lock_read_timeout ).intValue(),
                config.get( HaSettings.max_concurrent_channels_per_slave ).intValue(),
                config.get( HaSettings.com_chunk_size ).intValue()  );
        
        // Do this with a scheduler, so that if it fails, it can retry later with an exponential backoff with max wait time.
        final AtomicLong wait = new AtomicLong();
        scheduledExecutorService.schedule( new Runnable()
        {
            @Override
            public void run()
            {
                if (haCommunicationLife.getStatus() == LifecycleStatus.STARTED)
                {
                    return; // Already switched - this can happen if a second master becomes available while waiting
                }

                try
                {
                    haCommunicationLife = new LifeSupport();

                    URI masterUri = availableMasterId;

                    console.log( "ServerId " + config.get( ClusterSettings.server_id ) + ", moving to slave for master " +
                            masterUri  );

                    assert masterUri != null; // since we are here it must already have been set from outside
                    DependencyResolver resolver = graphDb.getDependencyResolver();
                    HaXaDataSourceManager xaDataSourceManager = resolver.resolveDependency(
                            HaXaDataSourceManager.class );
                    idGeneratorFactory.switchToSlave();
                    synchronized ( xaDataSourceManager )
                    {
                        if ( !isStorePresent( resolver.resolveDependency( FileSystemAbstraction.class ), config ) )
                        {
                            copyStoreFromMaster( masterUri );
                        }

                        /*
                         * We get here either with a fresh store from the master copy above so we need to start the ds
                         * or we already had a store, so we have already started the ds. Either way, make sure it's there.
                         */
                        NeoStoreXaDataSource nioneoDataSource = ensureDataSourceStarted( xaDataSourceManager, resolver );
                        if ( checkDataConsistency( xaDataSourceManager,
                                resolver.resolveDependency( RequestContextFactory.class ), nioneoDataSource, masterUri ) )
                        {
                            if ( startHaCommunication( xaDataSourceManager, nioneoDataSource, masterUri ) )
                            {
                                console.log( "ServerId " + config.get( ClusterSettings.server_id ) +
                                        ", successfully moved to slave for master " + masterUri );
                                return; // Done
                            }
                        }
                    }
                }
                catch ( Throwable t )
                {
                    msgLog.logMessage( "Error while trying to switch to slave", t );
                }

                haCommunicationLife.shutdown();

                // Try again later
                wait.set( (1 + wait.get()*2) ); // Exponential backoff
                wait.set(Math.min(wait.get(), 5*60)); // Wait maximum 5 minutes

                scheduledExecutorService.schedule( this, wait.get(), TimeUnit.SECONDS );

                msgLog.logMessage( "Attempting to switch to slave in "+wait.get()+"s");
            }
        }, wait.get(), TimeUnit.SECONDS);
    }

    private boolean startHaCommunication( HaXaDataSourceManager xaDataSourceManager,
            NeoStoreXaDataSource nioneoDataSource, URI masterUri )
    {
        try
        {
            MasterClient master = newMasterClient( masterUri, nioneoDataSource.getStoreId(), haCommunicationLife );

            Slave slaveImpl = new SlaveImpl( nioneoDataSource.getStoreId(), master,
                    new RequestContextFactory( getServerId( masterUri ), xaDataSourceManager,
                            graphDb.getDependencyResolver() ), xaDataSourceManager );

            SlaveServer server = new SlaveServer( slaveImpl, serverConfig(), logging,
                    graphDb.getDependencyResolver().resolveDependency( Monitors.class ) );
            assignMaster( master );
            haCommunicationLife.add( slaveImpl );
            haCommunicationLife.add( server );
            haCommunicationLife.start();

            slaveHaURI = createHaURI( server );
            clusterMemberAvailability.memberIsAvailable( SLAVE, slaveHaURI );
            return true;
        }
        catch ( Throwable t )
        {
            msgLog.logMessage( "Got exception while starting HA communication", t );
            haCommunicationLife.shutdown();
            haCommunicationLife = new LifeSupport();
        }
        return false;
    }

    private Server.Configuration serverConfig()
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

    private boolean checkDataConsistency( HaXaDataSourceManager xaDataSourceManager,
                                          RequestContextFactory requestContextFactory,
                                          NeoStoreXaDataSource nioneoDataSource, URI masterUri ) throws Throwable
    {
        // Must be called under lock on XaDataSourceManager
        LifeSupport checkConsistencyLife = new LifeSupport();
        try
        {
            MasterClient checkConsistencyMaster = newMasterClient( masterUri, nioneoDataSource.getStoreId(),
                    checkConsistencyLife );
            checkConsistencyLife.start();
            console.log( "Checking store consistency with master" );
            checkDataConsistencyWithMaster( checkConsistencyMaster, nioneoDataSource );
            console.log( "Store is consistent" );

            /*
             * Pull updates, since the store seems happy and everything. No matter how far back we are, this is just
             * one thread doing the pulling, while the guard is up. This will prevent a race between all transactions
             * that may start the moment the database becomes available, where all of them will pull the same txs from
             * the master but eventually only one will get to apply them.
             */
            console.log( "Catching up with master" );
            RequestContext context = requestContextFactory.newRequestContext( -1 );
            xaDataSourceManager.applyTransactions( checkConsistencyMaster.pullUpdates( context ) );
            console.log( "Now consistent with master" );
            return true;
        }
        catch ( StoreUnableToParticipateInClusterException upe )
        {
            console.log( "The store is inconsistent. Will treat it as branched and fetch a new one from the master" );
            msgLog.warn( "Current store is unable to participate in the cluster; fetching new store from master", upe );
            try
            {
                // Unregistering from a running DSManager stops the datasource
                xaDataSourceManager.unregisterDataSource( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
                stopServicesAndHandleBranchedStore( config.get( HaSettings.branched_data_policy ) );
            }
            catch ( IOException e )
            {
                msgLog.warn( "Failed while trying to handle branched data", e );
            }
        }
        catch ( MismatchingStoreIdException e )
        {
            console.log( "The store does not represent the same database as master. Will remove and fetch a new one from master" );
            if ( nioneoDataSource.getNeoStore().getLastCommittedTx() == 1 )
            {
                msgLog.warn( "Found and deleting empty store with mismatching store id " + e.getMessage() );
                stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );
            }
            else
            {
                msgLog.error( "Store cannot participate in cluster due to mismatching store IDs" );
                throw e;
            }
        }
        catch ( Throwable throwable )
        {
            msgLog.warn( "Consistency checker failed", throwable );
        }
        finally
        {
            checkConsistencyLife.shutdown();
        }
        return false;
    }

    private NeoStoreXaDataSource ensureDataSourceStarted( XaDataSourceManager xaDataSourceManager, DependencyResolver resolver )
    {
        // Must be called under lock on XaDataSourceManager
        NeoStoreXaDataSource nioneoDataSource = (NeoStoreXaDataSource) xaDataSourceManager.getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        if ( nioneoDataSource == null )
        {
            nioneoDataSource = new NeoStoreXaDataSource( config,
                    resolver.resolveDependency( StoreFactory.class ),
                    resolver.resolveDependency( StringLogger.class ),
                    resolver.resolveDependency( XaFactory.class ),
                    resolver.resolveDependency( TransactionStateFactory.class ),
                    resolver.resolveDependency( TransactionInterceptorProviders.class ),
                    resolver.resolveDependency( JobScheduler.class ),
                    logging,
                    updateableSchemaState,
                    new NonTransactionalTokenNameLookup(
                            resolver.resolveDependency( LabelTokenHolder.class ),
                            resolver.resolveDependency( PropertyKeyTokenHolder.class ) ),
                    resolver,
                    resolver.resolveDependency( AbstractTransactionManager.class ),
                    resolver.resolveDependency( PropertyKeyTokenHolder.class ),
                    resolver.resolveDependency( LabelTokenHolder.class ),
                    resolver.resolveDependency( RelationshipTypeTokenHolder.class ),
                    resolver.resolveDependency( PersistenceManager.class ),
                    resolver.resolveDependency( LockManager.class ),
                    (SchemaWriteGuard)graphDb);
            xaDataSourceManager.registerDataSource( nioneoDataSource );
                /*
                 * CAUTION: The next line may cause severe eye irritation, mental instability and potential
                 * emotional breakdown. On the plus side, it is correct.
                 * See, it is quite possible to get here without the NodeManager having stopped, because we don't
                 * properly manage lifecycle in this class (this is the cause of this ugliness). So, after we
                 * register the datasource with the DsMgr we need to make sure that NodeManager re-reads the reltype
                 * and propindex information. Normally, we would have shutdown everything before getting here.
                 */
            resolver.resolveDependency( NodeManager.class ).start();
        }
        return nioneoDataSource;
    }

    private void copyStoreFromMaster( URI masterUri ) throws Throwable
    {
        // Must be called under lock on XaDataSourceManager
        LifeSupport life = new LifeSupport();
        try
        {
            // Remove the current store - neostore file is missing, nothing we can really do
            stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );
            MasterClient copyMaster = newMasterClient( masterUri, null, life );

            life.start();

            // This will move the copied db to the graphdb location
            console.log( "Copying store from master" );
            new SlaveStoreWriter( config, kernelExtensions, console ).copyStore( copyMaster );

            startServicesAgain();
            console.log( "Finished copying store from master" );
        }
        finally
        {
            life.stop();
        }
    }

    private MasterClient newMasterClient( URI masterUri, StoreId storeId, LifeSupport life )
    {
        return masterClientResolver.instantiate( masterUri.getHost(), masterUri.getPort(),
                graphDb.getDependencyResolver().resolveDependency( Monitors.class ), storeId, life );
    }

    private void startServicesAgain() throws Throwable
    {
        @SuppressWarnings( "rawtypes" )
        List<Class> services = new ArrayList<>( Arrays.asList( SERVICES_TO_RESTART_FOR_STORE_COPY ) );
        for ( Class<?> serviceClass : services )
        {
            Lifecycle service = (Lifecycle) graphDb.getDependencyResolver().resolveDependency( serviceClass );
            service.start();
        }
    }

    private void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy ) throws Throwable
    {
        List<Class> services = new ArrayList<>( Arrays.asList( SERVICES_TO_RESTART_FOR_STORE_COPY ) );
        Collections.reverse( services );
        for ( Class<?> serviceClass : services )
        {
            Lifecycle service = (Lifecycle) graphDb.getDependencyResolver().resolveDependency( serviceClass );
            service.stop();
        }
        
        branchPolicy.handle( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) );
    }

    private void checkDataConsistencyWithMaster( Master master, NeoStoreXaDataSource nioneoDataSource )
    {
        long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
        Pair<Integer, Long> myMaster;
        try
        {
            myMaster = nioneoDataSource.getMasterForCommittedTx( myLastCommittedTx );
        }
        catch ( NoSuchLogVersionException e )
        {
            msgLog.logMessage(
                    "Logical log file for txId "
                            + myLastCommittedTx
                            + " missing [version="
                            + e.getVersion()
                            + "]. If this is startup then it will be recovered later, " +
                            "otherwise it might be a problem." );
            return;
        }
        catch ( IOException e )
        {
            msgLog.logMessage( "Failed to get master ID for txId " + myLastCommittedTx + ".", e );
            return;
        }
        catch ( Exception e )
        {
            throw new BranchedDataException( "Exception while getting master ID for txId "
                    + myLastCommittedTx + ".", e );
        }

        HandshakeResult handshake;
        try ( Response<HandshakeResult> response = master.handshake( myLastCommittedTx, nioneoDataSource.getStoreId() ) )
        {
            handshake = response.response();
            requestContextFactory.setEpoch( handshake.epoch() );
        }
        catch( BranchedDataException e )
        {
            // Rethrow wrapped in a branched data exception on our side, to clarify where the problem originates.
            throw new BranchedDataException( "Master detected branched data for this machine.", e );
        }
        catch ( RuntimeException e )
        {
            // Checked exceptions will be wrapped as the cause if this was a serialized
            // server-side exception
            if ( e.getCause() instanceof MissingLogDataException )
            {
                /*
                 * This means the master was unable to find a log entry for the txid we just asked. This
                 * probably means the thing we asked for is too old or too new. Anyway, since it doesn't
                 * have the tx it is better if we just throw our store away and ask for a new copy. Next
                 * time around it shouldn't have to even pass from here.
                 */
                throw new StoreOutOfDateException( "The master is missing the log required to complete the " +
                        "consistency check", e.getCause() );
            }
            throw e;
        }

        if ( myMaster.first() != XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER &&
                (myMaster.first() != handshake.txAuthor() || myMaster.other() != handshake.txChecksum()) )
        {
            String msg = "Branched data, I (machineId:" + config.get( ClusterSettings.server_id ) + ") think machineId for" +
                    " txId (" +
                    myLastCommittedTx + ") is " + myMaster + ", but master (machineId:" +
                    getServerId( availableMasterId ) + ") says that it's " + handshake;
            throw new BranchedDataException( msg );
        }
        msgLog.logMessage( "Master id for last committed tx ok with highestTxId=" +
                myLastCommittedTx + " with masterId=" + myMaster, true );
    }
}
