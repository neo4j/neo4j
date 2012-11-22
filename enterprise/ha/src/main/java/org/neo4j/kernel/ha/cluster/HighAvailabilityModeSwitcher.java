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

package org.neo4j.kernel.ha.cluster;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.com.ComSettings;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.BranchDetectingTxVerifier;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient18;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.RequestContextFactory;
import org.neo4j.kernel.ha.Slave;
import org.neo4j.kernel.ha.SlaveImpl;
import org.neo4j.kernel.ha.SlaveServer;
import org.neo4j.kernel.ha.SlaveStoreWriter;
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Performs the internal switches from pending to slave/master, by listening for
 * ClusterMemberChangeEvents. When finished it will invoke {@link HighAvailabilityEvents#memberIsAvailable(String)} to announce
 * to the cluster it's new status.
 */
public class HighAvailabilityModeSwitcher implements HighAvailabilityMemberListener, Lifecycle
{
    public static int getServerId( URI serverId )
    {
        String query = serverId.getQuery();
        for ( String param : query.split( "&" ) )
        {
            if ( param.startsWith( "serverId" ) )
            {
                return Integer.parseInt( param.substring( "serverId=".length() ) );
            }
        }
        return -1;
    }

    private URI availableMasterId;
    private final DelegateInvocationHandler delegateHandler;
    private final HighAvailabilityEvents clusterEvents;
    private final GraphDatabaseAPI graphDb;
    private final Config config;
    private LifeSupport life;
    private final StringLogger msgLog;
    private ScheduledExecutorService executor;
    private Future<?> toMasterTask;
    private Future<?> toSlaveTask;

    public HighAvailabilityModeSwitcher( DelegateInvocationHandler delegateHandler, HighAvailabilityEvents clusterEvents,
                                      HighAvailabilityMemberStateMachine stateHandler, GraphDatabaseAPI graphDb,
                                      Config config, StringLogger msgLog )
    {
        this.delegateHandler = delegateHandler;
        this.clusterEvents = clusterEvents;
        this.graphDb = graphDb;
        this.config = config;
        this.msgLog = msgLog;
        this.life = new LifeSupport();
        stateHandler.addHighAvailabilityMemberListener( this );
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        executor = Executors.newSingleThreadScheduledExecutor( new NamedThreadFactory( "Mode switcher" ) );
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        executor.shutdownNow();
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    @Override
    public void masterIsElected( HighAvailabilityMemberChangeEvent event )
    {
        stateChanged( event );
    }

    @Override
    public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
    {
        stateChanged( event );
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
                life.shutdown();
                life = new LifeSupport();
                switchToMaster();
                break;
            case TO_SLAVE:
                life.shutdown();
                life = new LifeSupport();
                switchToSlave();
                break;
            case PENDING:
                life.shutdown();
                life = new LifeSupport();
                break;
            default:
                // do nothing
        }
    }

    public void switchToMaster()
    {
        toMasterTask = executor.submit( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    MasterImpl masterImpl = new MasterImpl( graphDb, graphDb.getMessageLog(), config );
                    Server.Configuration serverConfig = new Server.Configuration()
                    {
                        @Override
                        public long getOldChannelThreshold()
                        {
                            return config.isSet( HaSettings.lock_read_timeout ) ?
                                    config.get( HaSettings.lock_read_timeout ) : config.get( ClusterSettings
                                    .read_timeout );
                        }

                        @Override
                        public int getMaxConcurrentTransactions()
                        {
                            return config.get( HaSettings.max_concurrent_channels_per_slave );
                        }

                        @Override
                        public int getPort()
                        {
                            int port = HaSettings.ha_server.getPort( config.getParams() );
                            if ( port > 0 )
                            {
                                return port;
                            }

                            // If not specified, use the default
                            return HaSettings.ha_server.getPort( MapUtil.stringMap( HaSettings.ha_server.name(),
                                    ConfigurationDefaults.getDefault( HaSettings.ha_server, HaSettings.class ) ) );
                        }

                        @Override
                        public int getChunkSize()
                        {
                            return config.isSet( ComSettings.com_chunk_size ) ? config.get( ComSettings
                                    .com_chunk_size ) :
                                    ComSettings.com_chunk_size.valueOf( ConfigurationDefaults.getDefault(
                                            ComSettings.com_chunk_size, ComSettings.class ), config );
                        }

                        @Override
                        public String getServerAddress()
                        {
                            return HaSettings.ha_server.getAddressWithLocalhostDefault( config.getParams() );
                        }
                    };
                    MasterServer masterServer = new MasterServer( masterImpl, msgLog, serverConfig,
                            new BranchDetectingTxVerifier( graphDb ) );
                    life.add( masterImpl );
                    life.add( masterServer );
                    delegateHandler.setDelegate( masterImpl );
                    DependencyResolver resolver = graphDb.getDependencyResolver();
                    HaXaDataSourceManager xaDsm = resolver.resolveDependency( HaXaDataSourceManager.class );
                    synchronized ( xaDsm )
                    {
                        XaDataSource nioneoDataSource = xaDsm.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
                        if ( nioneoDataSource == null )
                        {
                            try
                            {
                                nioneoDataSource = new NeoStoreXaDataSource( config,
                                        resolver.resolveDependency( StoreFactory.class ),
                                        resolver.resolveDependency( LockManager.class ),
                                        resolver.resolveDependency( StringLogger.class ),
                                        resolver.resolveDependency( XaFactory.class ),
                                        resolver.resolveDependency( TransactionStateFactory.class ),
                                        resolver.resolveDependency( TransactionInterceptorProviders.class ),
                                        resolver );
                                xaDsm.registerDataSource( nioneoDataSource );
                            }
                            catch ( IOException e )
                            {
                                msgLog.logMessage( "Failed while trying to create datasource", e );
                                return;
                            }
                        }
                    }
                    life.start();
                }
                catch ( Throwable e )
                {
                    msgLog.logMessage( "Failed to switch to master", e );
                    return;
                }
                clusterEvents.memberIsAvailable( ClusterConfiguration.COORDINATOR );
            }
        } );
    }

    public void switchToSlave()
    {
        // TODO factor out switch tasks to named methods
        toSlaveTask = executor.submit( new Runnable()
        {
            public int tries;

            @Override
            public void run()
            {
                try
                {
                    URI masterUri = availableMasterId;

                    msgLog.logMessage( "I am " + config.get( HaSettings.server_id ) + ", moving to slave for master " +
                            masterUri );

                    assert masterUri != null; // since we are here it must already have been set from outside
                    DependencyResolver resolver = graphDb.getDependencyResolver();
                    HaXaDataSourceManager xaDataSourceManager = resolver.resolveDependency(
                            HaXaDataSourceManager.class );
                    synchronized ( xaDataSourceManager )
                    {

                    if ( !NeoStore.isStorePresent( resolver.resolveDependency( FileSystemAbstraction.class ), config ) )
                    {
                        LifeSupport life = new LifeSupport();
                        try
                        {
                            // Remove the current store - neostore file is missing, nothing we can really do
                            stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none, true );
                            MasterClient18 copyMaster =
                                    new MasterClient18( masterUri, graphDb.getMessageLog(), null, config );

                            life.add( copyMaster );
                            life.start();

                            // This will move the copied db to the graphdb location
                            msgLog.logMessage( "Copying store from master" );
                            new SlaveStoreWriter( config ).copyStore( copyMaster );

                            startServicesAgain();
                            msgLog.logMessage( "Finished copying store from master" );
                        }
                        catch ( Throwable e )
                        {
                            msgLog.logMessage( "Failed to copy store from master", e );
                            retryLater( true );
                            return;
                        }
                        finally
                        {
                            life.stop();
                        }
                    }
                    NeoStoreXaDataSource nioneoDataSource = (NeoStoreXaDataSource) resolver.resolveDependency(
                            HaXaDataSourceManager.class ).getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
                    if ( nioneoDataSource == null )
                    {
                        try
                        {
                            nioneoDataSource = new NeoStoreXaDataSource( config,
                                    resolver.resolveDependency( StoreFactory.class ),
                                    resolver.resolveDependency( LockManager.class ),
                                    resolver.resolveDependency( StringLogger.class ),
                                    resolver.resolveDependency( XaFactory.class ),
                                    resolver.resolveDependency( TransactionStateFactory.class ),
                                    resolver.resolveDependency( TransactionInterceptorProviders.class ),
                                    resolver );
                            xaDataSourceManager.registerDataSource( nioneoDataSource );
                        }
                        catch ( IOException e )
                        {
                            msgLog.logMessage( "Failed while trying to create datasource", e );
                            return;
                        }
                    }

                    LifeSupport checkConsistencyLife = new LifeSupport();
                    try
                    {
                        MasterClient18 checkConsistencyMaster = new MasterClient18( masterUri,
                                graphDb.getMessageLog(), nioneoDataSource.getStoreId(), config );
                        checkConsistencyLife.add( checkConsistencyMaster );
                        checkConsistencyLife.start();
                        checkDataConsistencyWithMaster( checkConsistencyMaster, nioneoDataSource );
                    }
                    catch ( StoreUnableToParticipateInClusterException upe )
                    {
                        msgLog.logMessage( "Current store is unable to participate in the cluster", upe );
                        try
                        {
                            // Unregistering from a running DSManager stops the datasource
                            xaDataSourceManager.unregisterDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
                            stopServicesAndHandleBranchedStore( config.get( HaSettings.branched_data_policy ), false );
                        }
                        catch ( IOException e )
                        {
                            msgLog.logMessage( "Failed while trying to handle branched data", e );
                        }
                        retryLater( false );
                        return;
                    }
                    catch ( Throwable throwable )
                    {
                        msgLog.warn( "Consistency checker failed", throwable );
                    }
                    finally
                    {
                        checkConsistencyLife.shutdown();
                    }

                    try
                    {
                        MasterClient18 master = new MasterClient18( masterUri, graphDb.getMessageLog(),
                                nioneoDataSource.getStoreId(), config );

                        Slave slaveImpl = new SlaveImpl( nioneoDataSource.getStoreId(), master,
                                new RequestContextFactory(
                                        getServerId( masterUri ), xaDataSourceManager,
                                        graphDb.getDependencyResolver() ), xaDataSourceManager );
                        Server.Configuration serverConfig = new Server.Configuration()
                        {
                            @Override
                            public long getOldChannelThreshold()
                            {
                                return 20;
                            }

                            @Override
                            public int getMaxConcurrentTransactions()
                            {
                                return 1;
                            }

                            @Override
                            public int getPort()
                            {
                                int port = HaSettings.ha_server.getPort( config.getParams() );
                                if ( port > 0 )
                                {
                                    return port;
                                }

                                // If not specified, use the default
                                return HaSettings.ha_server.getPort( MapUtil.stringMap( HaSettings.ha_server.name(),
                                        ConfigurationDefaults.getDefault( HaSettings.ha_server, HaSettings.class ) ) );
                            }

                            @Override
                            public int getChunkSize()
                            {
                                return config.isSet( ComSettings.com_chunk_size ) ? config.get( ComSettings
                                        .com_chunk_size ) :
                                        ComSettings.com_chunk_size.valueOf( ConfigurationDefaults.getDefault(
                                                ComSettings.com_chunk_size, ComSettings.class ), config );
                            }

                            @Override
                            public String getServerAddress()
                            {
                                return HaSettings.ha_server.getAddressWithLocalhostDefault( config.getParams() );
                            }
                        };
                        SlaveServer server = new SlaveServer( slaveImpl, serverConfig, msgLog );
                        delegateHandler.setDelegate( master );
                        life.add( master );
                        life.add( slaveImpl );
                        life.add( server );
                        life.start();
                        clusterEvents.memberIsAvailable( ClusterConfiguration.SLAVE );

                        msgLog.logMessage( "I am " + config.get( HaSettings.server_id ) +
                                ", successfully moved to slave for master " + masterUri );
                        return; // finally, it's over
                    }
                    catch ( Throwable t )
                    {
                        life.shutdown();
                        life = new LifeSupport();
                        nioneoDataSource.stop();
                        msgLog.logMessage( "Got exception while trying to verify consistency with master", t );

                        retryLater( true );

                        return;
                    }
                }
                }
                catch ( Throwable t )
                {
                    msgLog.logMessage( "Unable to switch to slave", t );
                }
            }

            private void startServicesAgain() throws Throwable
            {
                graphDb.getDependencyResolver().resolveDependency( NodeManager.class ).start();
                graphDb.getDependencyResolver().resolveDependency( TxManager.class ).start();
                graphDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).start();
            }

            private void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy, boolean deleteIndexes ) throws Throwable
            {
                graphDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).stop();
                graphDb.getDependencyResolver().resolveDependency( TxManager.class ).stop();
                graphDb.getDependencyResolver().resolveDependency( NodeManager.class ).stop();
                branchPolicy.handle( new File( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) ) );
                if ( deleteIndexes )
                {
                    FileUtils.deleteRecursively( new File(
                            config.get( InternalAbstractGraphDatabase.Configuration.store_dir ), "index" ) );
                }
            }

            /*

            // Those left here for posterity, all data source start-stop cycles must happen through XaDSManager
            private void startOtherDataSources() throws Throwable
            {
                LuceneKernelExtension lucene = graphDb.getDependencyResolver().resolveDependency(
                        KernelExtensions.class ).resolveDependency( LuceneKernelExtension.class );
                lucene.start();
            }

            private void stopOtherDataSources() throws Throwable
            {
                LuceneKernelExtension lucene = graphDb.getDependencyResolver().resolveDependency(
                        KernelExtensions.class ).resolveDependency( LuceneKernelExtension.class );
                lucene.stop();
            }
            */
            private void retryLater( boolean wayLater )
            {
                if ( ++tries < 5 )
                {
                    executor.schedule( this, wayLater ? 15 : 1, TimeUnit.SECONDS );
                }
                else
                {
                    msgLog.error( "Giving up trying to switch to slave" );
                }
            }
        } );
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

        Response<Pair<Integer, Long>> response = null;
        Pair<Integer, Long> mastersMaster;
        try
        {
            response = master.getMasterIdForCommittedTx( myLastCommittedTx, nioneoDataSource.getStoreId() );
            mastersMaster = response.response();
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
            else
            {
                throw e;
            }
        }
        finally
        {
            if ( response != null )
            {
                response.close();
            }
        }

        if ( myMaster.first() != XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER
                && !myMaster.equals( mastersMaster ) )
        {
            String msg = "Branched data, I (machineId:" + config.get( HaSettings.server_id ) + ") think machineId for" +
                    " txId (" +
                    myLastCommittedTx + ") is " + myMaster + ", but master (machineId:" +
                    getServerId( availableMasterId ) + ") says that it's " + mastersMaster;
            throw new BranchedDataException( msg );
        }
        msgLog.logMessage( "Master id for last committed tx ok with highestTxId=" +
                myLastCommittedTx + " with masterId=" + myMaster, true );
    }
}
