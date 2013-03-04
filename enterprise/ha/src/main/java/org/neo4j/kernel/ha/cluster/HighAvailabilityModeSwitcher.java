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
package org.neo4j.kernel.ha.cluster;

import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Uris.parameter;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.isStorePresent;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchDetectingTxVerifier;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.MasterClient20;
import org.neo4j.kernel.ha.SlaveStoreWriter;
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.SlaveImpl;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

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
        XaDataSourceManager.class,
        TxManager.class,
        NodeManager.class,
        IndexStore.class,
        StoreLockerLifecycleAdapter.class
    };

    public static final String MASTER = "master";
    public static final String SLAVE = "slave";

    public static int getServerId( URI haUri )
    {
        // Get serverId parameter, default to -1 if it is missing, and parse to integer
        return INTEGER.apply( withDefaults(
                Functions.<URI, String>constant( "-1" ), parameter( "serverId" ) ).apply( haUri ));
    }

    private URI availableMasterId;

    private final HighAvailabilityMemberStateMachine stateHandler;
    private final DelegateInvocationHandler delegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final GraphDatabaseAPI graphDb;
    private final Config config;
    private LifeSupport life;
    private final StringLogger msgLog;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final Logging logging;

    public HighAvailabilityModeSwitcher( DelegateInvocationHandler delegateHandler,
                                         ClusterMemberAvailability clusterMemberAvailability,
                                         HighAvailabilityMemberStateMachine stateHandler, GraphDatabaseAPI graphDb,
                                         HaIdGeneratorFactory idGeneratorFactory, Config config, Logging logging )
    {
        this.delegateHandler = delegateHandler;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.graphDb = graphDb;
        this.idGeneratorFactory = idGeneratorFactory;
        this.config = config;
        this.logging = logging;
        this.msgLog = logging.getLogger( getClass() );
        this.life = new LifeSupport();
        this.stateHandler = stateHandler;
    }

    @Override
    public synchronized void init() throws Throwable
    {
        stateHandler.addHighAvailabilityMemberListener( this );
        life.init();
    }

    @Override
    public synchronized void start() throws Throwable
    {
        life.start();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        stateHandler.removeHighAvailabilityMemberListener( this );
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

                if ( event.getOldState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                }

                switchToMaster();
                break;
            case TO_SLAVE:
                life.shutdown();
                life = new LifeSupport();
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

                life.shutdown();
                life = new LifeSupport();
                break;
            default:
                // do nothing
        }
    }

    private void switchToMaster()
    {
        msgLog.logMessage( "I am " + config.get( HaSettings.server_id ) + ", moving to master" );
        try
        {
            MasterImpl masterImpl = new MasterImpl( graphDb, logging, config );
            
            MasterServer masterServer = new MasterServer( masterImpl, logging, serverConfig(),
                    new BranchDetectingTxVerifier( graphDb ) );
            life.add( masterImpl );
            life.add( masterServer );
            delegateHandler.setDelegate( masterImpl );
            DependencyResolver resolver = graphDb.getDependencyResolver();
            HaXaDataSourceManager xaDsm = resolver.resolveDependency( HaXaDataSourceManager.class );
            
            idGeneratorFactory.switchToMaster();
            synchronized ( xaDsm )
            {
                ensureDataSourceStarted( xaDsm, resolver );
            }
            life.start();

            URI haUri = URI.create( "ha://" + masterServer.getSocketAddress().getHostName() + ":" +
                    masterServer.getSocketAddress().getPort() + "?serverId=" +
                    config.get( HaSettings.server_id ) );
            clusterMemberAvailability.memberIsAvailable( MASTER, haUri );
            msgLog.logMessage( "I am " + config.get( HaSettings.server_id ) +
                    ", successfully moved to master" );
        }
        catch ( Throwable e )
        {
            msgLog.logMessage( "Failed to switch to master", e );
            return;
        }
    }

    private void switchToSlave()
    {
        for ( int tries = 5; tries-- > 0; )
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
                idGeneratorFactory.switchToSlave();
                synchronized ( xaDataSourceManager )
                {
                    if ( !isStorePresent( resolver.resolveDependency( FileSystemAbstraction.class ), config ) )
                    {
                        if ( !copyStoreFromMaster( masterUri ) )
                            continue; // to the outer loop for a retry
                    }
                    
                    NeoStoreXaDataSource nioneoDataSource = ensureDataSourceStarted( xaDataSourceManager, resolver );
                    if ( !checkDataConsistency( xaDataSourceManager, nioneoDataSource, masterUri ) )
                        continue; // to the outer loop for a retry

                    if ( !startHaCommunication( xaDataSourceManager, nioneoDataSource, masterUri ) )
                        continue; // to the outer loop for a retry

                    msgLog.logMessage( "I am " + config.get( HaSettings.server_id ) +
                            ", successfully moved to slave for master " + masterUri );
                    break; // from the retry loop
                }
            }
            catch ( Throwable t )
            {
                msgLog.logMessage( "Unable to switch to slave", t );
            }
        }
    }

    private boolean startHaCommunication( HaXaDataSourceManager xaDataSourceManager, NeoStoreXaDataSource nioneoDataSource, URI masterUri )
    {
        try
        {
            MasterClient20 master = new MasterClient20( masterUri, logging,
                    nioneoDataSource.getStoreId(), config );

            Slave slaveImpl = new SlaveImpl( nioneoDataSource.getStoreId(), master,
                    new RequestContextFactory( getServerId( masterUri ), xaDataSourceManager,
                            graphDb.getDependencyResolver() ), xaDataSourceManager );
            
            SlaveServer server = new SlaveServer( slaveImpl, serverConfig(), logging );
            delegateHandler.setDelegate( master );
            life.add( master );
            life.add( slaveImpl );
            life.add( server );
            life.start();

            URI haUri = URI.create( "ha://" + server.getSocketAddress().getHostName() + ":" +
                    server.getSocketAddress().getPort() + "?serverId=" +
                    config.get( HaSettings.server_id ) );
            clusterMemberAvailability.memberIsAvailable( SLAVE, haUri );
            return true;
        }
        catch ( Throwable t )
        {
            msgLog.logMessage( "Got exception while starting HA communication", t );
            life.shutdown();
            life = new LifeSupport();
            nioneoDataSource.stop();
        }
        return false;
    }

    private Server.Configuration serverConfig()
    {
        Server.Configuration serverConfig = new Server.Configuration()
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
        return serverConfig;
    }

    private boolean checkDataConsistency( XaDataSourceManager xaDataSourceManager,
            NeoStoreXaDataSource nioneoDataSource, URI masterUri ) throws Throwable
    {
        // Must be called under lock on XaDataSourceManager
        LifeSupport checkConsistencyLife = new LifeSupport();
        try
        {
            MasterClient20 checkConsistencyMaster = new MasterClient20( masterUri,
                    logging, nioneoDataSource.getStoreId(), config );
            checkConsistencyLife.add( checkConsistencyMaster );
            checkConsistencyLife.start();
            checkDataConsistencyWithMaster( checkConsistencyMaster, nioneoDataSource );
            return true;
        }
        catch ( StoreUnableToParticipateInClusterException upe )
        {
            msgLog.warn( "Current store is unable to participate in the cluster; fetching new store from master", upe );
            try
            {
                // Unregistering from a running DSManager stops the datasource
                xaDataSourceManager.unregisterDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
                stopServicesAndHandleBranchedStore( config.get( HaSettings.branched_data_policy ) );
            }
            catch ( IOException e )
            {
                msgLog.warn( "Failed while trying to handle branched data", e );
            }
        }
        catch ( MismatchingStoreIdException e )
        {
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
            throws IOException
    {
        // Must be called under lock on XaDataSourceManager
        NeoStoreXaDataSource nioneoDataSource = (NeoStoreXaDataSource) xaDataSourceManager.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
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
                        resolver.resolveDependency( CacheAccessBackDoor.class ),
                        resolver.resolveDependency( SchemaIndexProvider.class ),
                        resolver.resolveDependency( TransactionInterceptorProviders.class ),
                        resolver.resolveDependency( JobScheduler.class ), logging );
                xaDataSourceManager.registerDataSource( nioneoDataSource );
            }
            catch ( IOException e )
            {
                msgLog.logMessage( "Failed while trying to create datasource", e );
                throw e;
            }
        }
        return nioneoDataSource;
    }

    private boolean copyStoreFromMaster( URI masterUri )
    {
        // Must be called under lock on XaDataSourceManager
        LifeSupport life = new LifeSupport();
        try
        {
            // Remove the current store - neostore file is missing, nothing we can really do
            stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );
            MasterClient20 copyMaster = new MasterClient20( masterUri, logging, null, config );

            life.add( copyMaster );
            life.start();

            // This will move the copied db to the graphdb location
            msgLog.logMessage( "Copying store from master" );
            new SlaveStoreWriter( config ).copyStore( copyMaster );

            startServicesAgain();
            msgLog.logMessage( "Finished copying store from master" );
            return true;
        }
        catch ( Throwable e )
        {
            msgLog.logMessage( "Failed to copy store from master", e );
        }
        finally
        {
            life.stop();
        }
        return false;
    }

    private void startServicesAgain() throws Throwable
    {
        @SuppressWarnings( "rawtypes" )
        List<Class> services = new ArrayList<Class>( Arrays.asList( SERVICES_TO_RESTART_FOR_STORE_COPY ) );
        Collections.reverse( services );
        for ( Class<Lifecycle> serviceClass : services )
            graphDb.getDependencyResolver().resolveDependency( serviceClass ).start();
    }

    @SuppressWarnings( "unchecked" )
    private void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy )
            throws Throwable
    {
        for ( Class<Lifecycle> serviceClass : SERVICES_TO_RESTART_FOR_STORE_COPY )
            graphDb.getDependencyResolver().resolveDependency( serviceClass ).stop();
        
        branchPolicy.handle( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) );
    }

    /* Those left here for posterity, all data source start-stop cycles must happen through XaDSManager
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
    } */

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
