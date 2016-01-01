/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.List;
import javax.transaction.TransactionManager;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.storecopy.RemoteStoreCopier;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.MasterClient210;
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMemberVersionCheck;
import org.neo4j.kernel.ha.cluster.member.ClusterMemberVersionCheck.Outcome;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveImpl;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.transaction.DenseNodeTransactionTranslator;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.InconsistentlyUpgradedClusterException;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.UnableToCopyStoreFromOldMasterException;
import org.neo4j.kernel.impl.nioneo.store.UnavailableMembersException;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.getServerId;
import static org.neo4j.kernel.ha.cluster.member.ClusterMembers.inRole;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.isStorePresent;

public class SwitchToSlave
{
    @SuppressWarnings("unchecked")
    private static final Class<? extends Lifecycle>[] SERVICES_TO_RESTART_FOR_STORE_COPY = new Class[]{
            StoreLockerLifecycleAdapter.class,
            XaDataSourceManager.class,
            TransactionManager.class,
            NodeManager.class,
            IndexStore.class
    };

    private static final int VERSION_CHECK_TIMEOUT = 10;

    private final Logging logging;
    private final StringLogger msgLog;
    private final ConsoleLogger console;
    private final Config config;
    private final DependencyResolver resolver;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final RequestContextFactory requestContextFactory;
    private final UpdateableSchemaState updateableSchemaState;
    private final Monitors monitors;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final MasterClientResolver masterClientResolver;

    public SwitchToSlave( ConsoleLogger console, Config config, DependencyResolver resolver,
            HaIdGeneratorFactory idGeneratorFactory, Logging logging,
            DelegateInvocationHandler<Master> masterDelegateHandler,
            ClusterMemberAvailability clusterMemberAvailability, RequestContextFactory requestContextFactory,
            UpdateableSchemaState updateableSchemaState, MasterClientResolver masterClientResolver, Monitors monitors,
            Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        this.console = console;
        this.config = config;
        this.resolver = resolver;
        this.idGeneratorFactory = idGeneratorFactory;
        this.logging = logging;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.requestContextFactory = requestContextFactory;
        this.updateableSchemaState = updateableSchemaState;
        this.monitors = monitors;
        this.kernelExtensions = kernelExtensions;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.masterDelegateHandler = masterDelegateHandler;
        this.masterClientResolver = masterClientResolver;
    }

    /**
     * Performs a switch to the slave state. Starts the communication endpoints, switches components to the slave state
     * and ensures that the current database is appropriate for this cluster. It also broadcasts the appropriate
     * Slave Is Available event
     *
     * @param haCommunicationLife The LifeSupport instance to register the network facilities required for communication
     *                            with the rest of the cluster
     * @param me                  The URI this instance must bind to
     * @param masterUri           The URI of the master for which this instance must become slave to
     * @param cancellationRequest A handle for gracefully aborting the switch
     * @return The URI that was broadcasted as the slave endpoint or null if the task was cancelled
     * @throws Throwable
     */
    public URI switchToSlave( LifeSupport haCommunicationLife, URI me, URI masterUri,
                              CancellationRequest cancellationRequest ) throws Throwable
    {
        InstanceId myId = config.get( ClusterSettings.server_id );

        console.log( "ServerId " + myId + ", moving to slave for master " + masterUri );

        assert masterUri != null; // since we are here it must already have been set from outside

        HaXaDataSourceManager xaDataSourceManager = resolver.resolveDependency( HaXaDataSourceManager.class );
        idGeneratorFactory.switchToSlave();

        NeoStoreXaDataSource nioneoDataSource;
        StoreId myStoreId;
        synchronized ( xaDataSourceManager )
        {
            copyStoreFromMasterIfNeeded( masterUri, cancellationRequest );

           /*
            * The following check is mandatory, since the store copy can be cancelled and if it was actually
            * happening then we can't continue, as there is no store in place
            */
            if ( cancellationRequest.cancellationRequested() )
            {
                msgLog.info( "Switch to slave cancelled during store copy if no local store is present." );
                return null;
            }

           /*
            * We get here either with a fresh store from the master copy above so we need to
            * start the ds or we already had a store, so we have already started the ds. Either way,
            * make sure it's there.
            */
            nioneoDataSource = ensureDataSourceStarted( xaDataSourceManager, resolver );
            myStoreId = nioneoDataSource.getStoreId();

            boolean consistencyChecksExecutedSuccessfully = executeConsistencyChecks(
                    myId, masterUri, xaDataSourceManager, nioneoDataSource, cancellationRequest );

            if ( !consistencyChecksExecutedSuccessfully )
            {
                msgLog.info( "Switch to slave cancelled due to consistency check failure." );
                return null;
            }
        }

        if ( cancellationRequest.cancellationRequested() )
        {
            msgLog.info( "Switch to slave cancelled after consistency checks." );
            return null;
        }
        // no exception were thrown and we can proceed
        URI slaveUri = startHaCommunication( haCommunicationLife, xaDataSourceManager,
                nioneoDataSource, me, masterUri, myStoreId );

        console.log( "ServerId " + myId + ", successfully moved to slave for master " + masterUri );

        return slaveUri;
    }

    private void copyStoreFromMasterIfNeeded( URI masterUri, CancellationRequest cancellationRequest ) throws Throwable
    {
        if ( !isStorePresent( resolver.resolveDependency( FileSystemAbstraction.class ), config ) )
        {
            LifeSupport copyLife = new LifeSupport();
            try
            {
                MasterClient masterClient = copyLife.add( newMasterClient( masterUri, null ) );
                copyLife.start();

                boolean masterIsOld = MasterClient.CURRENT.compareTo( masterClient.getProtocolVersion() ) > 0;
                if ( masterIsOld )
                {
                    throw new UnableToCopyStoreFromOldMasterException( MasterClient.CURRENT.getApplicationProtocol(),
                            masterClient.getProtocolVersion().getApplicationProtocol() );
                }
                else
                {
                    copyStoreFromMaster( masterClient, cancellationRequest );
                }
            }
            finally
            {
                copyLife.shutdown();
            }
        }
    }

    private boolean executeConsistencyChecks( InstanceId myId, URI masterUri, HaXaDataSourceManager xaDataSourceManager,
                                              NeoStoreXaDataSource nioneoDataSource,
                                              CancellationRequest cancellationRequest ) throws Throwable
    {
        LifeSupport consistencyCheckLife = new LifeSupport();
        try
        {
            StoreId myStoreId = nioneoDataSource.getStoreId();

            MasterClient masterClient = consistencyCheckLife.add( newMasterClient( masterUri, myStoreId ) );
            consistencyCheckLife.start();

            boolean masterIsOld = MasterClient.CURRENT.compareTo( masterClient.getProtocolVersion() ) > 0;

            if ( masterIsOld )
            {
                ClusterMembers members = resolver.resolveDependency( ClusterMembers.class );
                ClusterMemberVersionCheck checker = new ClusterMemberVersionCheck( members, myId, SYSTEM_CLOCK );

                Outcome outcome = checker.doVersionCheck( myStoreId, VERSION_CHECK_TIMEOUT, SECONDS );
                msgLog.info( "Cluster members version  checked: " + outcome );

                if ( outcome.hasUnavailable() )
                {
                    throw new UnavailableMembersException( outcome.getUnavailable() );
                }
                if ( outcome.hasMismatched() )
                {
                    throw new InconsistentlyUpgradedClusterException( myStoreId, outcome.getMismatched() );
                }
            }

            if ( cancellationRequest.cancellationRequested() )
            {
                return false;
            }

            checkDataConsistency( xaDataSourceManager, masterClient,
                                  resolver.resolveDependency( RequestContextFactory.class ),
                                  nioneoDataSource, masterUri, masterIsOld );
        }
        finally
        {
            consistencyCheckLife.shutdown();
        }
        return true;
    }

    void checkDataConsistency( HaXaDataSourceManager xaDataSourceManager, MasterClient masterClient,
            RequestContextFactory requestContextFactory, NeoStoreXaDataSource nioneoDataSource, URI masterUri,
            boolean masterIsOld ) throws Throwable
    {
        // Must be called under lock on XaDataSourceManager
        try
        {
            console.log( "Checking store consistency with master" );

            checkMyStoreIdAndMastersStoreId( nioneoDataSource, masterIsOld );

            checkDataConsistencyWithMaster( masterUri, masterClient, nioneoDataSource );
            console.log( "Store is consistent" );

            /*
             * Pull updates, since the store seems happy and everything. No matter how far back we are, this is just
             * one thread doing the pulling, while the guard is up. This will prevent a race between all transactions
             * that may start the moment the database becomes available, where all of them will pull the same txs from
             * the master but eventually only one will get to apply them.
             */
            console.log( "Catching up with master" );
            RequestContext context = requestContextFactory.newRequestContext( -1 );
            xaDataSourceManager.applyTransactions( masterClient.pullUpdates( context ) );
            console.log( "Now consistent with master" );
        }
        catch ( NoSuchLogVersionException e )
        {
            msgLog.logMessage( "Cannot catch up to master by pulling updates, because I cannot find the archived " +
                    "logical log file that has the transaction I would start from. I'm going to copy the whole " +
                    "store from the master instead." );
            try
            {
                stopServicesAndHandleBranchedStore( config.get( HaSettings.branched_data_policy ) );
            }
            catch ( Throwable throwable )
            {
                msgLog.warn( "Failed preparing for copying the store from the master instance", throwable );
            }
            throw e;
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

            throw upe;
        }
        catch ( MismatchingStoreIdException e )
        {
            console.log( "The store does not represent the same database as master. " +
                    "Will remove and fetch a new one from master" );
            if ( nioneoDataSource.getNeoStore().getLastCommittedTx() == 1 )
            {
                msgLog.warn( "Found and deleting empty store with mismatching store id " + e.getMessage() );
                stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );
            }
            else
            {
                msgLog.error( "Store cannot participate in cluster due to mismatching store IDs" );
            }
            throw e;
        }
    }

    void checkMyStoreIdAndMastersStoreId( NeoStoreXaDataSource nioneoDataSource, boolean masterIsOld )
    {
        if ( !masterIsOld )
        {
            StoreId myStoreId = nioneoDataSource.getStoreId();

            ClusterMembers clusterMembers = resolver.resolveDependency( ClusterMembers.class );
            ClusterMember master = first( filter( inRole( MASTER ), clusterMembers.getMembers() ) );
            StoreId masterStoreId = master.getStoreId();

            if ( !myStoreId.equals( masterStoreId ) )
            {
                throw new MismatchingStoreIdException( myStoreId, master.getStoreId() );
            }
            else if ( !myStoreId.equalsByUpgradeId( master.getStoreId() ) )
            {
                throw new BranchedDataException( "My store with " + myStoreId + " was updated independently from " +
                                                 "master's store " + masterStoreId );
            }
        }
    }

    private URI startHaCommunication( LifeSupport haCommunicationLife, HaXaDataSourceManager xaDataSourceManager,
                                      NeoStoreXaDataSource nioneoDataSource, URI me, URI masterUri, StoreId storeId )
    {
        MasterClient master = haCommunicationLife.add( newMasterClient( masterUri, nioneoDataSource.getStoreId() ) );

        Slave slaveImpl = new SlaveImpl( nioneoDataSource.getStoreId(), master,
                new RequestContextFactory( getServerId( masterUri ).toIntegerIndex(), xaDataSourceManager,
                        resolver ), xaDataSourceManager );

        SlaveServer server = new SlaveServer( slaveImpl, serverConfig(), logging,
                resolver.resolveDependency( Monitors.class ) );

        masterDelegateHandler.setDelegate( master );

        haCommunicationLife.add( slaveImpl );
        haCommunicationLife.add( server );
        haCommunicationLife.start();

        URI slaveHaURI = createHaURI( me, server );
        clusterMemberAvailability.memberIsAvailable( HighAvailabilityModeSwitcher.SLAVE, slaveHaURI, storeId );

        return slaveHaURI;
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

    private URI createHaURI( URI me, Server<?, ?> server )
    {
        String hostString = ServerUtil.getHostString( server.getSocketAddress() );
        int port = server.getSocketAddress().getPort();
        InstanceId serverId = config.get( ClusterSettings.server_id );
        String host = hostString.contains( HighAvailabilityModeSwitcher.INADDR_ANY ) ? me.getHost() : hostString;
        return URI.create( "ha://" + host + ":" + port + "?serverId=" + serverId );
    }

    private void copyStoreFromMaster( final MasterClient masterClient,
                                      CancellationRequest cancellationRequest ) throws Throwable
    {
        FileSystemAbstraction fs = resolver.resolveDependency( FileSystemAbstraction.class );
        // Must be called under lock on XaDataSourceManager
        // Remove the current store - neostore file is missing, nothing we can really do
        stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );

        // This will move the copied db to the graphdb location
        console.log( "Copying store from master" );
        RemoteStoreCopier storeCopier = new RemoteStoreCopier( config, kernelExtensions, console, logging, fs,
                monitors );
        storeCopier.copyStore( new RemoteStoreCopier.StoreCopyRequester()
        {
            @Override
            public Response<?> copyStore( StoreWriter writer )
            {
                return masterClient.copyStore( new RequestContext( 0,
                        config.get( ClusterSettings.server_id ).toIntegerIndex(), 0, new RequestContext.Tx[0], 0,
                        0 ), writer );
            }

            @Override
            public void done()
            {
            }
        }, cancellationRequest );

        startServicesAgain();
        console.log( "Finished copying store from master" );
    }

    MasterClient newMasterClient( URI masterUri, StoreId storeId )
    {
        MasterClient masterClient = masterClientResolver.instantiate( masterUri.getHost(), masterUri.getPort(),
                resolver.resolveDependency( Monitors.class ), storeId );
        if ( masterClient.getProtocolVersion().compareTo( MasterClient210.PROTOCOL_VERSION ) < 0 )
        {
            idGeneratorFactory.enableCompatibilityMode();
        }
        return masterClient;
    }

    private void startServicesAgain() throws Throwable
    {
        for ( Class<? extends Lifecycle> serviceClass : SERVICES_TO_RESTART_FOR_STORE_COPY )
        {
            resolver.resolveDependency( serviceClass ).start();
        }
    }

    void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy ) throws Throwable
    {
        for ( int i = SERVICES_TO_RESTART_FOR_STORE_COPY.length - 1; i >= 0; i-- )
        {
            Class<? extends Lifecycle> serviceClass = SERVICES_TO_RESTART_FOR_STORE_COPY[i];
            resolver.resolveDependency( serviceClass ).stop();
        }

        branchPolicy.handle( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) );
    }

    void checkDataConsistencyWithMaster( URI masterUri, Master master, NeoStoreXaDataSource nioneoDataSource )
            throws NoSuchLogVersionException
    {
        long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
        Pair<Integer, Long> myMaster;
        try
        {
            myMaster = nioneoDataSource.getMasterForCommittedTx( myLastCommittedTx );
        }
        catch ( NoSuchLogVersionException e )
        {
            msgLog.logMessage( "Failed to get master ID for txId " + myLastCommittedTx + ".", e );
            throw e;
        }
        catch ( Exception e )
        {
            throw new BranchedDataException( "Unable to gather data for mandatory sanity check. Details: " +
                    "Exception while getting master ID for txId " + myLastCommittedTx + ".", e );
        }

        HandshakeResult handshake;
        try ( Response<HandshakeResult> response =
                      master.handshake( myLastCommittedTx, nioneoDataSource.getStoreId() ) )
        {
            handshake = response.response();
            requestContextFactory.setEpoch( handshake.epoch() );
        }
        catch ( BranchedDataException e )
        {
            // Rethrow wrapped in a branched data exception on our side, to clarify where the problem originates.
            throw new BranchedDataException( "The database stored on this machine has diverged from that " +
                    "of the master. This will be automatically resolved.", e );
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
            String msg = "The cluster contains two logically different versions of the database.. This will be " +
                    "automatically resolved. Details: I (machineId:" + config.get( ClusterSettings.server_id ) +
                    ") think machineId for txId (" + myLastCommittedTx + ") is " + myMaster +
                    ", but master (machineId:" + getServerId( masterUri ) + ") says that it's " + handshake;
            throw new BranchedDataException( msg );
        }
        msgLog.logMessage( "Master id for last committed tx ok with highestTxId=" +
                myLastCommittedTx + " with masterId=" + myMaster, true );
    }

    private NeoStoreXaDataSource ensureDataSourceStarted( XaDataSourceManager xaDataSourceManager,
                                                          DependencyResolver resolver ) throws IOException
    {
        // Must be called under lock on XaDataSourceManager
        NeoStoreXaDataSource nioneoDataSource = (NeoStoreXaDataSource) xaDataSourceManager.getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        if ( nioneoDataSource == null )
        {
            Function<NeoStore, Function<List<LogEntry>, List<LogEntry>>> thing =
                    new Function<NeoStore, Function<List<LogEntry>, List<LogEntry>>>()
                    {
                        @Override
                        public Function<List<LogEntry>, List<LogEntry>> apply( NeoStore neoStore )
                        {
                            return new DenseNodeTransactionTranslator( neoStore );
                        }
                    };

            nioneoDataSource = new NeoStoreXaDataSource( config,
                    resolver.resolveDependency( LockService.class ),
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
                    resolver.resolveDependency( SchemaWriteGuard.class ),
                    resolver.resolveDependency( TransactionEventHandlers.class ),
                    monitors.newMonitor( IndexingService.Monitor.class ),
                    resolver.resolveDependency( FileSystemAbstraction.class ),
                    thing,
                    resolver.resolveDependency( StoreUpgrader.class ) );
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
}
