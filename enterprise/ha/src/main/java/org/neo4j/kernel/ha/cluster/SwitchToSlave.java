/*
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

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.MasterClient210;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.UpdatePullerScheduler;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
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
import org.neo4j.kernel.ha.store.ForeignStoreException;
import org.neo4j.kernel.ha.store.UnableToCopyStoreFromOldMasterException;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.log.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.getServerId;
import static org.neo4j.kernel.ha.cluster.member.ClusterMembers.hasInstanceId;
import static org.neo4j.kernel.impl.store.NeoStore.isStorePresent;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class SwitchToSlave
{
    // TODO solve this with lifecycle instance grouping or something
    @SuppressWarnings("unchecked")
    private static final Class<? extends Lifecycle>[] SERVICES_TO_RESTART_FOR_STORE_COPY = new Class[]{
            StoreLockerLifecycleAdapter.class,
            DataSourceManager.class,
            RequestContextFactory.class,
            TransactionCommittingResponseUnpacker.class,
            IndexConfigStore.class,
            OnlineBackupKernelExtension.class,
    };

    public interface Monitor
    {
        void switchToSlaveStarted();
        void switchToSlaveCompleted( boolean wasSuccessful );

        void storeCopyStarted();
        void storeCopyCompleted( boolean wasSuccessful );

        void catchupStarted();
        void catchupCompleted();
    }

    private final Logging logging;
    private final StringLogger msgLog;
    private final ConsoleLogger console;
    private final Config config;
    private final DependencyResolver resolver;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final RequestContextFactory requestContextFactory;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final MasterClientResolver masterClientResolver;
    private final UpdatePuller updatePuller;
    private final ByteCounterMonitor byteCounterMonitor;
    private final RequestMonitor requestMonitor;
    private final PullerFactory updatePullerFactory;
    private final StoreCopyClient.Monitor storeCopyMonitor;
    private final Monitor monitor;

    public SwitchToSlave( ConsoleLogger console, Config config, DependencyResolver resolver,
            HaIdGeneratorFactory idGeneratorFactory, Logging logging,
            DelegateInvocationHandler<Master> masterDelegateHandler,
            ClusterMemberAvailability clusterMemberAvailability,
            RequestContextFactory requestContextFactory,
            Iterable<KernelExtensionFactory<?>> kernelExtensions, MasterClientResolver masterClientResolver,
            UpdatePuller updatePuller, PullerFactory pullerFactory, ByteCounterMonitor byteCounterMonitor,
            RequestMonitor requestMonitor, Monitor monitor, StoreCopyClient.Monitor storeCopyMonitor )
    {
        this.console = console;
        this.config = config;
        this.resolver = resolver;
        this.idGeneratorFactory = idGeneratorFactory;
        this.logging = logging;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.requestContextFactory = requestContextFactory;
        this.kernelExtensions = kernelExtensions;
        this.updatePuller = updatePuller;
        this.byteCounterMonitor = byteCounterMonitor;
        this.requestMonitor = requestMonitor;
        this.storeCopyMonitor = storeCopyMonitor;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.masterDelegateHandler = masterDelegateHandler;
        this.updatePullerFactory = pullerFactory;
        this.monitor = monitor;
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
        URI slaveUri;
        boolean success = false;

        monitor.switchToSlaveStarted();

        try
        {
            InstanceId myId = config.get( ClusterSettings.server_id );

            console.log( "ServerId " + myId + ", moving to slave for master " + masterUri );

            assert masterUri != null; // since we are here it must already have been set from outside

            idGeneratorFactory.switchToSlave();

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
            NeoStoreDataSource neoDataSource = resolver.resolveDependency( NeoStoreDataSource.class );
            neoDataSource.afterModeSwitch();
            StoreId myStoreId = neoDataSource.getStoreId();

            boolean consistencyChecksExecutedSuccessfully = executeConsistencyChecks( masterUri, neoDataSource,
                    cancellationRequest );

            if ( !consistencyChecksExecutedSuccessfully )
            {
                msgLog.info( "Switch to slave cancelled due to consistency check failure." );
                return null;
            }

            if ( cancellationRequest.cancellationRequested() )
            {
                msgLog.info( "Switch to slave cancelled after consistency checks." );
                return null;
            }

            // no exception were thrown and we can proceed
            slaveUri = startHaCommunication( haCommunicationLife, neoDataSource, me, masterUri, myStoreId, cancellationRequest );
            if ( slaveUri == null )
            {
                msgLog.info( "Switch to slave unable to connect." );
                return null;
            }

            success = true;
            console.log( "ServerId " + myId + ", successfully moved to slave for master " + masterUri );
        }
        finally
        {
            monitor.switchToSlaveCompleted( success );
        }

        return slaveUri;
    }

    private void copyStoreFromMasterIfNeeded( URI masterUri, CancellationRequest cancellationRequest ) throws Throwable
    {
        if ( !isStorePresent( resolver.resolveDependency( FileSystemAbstraction.class ), config ) )
        {
            boolean success = false;
            monitor.storeCopyStarted();
            LifeSupport copyLife = new LifeSupport();
            try
            {
                MasterClient masterClient = newMasterClient( masterUri, null, copyLife );
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
                    success = true;
                }
            }
            finally
            {
                monitor.storeCopyCompleted( success );
                copyLife.shutdown();
            }
        }
    }

    private boolean executeConsistencyChecks( URI masterUri, NeoStoreDataSource neoDataSource,
                                              CancellationRequest cancellationRequest ) throws Throwable
    {
        LifeSupport consistencyCheckLife = new LifeSupport();
        try
        {
            StoreId myStoreId = neoDataSource.getStoreId();

            MasterClient masterClient = newMasterClient( masterUri, myStoreId, consistencyCheckLife );
            consistencyCheckLife.start();

            if ( cancellationRequest.cancellationRequested() )
            {
                return false;
            }

            checkDataConsistency( masterClient, neoDataSource, masterUri );
        }
        finally
        {
            consistencyCheckLife.shutdown();
        }
        return true;
    }

    void checkDataConsistency( MasterClient masterClient, NeoStoreDataSource neoDataSource, URI masterUri )
            throws Throwable
    {
        TransactionIdStore txIdStore = neoDataSource.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        try
        {
            console.log( "Checking store consistency with master" );
            checkMyStoreIdAndMastersStoreId( neoDataSource, masterUri );
            checkDataConsistencyWithMaster( masterUri, masterClient, neoDataSource, txIdStore );
            console.log( "Store is consistent" );
        }
        catch ( StoreUnableToParticipateInClusterException upe )
        {
            console.log( "The store is inconsistent. Will treat it as branched and fetch a new one from the master" );
            msgLog.warn( "Current store is unable to participate in the cluster; fetching new store from master", upe );
            try
            {
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
            console.log( "The store does not represent the same database as master. Will remove and fetch a new one from master" );
            if ( txIdStore.getLastCommittedTransactionId() == BASE_TX_ID )
            {
                msgLog.warn( "Found and deleting empty store with mismatching store id", e );
                stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );
                throw e;
            }

            msgLog.error( "Store cannot participate in cluster due to mismatching store IDs", e );
            throw new ForeignStoreException( e.getExpected(), e.getEncountered() );
        }
    }

    private void checkMyStoreIdAndMastersStoreId( NeoStoreDataSource neoDataSource, URI masterUri )
    {
        StoreId myStoreId = neoDataSource.getStoreId();

        ClusterMembers clusterMembers = resolver.resolveDependency( ClusterMembers.class );

        ClusterMember master = single(
                filter( hasInstanceId( HighAvailabilityModeSwitcher.getServerId( masterUri ) ),
                        clusterMembers.getMembers() ) );

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

    private URI startHaCommunication( LifeSupport haCommunicationLife, NeoStoreDataSource neoDataSource,
            URI me, URI masterUri, StoreId storeId, CancellationRequest cancellationRequest )
            throws IllegalArgumentException, InterruptedException
    {
        MasterClient master = newMasterClient( masterUri, neoDataSource.getStoreId(), haCommunicationLife );

        TransactionObligationFulfiller obligationFulfiller = resolver.resolveDependency(
                TransactionObligationFulfiller.class );
        UpdatePullerScheduler updatePullerScheduler = updatePullerFactory.createUpdatePullerScheduler( updatePuller );


        Slave slaveImpl = new SlaveImpl( obligationFulfiller );

        SlaveServer server = new SlaveServer( slaveImpl, serverConfig(), logging, byteCounterMonitor, requestMonitor);

        if ( cancellationRequest.cancellationRequested() )
        {
            return null;
        }

        masterDelegateHandler.setDelegate( master );

        haCommunicationLife.add( updatePullerScheduler );
        haCommunicationLife.add( server );
        haCommunicationLife.start();


        /*
         * Take the opportunity to catch up with master, now that we're alone here, right before we
         * drop the availability guard, so that other transactions might start.
         */
        if ( !catchUpWithMaster( updatePuller ) )
        {
            return null;
        }

        URI slaveHaURI = createHaURI( me, server );
        clusterMemberAvailability.memberIsAvailable( HighAvailabilityModeSwitcher.SLAVE, slaveHaURI, storeId );

        return slaveHaURI;
    }

    private boolean catchUpWithMaster(UpdatePuller updatePuller)
            throws IllegalArgumentException, InterruptedException
    {
        monitor.catchupStarted();
        RequestContext catchUpRequestContext = requestContextFactory.newRequestContext();
        console.log( "Catching up with master. I'm at " + catchUpRequestContext );

        if ( !updatePuller.tryPullUpdates() )
        {
            return false;
        }

        console.log( "Now caught up with master" );
        monitor.catchupCompleted();
        return true;
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
        PageCache pageCache = resolver.resolveDependency( PageCache.class );

        // This will move the copied db to the graphdb location
        console.log( "Copying store from master" );
        new StoreCopyClient( config, kernelExtensions, console, logging, fs, pageCache, storeCopyMonitor ).copyStore(
                new StoreCopyClient.StoreCopyRequester()
                {
                    @Override
                    public Response<?> copyStore( StoreWriter writer )
                    {
                        return masterClient.copyStore( new RequestContext( 0,
                                config.get( ClusterSettings.server_id ).toIntegerIndex(), 0, BASE_TX_ID, 0 ), writer );
                    }

                    @Override
                    public void done()
                    {   // Nothing to clean up here
                    }
                }, cancellationRequest );

        startServicesAgain();
        console.log( "Finished copying store from master" );
    }

    private MasterClient newMasterClient( URI masterUri, StoreId storeId, LifeSupport life )
    {
        MasterClient masterClient = masterClientResolver.instantiate( masterUri.getHost(), masterUri.getPort(),
                resolver.resolveDependency( Monitors.class ), storeId, life );
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

    private void checkDataConsistencyWithMaster( URI availableMasterId, Master master,
                                                 NeoStoreDataSource neoDataSource,
                                                 TransactionIdStore transactionIdStore )
    {
        TransactionId myLastCommittedTxData = transactionIdStore.getLastCommittedTransaction();
        long myLastCommittedTx = myLastCommittedTxData.transactionId();
        HandshakeResult handshake;
        try ( Response<HandshakeResult> response = master.handshake( myLastCommittedTx, neoDataSource.getStoreId() ) )
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

        long myChecksum = myLastCommittedTxData.checksum();
        if ( myChecksum != handshake.txChecksum() )
        {
            String msg = "The cluster contains two logically different versions of the database.. This will be " +
                    "automatically resolved. Details: I (server_id:" + config.get( ClusterSettings.server_id ) +
                    ") think checksum for txId (" + myLastCommittedTx + ") is " + myChecksum +
                    ", but master (server_id:" + getServerId( availableMasterId ) + ") says that it's " +
                    handshake.txChecksum() + ", where handshake is " + handshake;
            throw new BranchedDataException( msg );
        }
        msgLog.info( "Checksum for last committed tx ok with lastTxId=" +
                myLastCommittedTx + " with checksum=" + myChecksum, true );
    }
}
