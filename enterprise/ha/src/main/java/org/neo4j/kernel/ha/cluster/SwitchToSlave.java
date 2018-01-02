/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
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
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.StoreUtil;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.getServerId;
import static org.neo4j.kernel.ha.cluster.member.ClusterMembers.hasInstanceId;
import static org.neo4j.kernel.impl.store.NeoStores.isStorePresent;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class SwitchToSlave
{
    // TODO solve this with lifecycle instance grouping or something
    @SuppressWarnings( "unchecked" )
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

    private final File storeDir;
    private final Supplier<NeoStoreDataSource> neoDataSourceSupplier;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final Function<Slave,SlaveServer> slaveServerFactory;
    private final UpdatePuller updatePuller;
    private final PageCache pageCache;
    private final Monitors monitors;
    private final TransactionCounters transactionCounters;

    private final Log userLog;
    private final Log msgLog;
    private final Config config;
    private final DependencyResolver resolver;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final RequestContextFactory requestContextFactory;
    private final MasterClientResolver masterClientResolver;
    private final StoreCopyClient storeCopyClient;
    private final PullerFactory updatePullerFactory;
    private final Monitor monitor;

    public SwitchToSlave( File storeDir,
            LogService logService,
            FileSystemAbstraction fileSystemAbstraction,
            Config config,
            DependencyResolver resolver,
            HaIdGeneratorFactory idGeneratorFactory,
            DelegateInvocationHandler<Master> masterDelegateHandler,
            ClusterMemberAvailability clusterMemberAvailability,
            RequestContextFactory requestContextFactory,
            PullerFactory pullerFactory,
            Iterable<KernelExtensionFactory<?>> kernelExtensions,
            MasterClientResolver masterClientResolver,
            Monitor monitor,
            StoreCopyClient.Monitor storeCopyMonitor,
            Supplier<NeoStoreDataSource> neoDataSourceSupplier,
            Supplier<TransactionIdStore> transactionIdStoreSupplier,
            Function<Slave,SlaveServer> slaveServerFactory,
            UpdatePuller updatePuller,
            PageCache pageCache,
            Monitors monitors,
            TransactionCounters transactionCounters )
    {
        this( storeDir,
                logService,
                config,
                resolver,
                idGeneratorFactory,
                masterDelegateHandler,
                clusterMemberAvailability,
                requestContextFactory,
                pullerFactory,
                masterClientResolver,
                monitor,
                new StoreCopyClient( storeDir, config, kernelExtensions, logService.getUserLogProvider(),
                        fileSystemAbstraction, pageCache, storeCopyMonitor, false ),
                neoDataSourceSupplier,
                transactionIdStoreSupplier,
                slaveServerFactory,
                updatePuller,
                pageCache,
                monitors,
                transactionCounters );
    }

    SwitchToSlave( File storeDir,
            LogService logService,
            Config config,
            DependencyResolver resolver,
            HaIdGeneratorFactory idGeneratorFactory,
            DelegateInvocationHandler<Master> masterDelegateHandler,
            ClusterMemberAvailability clusterMemberAvailability,
            RequestContextFactory requestContextFactory,
            PullerFactory pullerFactory,
            MasterClientResolver masterClientResolver,
            Monitor monitor,
            StoreCopyClient storeCopyClient,
            Supplier<NeoStoreDataSource> neoDataSourceSupplier,
            Supplier<TransactionIdStore> transactionIdStoreSupplier,
            Function<Slave,SlaveServer> slaveServerFactory,
            UpdatePuller updatePuller,
            PageCache pageCache,
            Monitors monitors,
            TransactionCounters transactionCounters )
    {
        this.neoDataSourceSupplier = neoDataSourceSupplier;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.slaveServerFactory = slaveServerFactory;
        this.updatePuller = updatePuller;
        this.pageCache = pageCache;
        this.monitors = monitors;
        this.transactionCounters = transactionCounters;
        this.userLog = logService.getUserLog( getClass() );
        this.storeDir = storeDir;
        this.config = config;
        this.resolver = resolver;
        this.idGeneratorFactory = idGeneratorFactory;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.requestContextFactory = requestContextFactory;
        this.msgLog = logService.getInternalLog( getClass() );
        this.masterDelegateHandler = masterDelegateHandler;
        this.updatePullerFactory = pullerFactory;
        this.monitor = monitor;
        this.masterClientResolver = masterClientResolver;
        this.storeCopyClient = storeCopyClient;
    }

    /**
     * Performs a switch to the slave state. Starts the communication endpoints, switches components to the slave state
     * and ensures that the current database is appropriate for this cluster. It also broadcasts the appropriate
     * Slave Is Available event
     *
     * @param haCommunicationLife The LifeSupport instance to register the network facilities required for
     *                            communication with the rest of the cluster
     * @param me The URI this instance must bind to
     * @param masterUri The URI of the master for which this instance must become slave to
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

        // Wait a short while for current transactions to stop first, just to be nice.
        // We can't wait forever since switching to our designated role is quite important.
        long deadline = SYSTEM_CLOCK.currentTimeMillis() + config.get( HaSettings.internal_state_switch_timeout );
        while ( transactionCounters.getNumberOfActiveTransactions() > 0 && SYSTEM_CLOCK.currentTimeMillis() < deadline )
        {
            parkNanos( MILLISECONDS.toNanos( 10 ) );
        }

        try
        {
            InstanceId myId = config.get( ClusterSettings.server_id );

            userLog.info( "ServerId %s, moving to slave for master %s", myId, masterUri );

            assert masterUri != null; // since we are here it must already have been set from outside

            idGeneratorFactory.switchToSlave();

            copyStoreFromMasterIfNeeded( masterUri, me, cancellationRequest );

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
            NeoStoreDataSource neoDataSource = neoDataSourceSupplier.get();
            neoDataSource.afterModeSwitch();
            StoreId myStoreId = neoDataSource.getStoreId();

            boolean consistencyChecksExecutedSuccessfully = executeConsistencyChecks(
                    transactionIdStoreSupplier.get(), masterUri, me, myStoreId, cancellationRequest );

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
            userLog.info( "ServerId %s, successfully moved to slave for master %s", myId, masterUri );
        }
        finally
        {
            monitor.switchToSlaveCompleted( success );
        }

        return slaveUri;
    }

    private void copyStoreFromMasterIfNeeded( URI masterUri, URI me, CancellationRequest cancellationRequest )
            throws Throwable
    {
        if ( !isStorePresent( pageCache, storeDir ) )
        {
            boolean success = false;
            monitor.storeCopyStarted();
            LifeSupport copyLife = new LifeSupport();
            try
            {
                MasterClient masterClient = newMasterClient( masterUri, me, null, copyLife );
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

    private boolean executeConsistencyChecks( TransactionIdStore txIdStore,
            URI masterUri, URI me,
            StoreId storeId,
            CancellationRequest cancellationRequest ) throws Throwable
    {
        LifeSupport consistencyCheckLife = new LifeSupport();
        try
        {
            MasterClient masterClient = newMasterClient( masterUri, me, storeId, consistencyCheckLife );
            consistencyCheckLife.start();

            if ( cancellationRequest.cancellationRequested() )
            {
                return false;
            }

            checkDataConsistency( masterClient, txIdStore, storeId, masterUri );
        }
        finally
        {
            consistencyCheckLife.shutdown();
        }
        return true;
    }

    void checkDataConsistency( MasterClient masterClient, TransactionIdStore txIdStore, StoreId storeId, URI masterUri )
        throws Throwable
    {
        try
        {
            userLog.info( "Checking store consistency with master" );
            checkMyStoreIdAndMastersStoreId( storeId, masterUri );
            checkDataConsistencyWithMaster( masterUri, masterClient, storeId, txIdStore );
            userLog.info( "Store is consistent" );
        }
        catch ( StoreUnableToParticipateInClusterException upe )
        {
            userLog.info( "The store is inconsistent. Will treat it as branched and fetch a new one from the master" );
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
            userLog.info(
                    "The store does not represent the same database as master. Will remove and fetch a new one from " +
                    "master" );
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

    private void checkMyStoreIdAndMastersStoreId( StoreId myStoreId, URI masterUri )
    {
        ClusterMembers clusterMembers = resolver.resolveDependency( ClusterMembers.class );
        InstanceId serverId = HighAvailabilityModeSwitcher.getServerId( masterUri );
        Iterable<ClusterMember> members = clusterMembers.getMembers();
        ClusterMember master = first( filter( hasInstanceId( serverId ), members ) );
        if ( master == null )
        {
            throw new IllegalStateException( "Cannot find the master among " + members +
                                             " with master serverId=" + serverId + " and uri=" + masterUri );
        }

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
        MasterClient master = newMasterClient( masterUri, me, neoDataSource.getStoreId(), haCommunicationLife );

        TransactionObligationFulfiller obligationFulfiller =
                resolver.resolveDependency( TransactionObligationFulfiller.class );
        UpdatePullerScheduler updatePullerScheduler = updatePullerFactory.createUpdatePullerScheduler( updatePuller );

        Slave slaveImpl = new SlaveImpl( obligationFulfiller );

        SlaveServer server = slaveServerFactory.apply( slaveImpl );

        if ( cancellationRequest.cancellationRequested() )
        {
            msgLog.info( "Switch to slave cancelled, unable to start HA-communication" );
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

    private boolean catchUpWithMaster( UpdatePuller updatePuller ) throws IllegalArgumentException, InterruptedException
    {
        monitor.catchupStarted();
        RequestContext catchUpRequestContext = requestContextFactory.newRequestContext();
        userLog.info( "Catching up with master. I'm at %s", catchUpRequestContext );

        if ( !updatePuller.tryPullUpdates() )
        {
            return false;
        }

        userLog.info( "Now caught up with master" );
        monitor.catchupCompleted();
        return true;
    }

    private URI createHaURI( URI me, Server<?,?> server )
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
        try
        {
            // This will move the copied db to the graphdb location
            userLog.info( "Copying store from master" );
            storeCopyClient.copyStore(
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
            userLog.info( "Finished copying store from master" );
        }
        catch ( Throwable t )
        {
            // Delete store so that we can copy from master without conflicts next time
            cleanStoreDir();
            throw t;
        }
    }

    void cleanStoreDir() throws IOException
    {
        // Tests verify that this method is called
        StoreUtil.cleanStoreDir( storeDir );
    }

    private MasterClient newMasterClient( URI masterUri, URI me, StoreId storeId, LifeSupport life )
    {
        MasterClient masterClient = masterClientResolver.instantiate( masterUri.getHost(), masterUri.getPort(),
                me.getHost(), monitors, storeId, life );
        if ( masterClient.getProtocolVersion().compareTo( MasterClient210.PROTOCOL_VERSION ) < 0 )
        {
            idGeneratorFactory.enableCompatibilityMode();
        }
        return masterClient;
    }

    private void startServicesAgain() throws Throwable
    {
        msgLog.debug( "Starting services again" );
        for ( Class<? extends Lifecycle> serviceClass : SERVICES_TO_RESTART_FOR_STORE_COPY )
        {
            resolver.resolveDependency( serviceClass ).start();
        }
    }

    void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy ) throws Throwable
    {
        msgLog.debug( "Stopping services to handle branched store" );
        for ( int i = SERVICES_TO_RESTART_FOR_STORE_COPY.length - 1; i >= 0; i-- )
        {
            Class<? extends Lifecycle> serviceClass = SERVICES_TO_RESTART_FOR_STORE_COPY[i];
            resolver.resolveDependency( serviceClass ).stop();
        }

        branchPolicy.handle( storeDir );
    }

    private void checkDataConsistencyWithMaster( URI availableMasterId, Master master,
            StoreId storeId,
            TransactionIdStore transactionIdStore )
    {
        TransactionId myLastCommittedTxData = transactionIdStore.getLastCommittedTransaction();
        long myLastCommittedTx = myLastCommittedTxData.transactionId();
        HandshakeResult handshake;
        try ( Response<HandshakeResult> response = master.handshake( myLastCommittedTx, storeId ) )
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
