/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha.cluster;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.storecopy.MoveAfterCopy;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.UpdatePullerScheduler;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveImpl;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.store.UnableToCopyStoreFromOldMasterException;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.log.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.locker.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.kernel.ha.cluster.member.ClusterMembers.hasInstanceId;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.getServerId;
import static org.neo4j.kernel.impl.store.NeoStores.isStorePresent;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public abstract class SwitchToSlave
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
            FileSystemWatcherService.class
    };
    private final StoreCopyClient storeCopyClient;
    private final Function<Slave,SlaveServer> slaveServerFactory;
    protected final UpdatePuller updatePuller;
    protected final Monitors monitors;
    final Log userLog;
    final Log msgLog;
    protected final Config config;
    protected final DependencyResolver resolver;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    protected final RequestContextFactory requestContextFactory;
    private final MasterClientResolver masterClientResolver;
    private final PullerFactory updatePullerFactory;
    protected final Monitor monitor;
    protected final File storeDir;
    protected final PageCache pageCache;

    private final Supplier<NeoStoreDataSource> neoDataSourceSupplier;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final TransactionStats transactionCounters;

    SwitchToSlave( HaIdGeneratorFactory idGeneratorFactory, DependencyResolver resolver, Monitors monitors,
                   RequestContextFactory requestContextFactory, DelegateInvocationHandler<Master>
                           masterDelegateHandler, ClusterMemberAvailability clusterMemberAvailability,
                   MasterClientResolver masterClientResolver, Monitor monitor, PullerFactory pullerFactory,
                   UpdatePuller updatePuller, Function<Slave, SlaveServer> slaveServerFactory, Config config,
                   LogService logService, PageCache pageCache, File storeDir,
                   Supplier<TransactionIdStore> transactionIdStoreSupplier, TransactionStats
                           transactionCounters, Supplier<NeoStoreDataSource> neoDataSourceSupplier, StoreCopyClient storeCopyClient )
    {
        this.idGeneratorFactory = idGeneratorFactory;
        this.resolver = resolver;
        this.monitors = monitors;
        this.requestContextFactory = requestContextFactory;
        this.masterDelegateHandler = masterDelegateHandler;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.masterClientResolver = masterClientResolver;
        this.userLog = logService.getUserLog( getClass() );
        this.msgLog = logService.getInternalLog( getClass() );
        this.monitor = monitor;
        this.updatePullerFactory = pullerFactory;
        this.updatePuller = updatePuller;
        this.slaveServerFactory = slaveServerFactory;
        this.config = config;
        this.pageCache = pageCache;
        this.storeDir = storeDir;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.transactionCounters = transactionCounters;
        this.neoDataSourceSupplier = neoDataSourceSupplier;
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
     */
    public URI switchToSlave( LifeSupport haCommunicationLife, URI me, URI masterUri,
                              CancellationRequest cancellationRequest ) throws Throwable
    {
        URI slaveUri;
        boolean success = false;

        monitor.switchToSlaveStarted();

        // Wait a short while for current transactions to stop first, just to be nice.
        // We can't wait forever since switching to our designated role is quite important.
        Clock clock = Clocks.systemClock();
        long deadline = clock.millis() + config.get( HaSettings.internal_state_switch_timeout ).toMillis();
        while ( transactionCounters.getNumberOfActiveTransactions() > 0 && clock.millis() < deadline )
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

    void checkMyStoreIdAndMastersStoreId( StoreId myStoreId, URI masterUri, DependencyResolver resolver )
    {
        ClusterMembers clusterMembers = resolver.resolveDependency( ClusterMembers.class );
        InstanceId serverId = HighAvailabilityModeSwitcher.getServerId( masterUri );
        Iterable<ClusterMember> members = clusterMembers.getMembers();
        ClusterMember master = firstOrNull( filter( hasInstanceId( serverId ), members ) );
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
                                      URI me, URI masterUri, StoreId storeId, CancellationRequest
                                              cancellationRequest )
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
        InetSocketAddress serverSocketAddress = server.getSocketAddress();
        String hostString = ServerUtil.getHostString( serverSocketAddress );

        String host = isWildcard( hostString ) ? me.getHost() : hostString;
        host = ensureWrapForIpv6Uri( host );

        InstanceId serverId = config.get( ClusterSettings.server_id );
        return URI.create( "ha://" + host + ":" + serverSocketAddress.getPort() + "?serverId=" + serverId );
    }

    private String ensureWrapForIpv6Uri( String host )
    {
        if ( host.contains( ":" ) && !host.contains( "[" ) )
        {
            host = "[" + host + "]";
        }
        return host;
    }

    private static boolean isWildcard( String hostString )
    {
        return hostString.contains( "0.0.0.0" ) || hostString.contains( "::" ) || hostString.contains( "0:0:0:0:0:0:0:0" );
    }

    MasterClient newMasterClient( URI masterUri, URI me, StoreId storeId, LifeSupport life )
    {
        return masterClientResolver.instantiate( masterUri.getHost(), masterUri.getPort(),
                me.getHost(), monitors, storeId, life );
    }

    private void startServicesAgain() throws Throwable
    {
        msgLog.debug( "Starting services again" );
        for ( Class<? extends Lifecycle> serviceClass : SwitchToSlave.SERVICES_TO_RESTART_FOR_STORE_COPY )
        {
            resolver.resolveDependency( serviceClass ).start();
        }
    }

    void checkDataConsistencyWithMaster( URI availableMasterId, Master master,
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
                myLastCommittedTx + " with checksum=" + myChecksum );
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
                    copyStoreFromMaster( masterClient, cancellationRequest, MoveAfterCopy.moveReplaceExisting() );
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

            checkDataConsistency( masterClient, txIdStore, storeId, masterUri, me, cancellationRequest );
        }
        finally
        {
            consistencyCheckLife.shutdown();
        }
        return true;
    }

    abstract void checkDataConsistency( MasterClient masterClient, TransactionIdStore txIdStore, StoreId storeId, URI
            masterUri, URI me, CancellationRequest cancellationRequest )
            throws Throwable;

    void cleanStoreDir() throws IOException
    {
        // Tests verify that this method is called
        StoreUtil.cleanStoreDir( storeDir, pageCache );
    }

    void stopServices() throws Exception
    {
        msgLog.debug( "Stopping services to handle branched store" );
        for ( int i = SERVICES_TO_RESTART_FOR_STORE_COPY.length - 1; i >= 0; i-- )
        {
            Class<? extends Lifecycle> serviceClass = SERVICES_TO_RESTART_FOR_STORE_COPY[i];
            try
            {
                resolver.resolveDependency( serviceClass ).stop();
            }
            catch ( Exception exception )
            {
                throw exception;
            }
            catch ( Throwable throwable )
            {
                throw new Exception( "Unexpected error while stopping services to handle branched data", throwable );
            }
        }
    }

    void copyStoreFromMaster( MasterClient masterClient, CancellationRequest cancellationRequest,
                              MoveAfterCopy moveAfterCopy )
            throws Throwable
    {
        try
        {
            userLog.info( "Copying store from master" );
            StoreCopyClient.StoreCopyRequester requester = new StoreCopyClient.StoreCopyRequester()
            {
                @Override
                public Response<?> copyStore( StoreWriter writer )
                {
                    return masterClient.copyStore( new RequestContext( 0,
                                    config.get( ClusterSettings.server_id ).toIntegerIndex(), 0, BASE_TX_ID, 0 ),
                            writer );
                }

                @Override
                public void done()
                {   // Nothing to clean up here
                }
            };
            MoveAfterCopy moveAfterCopyWithLogging = ( moves, fromDirectory, toDirectory ) ->
            {
                userLog.info( "Copied store from master to " + fromDirectory );
                msgLog.info( "Starting post copy operation to move store from " + fromDirectory + " to " + storeDir );
                moveAfterCopy.move( moves, fromDirectory, toDirectory );
            };
            storeCopyClient.copyStore(
                    requester, cancellationRequest, moveAfterCopyWithLogging );

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

    /**
     * Monitors events in {@link SwitchToSlave}
     */
    public interface Monitor
    {
        /**
         * Called before any other slave-switching code is executed.
         */
        default void switchToSlaveStarted()
        {   // no-op by default
        }

        /**
         * Called after all slave-switching code has been executed, regardless of whether it was successful or not.
         *
         * @param wasSuccessful whether or not the slave switch was successful. Depending on the type of failure
         * other failure handling outside this class kicks in and there may be a switch retry later.
         */
        default void switchToSlaveCompleted( boolean wasSuccessful )
        {   // no-op by default
        }

        /**
         * A full store-copy is required, either if this is the first time this db starts up or if this
         * store has branched and needs to fetch a new copy from master.
         */
        default void storeCopyStarted()
        {   // no-op by default
        }

        /**
         * A full store-copy has completed.
         *
         * @param wasSuccessful whether or not this store-copy was successful.
         */
        default void storeCopyCompleted( boolean wasSuccessful )
        {   // no-op by default
        }

        /**
         * After a successful handshake with master an optimized catch-up is performed.
         * This call marks the start of that.
         */
        default void catchupStarted()
        {   // no-op by default
        }

        /**
         * This db is now caught up with the master.
         */
        default void catchupCompleted()
        {   // no-op by default
        }
    }
}
