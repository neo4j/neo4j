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

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
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
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.ha.UpdatePuller;
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
import org.neo4j.kernel.impl.store.InconsistentlyUpgradedClusterException;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.UnableToCopyStoreFromOldMasterException;
import org.neo4j.kernel.impl.store.UnavailableMembersException;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.log.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.getServerId;
import static org.neo4j.kernel.ha.cluster.member.ClusterMembers.inRole;
import static org.neo4j.kernel.impl.store.NeoStore.isStorePresent;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class SwitchToSlave
{
    // TODO solve this with lifecycle instance grouping or something
    @SuppressWarnings("unchecked")
    private static final Class<? extends Lifecycle>[] SERVICES_TO_RESTART_FOR_STORE_COPY = new Class[]{
            StoreLockerLifecycleAdapter.class,
            NeoStoreDataSource.class,
            RequestContextFactory.class,
            TransactionCommittingResponseUnpacker.class,
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
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final MasterClientResolver masterClientResolver;
    private final ByteCounterMonitor byteCounterMonitor;
    private final RequestMonitor requestMonitor;

    public SwitchToSlave( ConsoleLogger console, Config config, DependencyResolver resolver,
                          HaIdGeneratorFactory idGeneratorFactory, Logging logging,
                          DelegateInvocationHandler<Master> masterDelegateHandler,
                          ClusterMemberAvailability clusterMemberAvailability,
                          RequestContextFactory requestContextFactory,
                          Iterable<KernelExtensionFactory<?>> kernelExtensions, ResponseUnpacker responseUnpacker,
                          ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        this.console = console;
        this.config = config;
        this.resolver = resolver;
        this.idGeneratorFactory = idGeneratorFactory;
        this.logging = logging;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.requestContextFactory = requestContextFactory;
        this.kernelExtensions = kernelExtensions;
        this.byteCounterMonitor = byteCounterMonitor;
        this.requestMonitor = requestMonitor;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.masterDelegateHandler = masterDelegateHandler;

        this.masterClientResolver = new MasterClientResolver( logging, responseUnpacker,
                config.get( HaSettings.read_timeout ).intValue(),
                config.get( HaSettings.lock_read_timeout ).intValue(),
                config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( HaSettings.com_chunk_size ).intValue() );
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

        idGeneratorFactory.switchToSlave();

        copyStoreFromMasterIfNeeded( masterUri, cancellationRequest );

        /*
         * The following check is mandatory, since the store copy can be cancelled and if it was actually
         * happening then we can't continue, as there is no store in place
         */
        if ( cancellationRequest.cancellationRequested() )
        {
            return null;
        }

        /*
         * We get here either with a fresh store from the master copy above so we need to
         * start the ds or we already had a store, so we have already started the ds. Either way,
         * make sure it's there.
         */
        NeoStoreDataSource nioneoDataSource = resolver.resolveDependency( NeoStoreDataSource.class );
        nioneoDataSource.afterModeSwitch();
        StoreId myStoreId = nioneoDataSource.getStoreId();

        boolean consistencyChecksExecutedSuccessfully = executeConsistencyChecks(
                myId, masterUri, nioneoDataSource, cancellationRequest );

        if ( !consistencyChecksExecutedSuccessfully )
        {
            return null;
        }

        if ( cancellationRequest.cancellationRequested() )
        {
            return null;
        }

        // no exception were thrown and we can proceed
        URI slaveUri = startHaCommunication( haCommunicationLife, nioneoDataSource, me, masterUri, myStoreId );

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
                }
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
                throw e;
            }
            finally
            {
                copyLife.shutdown();
            }
        }
    }

    private boolean executeConsistencyChecks( InstanceId myId, URI masterUri, NeoStoreDataSource nioneoDataSource,
                                              CancellationRequest cancellationRequest ) throws Throwable
    {
        LifeSupport consistencyCheckLife = new LifeSupport();
        try
        {
            StoreId myStoreId = nioneoDataSource.getStoreId();

            MasterClient masterClient = newMasterClient( masterUri, myStoreId, consistencyCheckLife );
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

            checkDataConsistency( masterClient, resolver.resolveDependency( RequestContextFactory.class ),
                    nioneoDataSource, masterUri, masterIsOld );
        }
        finally
        {
            consistencyCheckLife.shutdown();
        }
        return true;
    }

    private void checkDataConsistency( MasterClient masterClient, RequestContextFactory requestContextFactory,
                                       NeoStoreDataSource nioneoDataSource, URI masterUri, boolean masterIsOld )
            throws Throwable
    {
        TransactionIdStore txIdStore = nioneoDataSource.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        try
        {
            console.log( "Checking store consistency with master" );
            checkMyStoreIdAndMastersStoreId( nioneoDataSource, masterIsOld );
            checkDataConsistencyWithMaster( masterUri, masterClient, nioneoDataSource, txIdStore );
            console.log( "Store is consistent" );

            /*
             * Pull updates, since the store seems happy and everything. No matter how far back we are, this is just
             * one thread doing the pulling, while the guard is up. This will prevent a race between all transactions
             * that may start the moment the database becomes available, where all of them will pull the same txs from
             * the master but eventually only one will get to apply them.
             */
            RequestContext catchUpRequestContext = requestContextFactory.newRequestContext();
            console.log( "Catching up with master. I'm at " + catchUpRequestContext );

            masterClient.pullUpdates( catchUpRequestContext, new TxHandler()
            {
                @Override
                public void accept( CommittedTransactionRepresentation tx )
                {
                    long txId = tx.getCommitEntry().getTxId();
                    if ( txId % 50 == 0 )
                    {
                        console.log( "  ...still catching up with master, now at " + txId );
                    }
                }

                @Override
                public void done()
                {   // We print a message after the pullUpdates call as a whole anyway, so don't do anything here
                }
            } );

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
            }
            else
            {
                msgLog.error( "Store cannot participate in cluster due to mismatching store IDs", e );
            }
            throw e;
        }
    }

    private void checkMyStoreIdAndMastersStoreId( NeoStoreDataSource nioneoDataSource, boolean masterIsOld )
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

    private URI startHaCommunication( LifeSupport haCommunicationLife, NeoStoreDataSource nioneoDataSource,
                                      URI me, URI masterUri, StoreId storeId )
    {
        MasterClient master = newMasterClient( masterUri, nioneoDataSource.getStoreId(), haCommunicationLife );

        Slave slaveImpl = new SlaveImpl( nioneoDataSource.getStoreId(), resolver.resolveDependency( UpdatePuller.class ) );

        SlaveServer server = new SlaveServer( slaveImpl, serverConfig(), logging, byteCounterMonitor, requestMonitor);

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

        // This will move the copied db to the graphdb location
        console.log( "Copying store from master" );
        new StoreCopyClient( config, kernelExtensions, console, logging, fs ).copyStore(
                new StoreCopyClient.StoreCopyRequester()
                {
                    @Override
                    public Response<?> copyStore( StoreWriter writer )
                    {
                        return masterClient.copyStore( new RequestContext( 0,
                                config.get( ClusterSettings.server_id ).toIntegerIndex(), 0, BASE_TX_ID, 0, 0 ), writer );
                    }

                    @Override
                    public void done()
                    {   // Nothing to clean up here
                    }
                }, cancellationRequest );

        startServicesAgain();
        console.log( "Finished copying store from master" );
    }

    MasterClient newMasterClient( URI masterUri, StoreId storeId, LifeSupport life )
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

    private void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy ) throws Throwable
    {
        for ( int i = SERVICES_TO_RESTART_FOR_STORE_COPY.length - 1; i >= 0; i-- )
        {
            Class<? extends Lifecycle> serviceClass = SERVICES_TO_RESTART_FOR_STORE_COPY[i];
            resolver.resolveDependency( serviceClass ).stop();
        }

        branchPolicy.handle( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) );
    }

    private void checkDataConsistencyWithMaster( URI availableMasterId, Master master,
                                                 NeoStoreDataSource nioneoDataSource,
                                                 TransactionIdStore transactionIdStore )
            throws IOException
    {
        long myLastCommittedTx = transactionIdStore.getLastCommittedTransactionId();
        HandshakeResult handshake;
        try ( Response<HandshakeResult> response = master.handshake( myLastCommittedTx, nioneoDataSource.getStoreId() ) )
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

        final TransactionMetadataCache.TransactionMetadata metadata = nioneoDataSource.getDependencyResolver()
                .resolveDependency( LogicalTransactionStore.class ).getMetadataFor( myLastCommittedTx );

        int myMaster = metadata.getMasterId();
        long myChecksum = metadata.getChecksum();
        if ( myMaster != -1 &&
                ( myMaster != handshake.txAuthor() || myChecksum != handshake.txChecksum() ) )
        {
            String msg = "The cluster contains two logically different versions of the database.. This will be " +
                    "automatically resolved. Details: I (machineId:" + config.get( ClusterSettings.server_id ) +
                    ") think machineId for txId (" + myLastCommittedTx + ") is " + myMaster +
                    ", but master (machineId:" + getServerId( availableMasterId ) + ") says that it's " + handshake;
            throw new BranchedDataException( msg );
        }
        msgLog.logMessage( "Master id for last committed tx ok with highestTxId=" +
                myLastCommittedTx + " with masterId=" + myMaster, true );
    }
}
