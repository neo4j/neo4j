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
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
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
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveImpl;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.getServerId;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.isStorePresent;

public class SwitchToSlave
{
    // TODO solve this with lifecycle instance grouping or something
    @SuppressWarnings("unchecked")
    private static final Class<? extends Lifecycle>[] SERVICES_TO_RESTART_FOR_STORE_COPY = new Class[]{
            StoreLockerLifecycleAdapter.class,
            NeoStoreXaDataSource.class,
            RequestContextFactory.class,
            TransactionCommittingResponseUnpacker.class,
    };

    private final Logging logging;
    private final StringLogger msgLog;
    private final ConsoleLogger console;
    private final Config config;
    private final DependencyResolver resolver;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final RequestContextFactory requestContextFactory;
    // TODO 2.2-future add something to monitor
    private final Monitors monitors;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final MasterClientResolver masterClientResolver;

    public SwitchToSlave( ConsoleLogger console, Config config, DependencyResolver resolver, HaIdGeneratorFactory
            idGeneratorFactory, Logging logging, DelegateInvocationHandler<Master> masterDelegateHandler,
                          ClusterMemberAvailability clusterMemberAvailability, RequestContextFactory
            requestContextFactory, Monitors monitors, Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        this.console = console;
        this.config = config;
        this.resolver = resolver;
        this.idGeneratorFactory = idGeneratorFactory;
        this.logging = logging;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.requestContextFactory = requestContextFactory;
        this.monitors = monitors;
        this.kernelExtensions = kernelExtensions;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.masterDelegateHandler = masterDelegateHandler;

        this.masterClientResolver = new MasterClientResolver( logging,
                config.get( HaSettings.read_timeout ).intValue(),
                config.get( HaSettings.lock_read_timeout ).intValue(),
                config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( HaSettings.com_chunk_size ).intValue()  );
    }

    public URI switchToSlave( LifeSupport haCommunicationLife, URI me, URI masterUri ) throws Throwable
    {
        console.log( "ServerId " + config.get( ClusterSettings.server_id ) + ", moving to slave for master " +
                masterUri );

        assert masterUri != null; // since we are here it must already have been set from outside

        idGeneratorFactory.switchToSlave();
        if ( !isStorePresent( resolver.resolveDependency( FileSystemAbstraction.class ), config ) )
        {
            copyStoreFromMaster( masterUri );
        }

        NeoStoreXaDataSource nioneoDataSource = resolver.resolveDependency( NeoStoreXaDataSource.class );
        nioneoDataSource.afterModeSwitch();

        checkDataConsistency( resolver.resolveDependency( RequestContextFactory.class ), nioneoDataSource, masterUri );

        URI slaveUri = startHaCommunication( haCommunicationLife, nioneoDataSource, me, masterUri );

        console.log( "ServerId " + config.get( ClusterSettings.server_id ) +
                ", successfully moved to slave for master " + masterUri );

        return slaveUri;
    }

    private void checkDataConsistency( RequestContextFactory requestContextFactory,
                                          NeoStoreXaDataSource nioneoDataSource, URI masterUri ) throws Throwable
    {
        // Must be called under lock on XaDataSourceManager
        LifeSupport checkConsistencyLife = new LifeSupport();
        TransactionIdStore txIdStore = null;
        try
        {
            MasterClient checkConsistencyMaster = newMasterClient( masterUri, nioneoDataSource.getStoreId(),
                    checkConsistencyLife );
            checkConsistencyLife.start();
            console.log( "Checking store consistency with master" );
            txIdStore = nioneoDataSource.getDependencyResolver().resolveDependency( TransactionIdStore.class );
            checkDataConsistencyWithMaster( masterUri, checkConsistencyMaster, nioneoDataSource, txIdStore );
            console.log( "Store is consistent" );

            /*
             * Pull updates, since the store seems happy and everything. No matter how far back we are, this is just
             * one thread doing the pulling, while the guard is up. This will prevent a race between all transactions
             * that may start the moment the database becomes available, where all of them will pull the same txs from
             * the master but eventually only one will get to apply them.
             */
            console.log( "Catching up with master" );

            resolver.resolveDependency( TransactionCommittingResponseUnpacker.class ).
                    unpackResponse( checkConsistencyMaster.pullUpdates( requestContextFactory.newRequestContext() ) );
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
            if ( txIdStore != null && txIdStore.getLastCommittedTransactionId() == 0 )
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
        finally
        {
            checkConsistencyLife.shutdown();
        }
    }

    private URI startHaCommunication( LifeSupport haCommunicationLife, NeoStoreXaDataSource nioneoDataSource,
                                      URI me, URI masterUri )
    {
        MasterClient master = newMasterClient( masterUri, nioneoDataSource.getStoreId(), haCommunicationLife );

        Slave slaveImpl = new SlaveImpl( nioneoDataSource.getStoreId(), resolver.resolveDependency( UpdatePuller.class ) );

        SlaveServer server = new SlaveServer( slaveImpl, serverConfig(), logging,
                resolver.resolveDependency( Monitors.class ) );

        masterDelegateHandler.setDelegate( master );

        haCommunicationLife.add( slaveImpl );
        haCommunicationLife.add( server );
        haCommunicationLife.start();

        URI slaveHaURI = createHaURI( me, server );
        clusterMemberAvailability.memberIsAvailable( HighAvailabilityModeSwitcher.SLAVE, slaveHaURI );

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

    private URI createHaURI( URI me, Server<?,?> server )
    {
        String hostString = ServerUtil.getHostString( server.getSocketAddress() );
        int port = server.getSocketAddress().getPort();
        InstanceId serverId = config.get( ClusterSettings.server_id );
        String host = hostString.contains( HighAvailabilityModeSwitcher.INADDR_ANY ) ? me.getHost() : hostString;
        return URI.create( "ha://" + host + ":" + port + "?serverId=" + serverId );
    }

    private void copyStoreFromMaster( URI masterUri ) throws Throwable
    {
        FileSystemAbstraction fs = resolver.resolveDependency( FileSystemAbstraction.class );
        // Must be called under lock on XaDataSourceManager
        LifeSupport life = new LifeSupport();
        try
        {
            // Remove the current store - neostore file is missing, nothing we can really do
//            stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );
            final MasterClient copyMaster = newMasterClient( masterUri, null, life );
            life.start();

            // This will move the copied db to the graphdb location
            console.log( "Copying store from master" );
            new StoreCopyClient( config, kernelExtensions, console, fs ).copyStore(
                    new StoreCopyClient.StoreCopyRequester()
            {
                @Override
                public Response<?> copyStore( StoreWriter writer )
                {
                    return copyMaster.copyStore( new RequestContext( 0,
                            config.get( ClusterSettings.server_id ).toIntegerIndex(), 0, 0, 0, 0 ), writer );
                }

                @Override
                public void done()
                {   // Nothing to clean up here
                }
            } );

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
        MasterClient masterClient = masterClientResolver.instantiate( masterUri.getHost(), masterUri.getPort(),
                resolver.resolveDependency( Monitors.class ), storeId, life );
        if ( !(masterClient instanceof MasterClient210 ))
        {
            idGeneratorFactory.doTheThing();
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

        handleBranchedStore( branchPolicy );
    }

    private void handleBranchedStore( BranchedDataPolicy branchPolicy  ) throws IOException
    {
        branchPolicy.handle( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) );
    }

    private void checkDataConsistencyWithMaster( URI availableMasterId, Master master,
                                                 NeoStoreXaDataSource nioneoDataSource,
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
