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
import java.util.Collections;
import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.storecopy.RemoteStoreCopier;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaRecovery;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.StoreOutOfDateException;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveImpl;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.getServerId;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.isStorePresent;
import static org.neo4j.kernel.impl.util.Providers.singletonProvider;

public class SwitchToSlave
{
    // TODO solve this with lifecycle instance grouping or something
    private final List<Lifecycle> servicesToRestartForStoreCopy = new ArrayList<>(  );

    private final Logging logging;
    private final StringLogger msgLog;
    private final ConsoleLogger console;
    private final Config config;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final HaXaDataSourceManager xaDataSourceManager;
    private final StoreLockerLifecycleAdapter storeLockerLifecycleAdapter;
    private final IndexStore indexStore;
    private final AbstractTransactionManager txManager;
    private final NodeManager nodeManager;
    private final Factory<NeoStoreXaDataSource> dataSourceFactory;
    private final HaRecovery recovery;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final RequestContextFactory requestContextFactory;
    private final UpdateableSchemaState updateableSchemaState;
    private final Monitors monitors;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private HighlyAvailableGraphDatabase highlyAvailableGraphDatabase;

    private MasterClientResolver masterClientResolver;

    public SwitchToSlave( ConsoleLogger console, Config config, HaIdGeneratorFactory
            idGeneratorFactory, Logging logging, DelegateInvocationHandler<Master> masterDelegateHandler,
                          ClusterMemberAvailability clusterMemberAvailability, RequestContextFactory
            requestContextFactory, UpdateableSchemaState updateableSchemaState, Monitors monitors,
                          Iterable<KernelExtensionFactory<?>> kernelExtensions,
                          HighlyAvailableGraphDatabase highlyAvailableGraphDatabase,
                          FileSystemAbstraction fileSystemAbstraction,
                          HaXaDataSourceManager xaDataSourceManager,
                          HaRecovery recovery,
                          StoreLockerLifecycleAdapter storeLockerLifecycleAdapter,
                          IndexStore indexStore, AbstractTransactionManager txManager, NodeManager nodeManager,
                          Factory<NeoStoreXaDataSource> dataSourceFactory )
    {
        this.console = console;
        this.config = config;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.xaDataSourceManager = xaDataSourceManager;
        this.recovery = recovery;
        this.idGeneratorFactory = idGeneratorFactory;
        this.logging = logging;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.requestContextFactory = requestContextFactory;
        this.updateableSchemaState = updateableSchemaState;
        this.monitors = monitors;
        this.kernelExtensions = kernelExtensions;
        this.highlyAvailableGraphDatabase = highlyAvailableGraphDatabase;
        this.storeLockerLifecycleAdapter = storeLockerLifecycleAdapter;
        this.indexStore = indexStore;
        this.txManager = txManager;
        this.nodeManager = nodeManager;
        this.dataSourceFactory = dataSourceFactory;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.masterDelegateHandler = masterDelegateHandler;

        servicesToRestartForStoreCopy.add( storeLockerLifecycleAdapter );
        servicesToRestartForStoreCopy.add( xaDataSourceManager );
        servicesToRestartForStoreCopy.add( txManager );
        servicesToRestartForStoreCopy.add( nodeManager );
        servicesToRestartForStoreCopy.add( indexStore );
    }

    public URI switchToSlave( LifeSupport haCommunicationLife, URI me, URI masterUri ) throws Throwable
    {
        console.log( "ServerId " + config.get( ClusterSettings.server_id ) + ", moving to slave for master " +
                masterUri );

        assert masterUri != null; // since we are here it must already have been set from outside

        this.masterClientResolver = new MasterClientResolver( logging,
                config.get( HaSettings.read_timeout ).intValue(),
                config.get( HaSettings.lock_read_timeout ).intValue(),
                config.get( HaSettings.max_concurrent_channels_per_slave ).intValue(),
                config.get( HaSettings.com_chunk_size ).intValue()  );


        idGeneratorFactory.switchToSlave();
        synchronized ( xaDataSourceManager )
        {
            if ( !isStorePresent( fileSystemAbstraction, config ) )
            {
                copyStoreFromMaster( masterUri );
            }

                            /*
                             * We get here either with a fresh store from the master copy above so we need to
                             * start the ds
                             * or we already had a store, so we have already started the ds. Either way,
                             * make sure it's there.
                             */
            NeoStoreXaDataSource nioneoDataSource = ensureDataSourceStarted(  );
            checkDataConsistency( nioneoDataSource, masterUri );

            URI slaveUri = startHaCommunication( haCommunicationLife, nioneoDataSource, me, masterUri );

            // Recover database
            recovery.recover();
            highlyAvailableGraphDatabase.doAfterRecoveryAndStartup( false );

            console.log( "ServerId " + config.get( ClusterSettings.server_id ) +
                    ", successfully moved to slave for master " + masterUri );

            return slaveUri;
        }
    }

    private void checkDataConsistency( NeoStoreXaDataSource nioneoDataSource, URI masterUri ) throws Throwable
    {
        // Must be called under lock on XaDataSourceManager
        LifeSupport checkConsistencyLife = new LifeSupport();
        try
        {
            MasterClient checkConsistencyMaster = newMasterClient( masterUri, nioneoDataSource.getStoreId(),
                    checkConsistencyLife );
            checkConsistencyLife.start();
            console.log( "Checking store consistency with master" );
            checkDataConsistencyWithMaster( masterUri, checkConsistencyMaster, nioneoDataSource );
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
            console.log( "The store does not represent the same database as master. Will remove and fetch a new one from master" );
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
        finally
        {
            checkConsistencyLife.shutdown();
        }
    }

    private URI startHaCommunication( LifeSupport haCommunicationLife,
            NeoStoreXaDataSource nioneoDataSource, URI me, URI masterUri )
    {
        MasterClient master = newMasterClient( masterUri, nioneoDataSource.getStoreId(), haCommunicationLife );

        Slave slaveImpl = new SlaveImpl( nioneoDataSource.getStoreId(), master,
                new RequestContextFactory( getServerId( masterUri ).toIntegerIndex(), xaDataSourceManager,
                        singletonProvider( txManager ) ), xaDataSourceManager );

        SlaveServer server = new SlaveServer( slaveImpl, serverConfig(), logging, monitors );

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
        // Must be called under lock on XaDataSourceManager
        LifeSupport life = new LifeSupport();
        try
        {
            // Remove the current store - neostore file is missing, nothing we can really do
            stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none );
            final MasterClient copyMaster = newMasterClient( masterUri, null, life );
            life.start();

            // This will move the copied db to the graphdb location
            console.log( "Copying store from master" );
            new RemoteStoreCopier( config, kernelExtensions, console,
                    fileSystemAbstraction ).copyStore( new RemoteStoreCopier.StoreCopyRequester()

            {
                @Override
                public Response<?> copyStore( StoreWriter writer )
                {
                    return copyMaster.copyStore( new RequestContext( 0,
                            config.get( ClusterSettings.server_id ).toIntegerIndex(), 0, new RequestContext.Tx[0], 0, 0 ), writer );
                }

                @Override
                public void done()
                {
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
        return masterClientResolver.instantiate( masterUri.getHost(), masterUri.getPort(), monitors, storeId, life );
    }

    private void startServicesAgain() throws Throwable
    {
        for ( Lifecycle lifecycle : servicesToRestartForStoreCopy )
        {
            lifecycle.start();
        }
    }

    private void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy ) throws Throwable
    {
        List<Lifecycle> reversedList = new ArrayList<>( servicesToRestartForStoreCopy );
        Collections.reverse( reversedList );

        for ( Lifecycle lifecycle : reversedList )
        {
            lifecycle.stop();
        }

        branchPolicy.handle( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) );
    }

    private void checkDataConsistencyWithMaster( URI availableMasterId, Master master, NeoStoreXaDataSource nioneoDataSource )
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

    private NeoStoreXaDataSource ensureDataSourceStarted(  )
            throws IOException
    {
        // Must be called under lock on XaDataSourceManager
        NeoStoreXaDataSource nioneoDataSource = (NeoStoreXaDataSource) xaDataSourceManager.getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        if ( nioneoDataSource == null )
        {
            nioneoDataSource = dataSourceFactory.newInstance();
            xaDataSourceManager.registerDataSource( nioneoDataSource );
                /*
                 * CAUTION: The next line may cause severe eye irritation, mental instability and potential
                 * emotional breakdown. On the plus side, it is correct.
                 * See, it is quite possible to get here without the NodeManager having stopped, because we don't
                 * properly manage lifecycle in this class (this is the cause of this ugliness). So, after we
                 * register the datasource with the DsMgr we need to make sure that NodeManager re-reads the reltype
                 * and propindex information. Normally, we would have shutdown everything before getting here.
                 */
            nodeManager.start();
        }
        return nioneoDataSource;
    }
}
