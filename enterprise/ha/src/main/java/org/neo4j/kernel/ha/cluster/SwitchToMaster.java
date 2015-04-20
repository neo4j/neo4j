/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.net.URI;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchDetectingTxVerifier;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.TransactionChecksumLookup;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;

public class SwitchToMaster implements AutoCloseable
{
    private Logging logging;
    private ConsoleLogger console;
    private GraphDatabaseAPI graphDb;
    private HaIdGeneratorFactory idGeneratorFactory;
    private Config config;
    private Provider<SlaveFactory> slaveFactorySupplier;
    private MasterImpl.Monitor masterImplMonitor;
    private DelegateInvocationHandler<Master> masterDelegateHandler;
    private ClusterMemberAvailability clusterMemberAvailability;
    private DataSourceManager dataSourceManager;
    private ByteCounterMonitor masterByteCounterMonitor;
    private RequestMonitor masterRequestMonitor;

    public SwitchToMaster( Logging logging, ConsoleLogger console, GraphDatabaseAPI graphDb,
            HaIdGeneratorFactory idGeneratorFactory, Config config, Provider<SlaveFactory> slaveFactorySupplier,
            DelegateInvocationHandler<Master> masterDelegateHandler, ClusterMemberAvailability clusterMemberAvailability,
            DataSourceManager dataSourceManager, ByteCounterMonitor masterByteCounterMonitor, RequestMonitor masterRequestMonitor, MasterImpl.Monitor masterImplMonitor)
    {
        this.logging = logging;
        this.console = console;
        this.graphDb = graphDb;
        this.idGeneratorFactory = idGeneratorFactory;
        this.config = config;
        this.slaveFactorySupplier = slaveFactorySupplier;
        this.masterImplMonitor = masterImplMonitor;
        this.masterDelegateHandler = masterDelegateHandler;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.dataSourceManager = dataSourceManager;
        this.masterByteCounterMonitor = masterByteCounterMonitor;
        this.masterRequestMonitor = masterRequestMonitor;
    }

    /**
     * Performs a switch to the master state. Starts communication endpoints, switches components to the master state
     * and broadcasts the appropriate Master Is Available event.
     * @param haCommunicationLife The LifeSupport instance to register communication endpoints.
     * @param me The URI that the communication endpoints should bind to
     * @return The URI at which the master communication was bound.
     */
    public URI switchToMaster( LifeSupport haCommunicationLife, URI me )
    {
        console.log( "I am " + myId() + ", moving to master" );

        /*
         * Synchronizing on the xaDataSourceManager makes sense if you also look at HaKernelPanicHandler. In
         * particular, it is possible to get a masterIsElected while recovering the database. That is generally
         * going to break things. Synchronizing on the xaDSM as HaKPH does solves this.
         */
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
//        synchronized ( xaDataSourceManager )
        {

            idGeneratorFactory.switchToMaster();
            NeoStoreDataSource neoStoreXaDataSource = dataSourceManager.getDataSource();
            neoStoreXaDataSource.afterModeSwitch();

            MasterImpl.SPI spi = new DefaultMasterImplSPI( graphDb );

            MasterImpl masterImpl = new MasterImpl( spi, masterImplMonitor,
                    logging, config );

            DependencyResolver resolver = neoStoreXaDataSource.getDependencyResolver();
            TransactionChecksumLookup txChecksumLookup = new TransactionChecksumLookup(
                    resolver.resolveDependency( TransactionIdStore.class ),
                    resolver.resolveDependency( LogicalTransactionStore.class ) );
            MasterServer masterServer = new MasterServer( masterImpl, logging, serverConfig(),
                    new BranchDetectingTxVerifier( logging.getMessagesLog( BranchDetectingTxVerifier.class ),
                            txChecksumLookup ), masterByteCounterMonitor, masterRequestMonitor );
            haCommunicationLife.add( masterImpl );
            haCommunicationLife.add( masterServer );
            masterDelegateHandler.setDelegate( masterImpl );

            haCommunicationLife.start();

            URI masterHaURI = getMasterUri( me, masterServer );
            clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI, neoStoreXaDataSource.getStoreId() );
            console.log( "I am " + myId() + ", successfully moved to master" );

            slaveFactorySupplier.instance().setStoreId( neoStoreXaDataSource.getStoreId() );

            return masterHaURI;
        }
    }

    private URI getMasterUri( URI me, MasterServer masterServer )
    {
        String hostname = ServerUtil.getHostString( masterServer.getSocketAddress() ).contains( "0.0.0.0" ) ?
                            me.getHost() :
                            ServerUtil.getHostString( masterServer.getSocketAddress() );

        int port = masterServer.getSocketAddress().getPort();

        return URI.create( "ha://" + hostname + ":" + port + "?serverId=" + myId() );
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

    private InstanceId myId()
    {
        return config.get( ClusterSettings.server_id );
    }

    @Override
    public void close() throws Exception
    {
        logging = null;
        console = null;
        graphDb = null;
        idGeneratorFactory = null;
        config = null;
        slaveFactorySupplier = null;
        masterImplMonitor = null;
        masterDelegateHandler = null;
        clusterMemberAvailability = null;
        dataSourceManager = null;
        masterByteCounterMonitor = null;
        masterRequestMonitor = null;
    }
}
