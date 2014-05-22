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

import java.net.URI;
import javax.transaction.TransactionManager;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.Server;
import org.neo4j.com.ServerUtil;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchDetectingTxVerifier;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaRecovery;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

public class SwitchToMaster
{
    private final Logging logging;
    private final StringLogger msgLog;
    private final HighlyAvailableGraphDatabase graphDb;
    private final HaIdGeneratorFactory idGeneratorFactory;
    private final Config config;
    private final TransactionManager txManager;
    private final XaDataSourceManager xaDataSourceManager;
    private final HaRecovery recovery;
    private final DelegateInvocationHandler<Master> masterDelegateHandler;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final Monitors monitors;
    private final Provider<XaDataSource> xaDataSourceProvider;

    public SwitchToMaster( Logging logging, StringLogger msgLog, HighlyAvailableGraphDatabase graphDb, HaIdGeneratorFactory
            idGeneratorFactory, Config config, TransactionManager txManager,
            XaDataSourceManager xaDataSourceManager,
            DelegateInvocationHandler<Master> masterDelegateHandler, ClusterMemberAvailability clusterMemberAvailability,

            HaRecovery recovery, Monitors monitors )
    {
        this.logging = logging;
        this.msgLog = msgLog;
        this.graphDb = graphDb;
        this.idGeneratorFactory = idGeneratorFactory;
        this.config = config;
        this.txManager = txManager;
        this.xaDataSourceManager = xaDataSourceManager;
        this.recovery = recovery;
        this.masterDelegateHandler = masterDelegateHandler;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.monitors = monitors;

        xaDataSourceProvider = new Provider<XaDataSource>()
        {
            @Override
            public XaDataSource instance()
            {
                return SwitchToMaster.this.xaDataSourceManager.getXaDataSource( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
            }
        };
    }

    public URI switchToMaster(LifeSupport haCommunicationLife, URI me)
    {
        msgLog.logMessage( "I am " + config.get( ClusterSettings.server_id ) + ", moving to master" );

        /*
         * Synchronizing on the xaDataSourceManager makes sense if you also look at HaKernelPanicHandler. In
         * particular, it is possible to get a masterIsElected while recovering the database. That is generally
         * going to break things. Synchronizing on the xaDSM as HaKPH does solves this.
         */
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized ( xaDataSourceManager )
        {
            idGeneratorFactory.switchToMaster();

            // Start networking
            MasterImpl.SPI spi = new DefaultMasterImplSPI( graphDb, logging, txManager, monitors );

            MasterImpl masterImpl = new MasterImpl( spi, monitors.newMonitor( MasterImpl.Monitor.class ),
                    logging, config );

            MasterServer masterServer = new MasterServer( masterImpl, logging, serverConfig(),
                    new BranchDetectingTxVerifier( logging.getMessagesLog( BranchDetectingTxVerifier.class ), xaDataSourceProvider ),
                    monitors );
            haCommunicationLife.add( masterImpl );
            haCommunicationLife.add( masterServer );
            masterDelegateHandler.setDelegate( masterImpl );

            haCommunicationLife.start();

            // Recover database
            recovery.recover();
            graphDb.doAfterRecoveryAndStartup( true );

            URI masterHaURI = URI.create( "ha://" + (ServerUtil.getHostString( masterServer.getSocketAddress
                    () ).contains
                    ( "0.0.0.0" ) ? me.getHost() : ServerUtil.getHostString( masterServer
                    .getSocketAddress() )) + ":" +
                    masterServer.getSocketAddress().getPort() + "?serverId=" +
                    config.get( ClusterSettings.server_id ) );
            clusterMemberAvailability.memberIsAvailable( HighAvailabilityModeSwitcher.MASTER, masterHaURI );
            msgLog.logMessage( "I am " + config.get( ClusterSettings.server_id ) +
                    ", successfully moved to master" );

            return masterHaURI;
        }
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
}
