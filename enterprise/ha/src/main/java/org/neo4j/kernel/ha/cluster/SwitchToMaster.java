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

import java.net.URI;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.ServerUtil;
import org.neo4j.function.BiFunction;
import org.neo4j.function.Factory;
import org.neo4j.function.Supplier;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.com.master.ConversationManager;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;

public class SwitchToMaster implements AutoCloseable
{
    private LogService logService;
    Factory<ConversationManager> conversationManagerFactory;
    BiFunction<ConversationManager, LifeSupport, Master> masterFactory;
    BiFunction<Master, ConversationManager, MasterServer> masterServerFactory;
    private Log userLog;
    private HaIdGeneratorFactory idGeneratorFactory;
    private Config config;
    private Supplier<SlaveFactory> slaveFactorySupplier;
    private DelegateInvocationHandler<Master> masterDelegateHandler;
    private ClusterMemberAvailability clusterMemberAvailability;
    private Supplier<NeoStoreDataSource> dataSourceSupplier;

    public SwitchToMaster( LogService logService,
            HaIdGeneratorFactory idGeneratorFactory, Config config, Supplier<SlaveFactory> slaveFactorySupplier,
            Factory<ConversationManager> conversationManagerFactory,
            BiFunction<ConversationManager, LifeSupport, Master> masterFactory,
            BiFunction<Master, ConversationManager, MasterServer> masterServerFactory,
            DelegateInvocationHandler<Master> masterDelegateHandler, ClusterMemberAvailability clusterMemberAvailability,
            Supplier<NeoStoreDataSource> dataSourceSupplier )
    {
        this.logService = logService;
        this.conversationManagerFactory = conversationManagerFactory;
        this.masterFactory = masterFactory;
        this.masterServerFactory = masterServerFactory;
        this.userLog = logService.getUserLog( getClass() );
        this.idGeneratorFactory = idGeneratorFactory;
        this.config = config;
        this.slaveFactorySupplier = slaveFactorySupplier;
        this.masterDelegateHandler = masterDelegateHandler;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.dataSourceSupplier = dataSourceSupplier;
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
        userLog.info( "I am %s, moving to master", myId( config ) );

        // Do not wait for currently active transactions to complete before continuing switching.
        // - A master in a cluster is very important, without it the cluster cannot process any write requests
        // - Awaiting open transactions to complete assumes that this instance just now was a slave that is
        //   switching to master, which means the previous master where these active transactions were hosted
        //   is no longer available so these open transactions cannot continue and complete anyway,
        //   so what's the point waiting for them?
        // - Read transactions may still be able to complete, but the correct response to failures in those
        //   is to have them throw transient error exceptions hinting that they should be retried,
        //   at which point they may get redirected to another instance, or to this instance if it has completed
        //   the switch until then.

        idGeneratorFactory.switchToMaster();
        NeoStoreDataSource neoStoreXaDataSource = dataSourceSupplier.get();
        neoStoreXaDataSource.afterModeSwitch();

        ConversationManager conversationManager = conversationManagerFactory.newInstance();
        Master master = masterFactory.apply( conversationManager, haCommunicationLife );

        MasterServer masterServer = masterServerFactory.apply( master, conversationManager );

        haCommunicationLife.add( masterServer );
        masterDelegateHandler.setDelegate( master );

        haCommunicationLife.start();

        URI masterHaURI = getMasterUri( me, masterServer, config );
        clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI, neoStoreXaDataSource.getStoreId() );
        userLog.info( "I am %s, successfully moved to master", myId( config ) );

        slaveFactorySupplier.get().setStoreId( neoStoreXaDataSource.getStoreId() );

        return masterHaURI;
    }

    static URI getMasterUri( URI me, MasterServer masterServer, Config config )
    {

        String hostname = config.get( HaSettings.ha_server ).getHost();
        if ( hostname == null || hostname.contains( "0.0.0.0" ) )
        {
            String masterAddress = ServerUtil.getHostString( masterServer.getSocketAddress() );
            hostname = masterAddress.contains( "0.0.0.0" ) ? me.getHost() : masterAddress;
        }

        int port = masterServer.getSocketAddress().getPort();

        return URI.create( "ha://" + hostname + ":" + port + "?serverId=" + myId( config ) );
    }

    private static InstanceId myId( Config config )
    {
        return config.get( ClusterSettings.server_id );
    }

    @Override
    public void close() throws Exception
    {
        logService = null;
        userLog = null;
        conversationManagerFactory = null;
        masterFactory = null;
        masterServerFactory = null;
        idGeneratorFactory = null;
        config = null;
        slaveFactorySupplier = null;
        masterDelegateHandler = null;
        clusterMemberAvailability = null;
        dataSourceSupplier = null;
    }
}
