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
import org.neo4j.com.ServerUtil;
import org.neo4j.function.Factory;
import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
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
    private Factory<Master> masterFactory;
    private Function<Master, MasterServer> masterServerFactory;
    private Log userLog;
    private HaIdGeneratorFactory idGeneratorFactory;
    private Config config;
    private Supplier<SlaveFactory> slaveFactorySupplier;
    private DelegateInvocationHandler<Master> masterDelegateHandler;
    private ClusterMemberAvailability clusterMemberAvailability;
    private Supplier<NeoStoreDataSource> dataSourceSupplier;

    public SwitchToMaster( LogService logService,
            HaIdGeneratorFactory idGeneratorFactory, Config config, Supplier<SlaveFactory> slaveFactorySupplier,
            Factory<Master> masterFactory,
            Function<Master, MasterServer> masterServerFactory,
            DelegateInvocationHandler<Master> masterDelegateHandler, ClusterMemberAvailability clusterMemberAvailability,
            Supplier<NeoStoreDataSource> dataSourceSupplier)
    {
        this.logService = logService;
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
        userLog.info( "I am %s, moving to master", myId() );

        /*
         * Synchronizing on the xaDataSourceManager makes sense if you also look at HaKernelPanicHandler. In
         * particular, it is possible to get a masterIsElected while recovering the database. That is generally
         * going to break things. Synchronizing on the xaDSM as HaKPH does solves this.
         */
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
//        synchronized ( xaDataSourceManager )
        {

            idGeneratorFactory.switchToMaster();
            NeoStoreDataSource neoStoreXaDataSource = dataSourceSupplier.get();
            neoStoreXaDataSource.afterModeSwitch();

            Master master = masterFactory.newInstance();

            MasterServer masterServer = masterServerFactory.apply( master );

            haCommunicationLife.add( master );
            haCommunicationLife.add( masterServer );
            masterDelegateHandler.setDelegate( master );

            haCommunicationLife.start();

            URI masterHaURI = getMasterUri( me, masterServer );
            clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI, neoStoreXaDataSource.getStoreId() );
            userLog.info( "I am %s, successfully moved to master", myId() );

            slaveFactorySupplier.get().setStoreId( neoStoreXaDataSource.getStoreId() );

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

    private InstanceId myId()
    {
        return config.get( ClusterSettings.server_id );
    }

    @Override
    public void close() throws Exception
    {
        logService = null;
        userLog = null;
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
