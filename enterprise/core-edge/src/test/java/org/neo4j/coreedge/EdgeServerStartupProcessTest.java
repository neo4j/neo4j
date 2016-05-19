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
package org.neo4j.coreedge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.edge.TxPollingClient;
import org.neo4j.coreedge.discovery.EdgeTopologyService;
import org.neo4j.coreedge.discovery.HazelcastClusterTopology;
import org.neo4j.coreedge.raft.replication.tx.ConstantTimeRetryStrategy;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.AlwaysChooseFirstServer;
import org.neo4j.coreedge.server.edge.EdgeServerStartupProcess;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EdgeServerStartupProcessTest
{
    @Test
    public void startShouldReplaceLocalStoreWithStoreFromCoreServerAndStartPolling() throws Throwable
    {
        // given
        final Map<String, String> params = new HashMap<>();

        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).address.name(), "127.0.0.1:" + 8001 );

        Config config = new Config( params );

        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        LocalDatabase localDatabase = mock( LocalDatabase.class );

        AdvertisedSocketAddress coreServerAddress = new AdvertisedSocketAddress( "localhost:1999" );
        EdgeTopologyService hazelcastTopology = mock( EdgeTopologyService.class );

        HazelcastClusterTopology clusterTopology = mock( HazelcastClusterTopology.class );
        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        when( clusterTopology.coreMembers() ).thenReturn( coreMembers( coreServerAddress ) );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        TxPollingClient txPuller = mock( TxPollingClient.class );
        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPuller, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), config );

        // when
        edgeServerStartupProcess.start();

        // then
        verify( localDatabase ).copyStoreFrom( coreServerAddress, storeFetcher );
        verify( dataSourceManager ).start();
        verify( txPuller ).startPolling();
    }

    @Test
    public void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        LocalDatabase localDatabase = mock( LocalDatabase.class );

        AdvertisedSocketAddress coreServerAddress = new AdvertisedSocketAddress( "localhost:1999" );
        EdgeTopologyService hazelcastTopology = mock( EdgeTopologyService.class );
        HazelcastClusterTopology clusterTopology = mock( HazelcastClusterTopology.class );
        when( clusterTopology.coreMembers() ).thenReturn( coreMembers( coreServerAddress ) );

        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        TxPollingClient txPuller = mock( TxPollingClient.class );
        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPuller, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), null );

        // when
        edgeServerStartupProcess.stop();

        // then
        verify( txPuller ).stop();
        verify( dataSourceManager ).stop();
    }

    private Set<CoreMember> coreMembers( AdvertisedSocketAddress coreServerAddress )
    {
        final Set<CoreMember> coreMembers = new HashSet<>();
        coreMembers.add( new CoreMember( coreServerAddress, null, null ) );
        return coreMembers;
    }
}
