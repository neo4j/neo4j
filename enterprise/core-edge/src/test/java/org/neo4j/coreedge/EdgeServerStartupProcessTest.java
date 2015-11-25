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
package org.neo4j.coreedge;

import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.discovery.HazelcastClusterTopology;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.discovery.CoreDiscoveryService;
import org.neo4j.coreedge.catchup.tx.edge.TxPollingClient;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.server.edge.EdgeServerStartupProcess;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class EdgeServerStartupProcessTest
{
    @Test
    public void startShouldReplaceLocalStoreWithStoreFromCoreServerAndStartPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        LocalDatabase localDatabase = mock( LocalDatabase.class );

        AdvertisedSocketAddress coreServerAddress = address( "localhost:1999" );
        CoreDiscoveryService hazelcastTopology = mock( CoreDiscoveryService.class );
        HazelcastClusterTopology clusterTopology = mock( HazelcastClusterTopology.class );
        when( clusterTopology.firstTransactionServer() ).thenReturn( coreServerAddress );

        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        TxPollingClient txPuller = mock( TxPollingClient.class );
        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPuller, hazelcastTopology, dataSourceManager );

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

        AdvertisedSocketAddress coreServerAddress = address( "localhost:1999" );
        CoreDiscoveryService hazelcastTopology = mock( CoreDiscoveryService.class );
        HazelcastClusterTopology clusterTopology = mock( HazelcastClusterTopology.class );
        when( clusterTopology.firstTransactionServer() ).thenReturn( coreServerAddress );

        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        TxPollingClient txPuller = mock( TxPollingClient.class );
        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPuller, hazelcastTopology, dataSourceManager );

        // when
        edgeServerStartupProcess.stop();

        // then
        verify( txPuller ).stop();
        verify( dataSourceManager ).stop();
    }
}
