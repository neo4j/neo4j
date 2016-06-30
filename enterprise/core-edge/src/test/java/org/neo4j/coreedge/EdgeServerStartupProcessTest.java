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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.EdgeTopologyService;
import org.neo4j.coreedge.discovery.TopologyService;
import org.neo4j.coreedge.raft.replication.tx.ConstantTimeRetryStrategy;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.AlwaysChooseFirstServer;
import org.neo4j.coreedge.server.edge.EdgeServerStartupProcess;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;

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

        CoreMember coreMember = new CoreMember( UUID.randomUUID() );
        TopologyService hazelcastTopology = mock( TopologyService.class );

        ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        when( clusterTopology.coreMembers() ).thenReturn( asSet( coreMember ) );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        Lifecycle txPulling = mock( Lifecycle.class );

        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), config );

        // when
        edgeServerStartupProcess.start();

        // then
        verify( localDatabase ).copyStoreFrom( coreMember, storeFetcher );
        verify( dataSourceManager ).start();
        verify( txPulling ).start();
    }

    @Test
    public void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        LocalDatabase localDatabase = mock( LocalDatabase.class );

        CoreMember coreMember = new CoreMember( UUID.randomUUID() );
        TopologyService hazelcastTopology = mock( TopologyService.class );
        ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( clusterTopology.coreMembers() ).thenReturn( asSet( coreMember ) );

        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        Lifecycle txPulling = mock( Lifecycle.class );
        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), null );

        // when
        edgeServerStartupProcess.stop();

        // then
        verify( txPulling ).stop();
        verify( dataSourceManager ).stop();
    }
}
