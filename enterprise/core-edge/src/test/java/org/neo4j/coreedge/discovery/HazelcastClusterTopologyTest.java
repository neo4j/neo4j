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
package org.neo4j.coreedge.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.tx.edge.TxPollingClient;
import org.neo4j.coreedge.raft.replication.tx.ConstantTimeRetryStrategy;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.coreedge.server.edge.EdgeServerStartupProcess;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HazelcastClusterTopologyTest
{
    @Test
    public void edgeServersShouldRegisterThemselvesWithTheTopologyWhenTheyStart() throws Throwable
    {
        // given
        final Map<String, String> params = new HashMap<>();

        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).address.name(), "127.0.0.1:" + 8001 );

        Config config = new Config( params );

        final EdgeTopologyService topology = mock( EdgeTopologyService.class );

        // when

        final CoreServerSelectionStrategy connectionStrategy = mock( CoreServerSelectionStrategy.class );
        when( connectionStrategy.coreServer() ).thenReturn( new AdvertisedSocketAddress( "host:1234" ) );

        final EdgeServerStartupProcess startupProcess = new EdgeServerStartupProcess( null,
                mock( LocalDatabase.class ),
                mock( TxPollingClient.class ),
                mock( DataSourceManager.class ),
                connectionStrategy,
                new ConstantTimeRetryStrategy( 1, TimeUnit.MILLISECONDS ),
                NullLogProvider.getInstance(), topology, config );

        startupProcess.start();

        // then
        verify( topology ).registerEdgeServer( anyObject() );
    }
}
