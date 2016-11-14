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
package org.neo4j.causalclustering.scenarios;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.state.CoreState;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.configuration.ClientConnectorSettings;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.discovery_listen_address;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.transaction_listen_address;
import static org.neo4j.kernel.configuration.Config.defaults;

public class ConnectionInfoIT
{
    private Socket testSocket;

    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 );

    @After
    public void teardown() throws IOException
    {
        if ( testSocket != null )
        {
            unbind( testSocket );
        }
    }

    @Test
    public void catchupServerMessage() throws Throwable
    {
        // given
        testSocket = bindPort( "localhost", 4242 );

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider();
        AssertableLogProvider userLogProvider = new AssertableLogProvider();
        CoreState coreState = mock( CoreState.class );
        Config config = Config.defaults()
                .with( singletonMap( transaction_listen_address.name(), ":" + testSocket.getLocalPort() ) );

        CatchupServer catchupServer =
                new CatchupServer( logProvider, userLogProvider, mockSupplier(), mockSupplier(), mockSupplier(),
                        mockSupplier(), mock( BooleanSupplier.class ), coreState, config, new Monitors(),
                        mockSupplier(), mock( FileSystemAbstraction.class ) );

        //then
        try
        {
            catchupServer.start();
        }
        catch ( Throwable throwable )
        {
            //expected.
        }
        logProvider.assertContainsMessageContaining( "Address is already bound for setting" );
        userLogProvider.assertContainsMessageContaining( "Address is already bound for setting" );
    }

    @SuppressWarnings( "unchecked" )
    private <T> Supplier<T> mockSupplier()
    {
        return mock( Supplier.class );
    }

    @Test
    public void hzTest() throws Throwable
    {
        // given
        testSocket = bindPort( "0.0.0.0", 4243 );

        //when
        AssertableLogProvider logProvider = new AssertableLogProvider();
        AssertableLogProvider userLogProvider = new AssertableLogProvider();

        HazelcastDiscoveryServiceFactory hzFactory = new HazelcastDiscoveryServiceFactory();
        Config config =
                defaults().with( singletonMap( discovery_listen_address.name(), ":" + testSocket.getLocalPort() ) );
        config.augment( singletonMap( CausalClusteringSettings.initial_discovery_members.name(),
                "localhost:" + testSocket.getLocalPort() ) );
        config.augment( singletonMap( GraphDatabaseSettings.boltConnector( "bolt" ).enabled.name(), "true" ) );
        config.augment( singletonMap( ClientConnectorSettings.httpConnector( "http" ).enabled.name(), "true" ) );

        Neo4jJobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();

        CoreTopologyService coreTopologyService = hzFactory
                .coreTopologyService( config, new MemberId( UUID.randomUUID() ), jobScheduler, logProvider,
                        userLogProvider );

        try
        {
            coreTopologyService.init();
            coreTopologyService.start();
        }

        //then
        catch ( Throwable throwable )
        {
            //expected
        }

        logProvider.assertContainsMessageContaining( "Hazelcast was unable to start with setting" );
        userLogProvider.assertContainsMessageContaining( "Hazelcast was unable to start with setting" );
    }

    private Socket bindPort( String address, int port ) throws IOException
    {
        Socket socket = new Socket();
        socket.bind( new InetSocketAddress( address, port ) );
        return socket;
    }

    private void unbind( Socket socket ) throws IOException
    {
        socket.close();
    }
}
