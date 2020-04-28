/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

public class BoltIT extends ExclusiveWebContainerTestBase
{
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    private TestWebContainer testWebContainer;

    @After
    public void stopTheServer()
    {
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
        }
    }

    @Test
    public void shouldLaunchBolt() throws Throwable
    {
        // When I run Neo4j with Bolt enabled
        startServerWithBoltEnabled();

        ConnectorPortRegister connectorPortRegister = getDependency( ConnectorPortRegister.class );

        // Then
        assertEventuallyServerResponds( "localhost", connectorPortRegister.getLocalAddress( "bolt" ).getPort() );
    }

    @Test
    public void shouldBeAbleToSpecifyHostAndPort() throws Throwable
    {
        // When
        startServerWithBoltEnabled();

        ConnectorPortRegister connectorPortRegister = getDependency( ConnectorPortRegister.class );
        // Then
        assertEventuallyServerResponds( "localhost", connectorPortRegister.getLocalAddress( "bolt" ).getPort()  );
    }

    @Test
    public void boltAddressShouldComeFromConnectorAdvertisedAddress() throws Throwable
    {
        // Given
        String host = "neo4j.com";

        startServerWithBoltEnabled( host, 9999, "localhost", 0 );

        HttpRequest request = HttpRequest.newBuilder( testWebContainer.getBaseUri() ).GET().build();

        // When
        HttpResponse<String> response = newHttpClient().send( request, ofString() );

        // Then
        Map<String,Object> map = JsonHelper.jsonToMap( response.body() );
        assertThat( String.valueOf( map.get( "bolt_direct" ) ) ).contains( "bolt://" + host + ':' + 9999 );
    }

    private void startServerWithBoltEnabled() throws IOException
    {
        startServerWithBoltEnabled( "localhost", 7687, "localhost", 7687 );
    }

    private void startServerWithBoltEnabled( String advertisedHost, int advertisedPort, String listenHost, int listenPort ) throws IOException
    {
        testWebContainer = serverOnRandomPorts()
                .withProperty( BoltConnector.enabled.name(), TRUE )
                .withProperty( BoltConnector.encryption_level.name(), "DISABLED" )
                .withProperty( BoltConnector.advertised_address.name(), advertisedHost + ':' + advertisedPort )
                .withProperty( BoltConnector.listen_address.name(), listenHost + ':' + listenPort )
                .usingDataDir( tmpDir.getRoot().getAbsolutePath() ).build();
    }

    private void assertEventuallyServerResponds( String host, int port ) throws Exception
    {
        SocketConnection conn = new SocketConnection();
        conn.connect( new HostnamePort( host, port ) );
        conn.send(
                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0} );
        assertThat( conn.recv( 4 ) ).isEqualTo( new byte[]{0, 0, 0, 4} );
    }

    private <T> T getDependency( Class<T> clazz )
    {
        return testWebContainer.resolveDependency( clazz );
    }
}
