/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.Test;

import java.io.IOException;
import javax.ws.rs.core.MediaType;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityServerBuilder.server;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;

public class ServerConfigIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Test
    public void shouldPickUpAddressFromConfig() throws Exception
    {
        ListenSocketAddress nonDefaultAddress = new ListenSocketAddress( "0.0.0.0", 0 );
        server = server().onAddress( nonDefaultAddress )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        HostnamePort localHttpAddress = getLocalHttpAddress();
        assertNotEquals( HttpConnector.Encryption.NONE.defaultPort, localHttpAddress.getPort() );
        assertEquals( nonDefaultAddress.getHostname(), localHttpAddress.getHost() );

        JaxRsResponse response = new RestRequest( server.baseUri() ).get();

        assertThat( response.getStatus(), is( 200 ) );
        response.close();
    }

    @Test
    public void shouldPickupRelativeUrisForManagementApiAndRestApi() throws IOException
    {
        String dataUri = "a/different/data/uri/";
        String managementUri = "a/different/management/uri/";

        server = serverOnRandomPorts().withRelativeRestApiUriPath( "/" + dataUri )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withRelativeManagementApiUriPath( "/" + managementUri )
                .build();
        server.start();

        JaxRsResponse response = new RestRequest().get( server.baseUri().toString() + dataUri,
                MediaType.TEXT_HTML_TYPE );
        assertEquals( 200, response.getStatus() );

        response = new RestRequest().get( server.baseUri().toString() + managementUri );
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGenerateWADLWhenExplicitlyEnabledInConfig() throws IOException
    {
        server = serverOnRandomPorts().withProperty( ServerSettings.wadl_enabled.name(), "true" )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        JaxRsResponse response = new RestRequest().get( server.baseUri().toString() + "application.wadl",
                MediaType.WILDCARD_TYPE );

        assertEquals( 200, response.getStatus() );
        assertEquals( "application/vnd.sun.wadl+xml", response.getHeaders().get( "Content-Type" ).iterator().next() );
        assertThat( response.getEntity(), containsString( "<application xmlns=\"http://wadl.dev.java" +
                ".net/2009/02\">" ) );
    }

    @Test
    public void shouldNotGenerateWADLWhenNotExplicitlyEnabledInConfig() throws IOException
    {
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        JaxRsResponse response = new RestRequest().get( server.baseUri().toString() + "application.wadl",
                MediaType.WILDCARD_TYPE );

        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldNotGenerateWADLWhenExplicitlyDisabledInConfig() throws IOException
    {
        server = serverOnRandomPorts().withProperty( ServerSettings.wadl_enabled.name(), "false" )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        JaxRsResponse response = new RestRequest().get( server.baseUri().toString() + "application.wadl",
                MediaType.WILDCARD_TYPE );

        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldEnableConsoleServiceByDefault() throws IOException
    {
        // Given
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        // When & then
        assertEquals( 200, new RestRequest().get( server.baseUri().toString() + "db/manage/server/console" ).getStatus() );
    }

    @Test
    public void shouldDisableConsoleServiceWhenAskedTo() throws IOException
    {
        // Given
        server = serverOnRandomPorts().withProperty( ServerSettings.console_module_enabled.name(), "false" )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        // When & then
        assertEquals( 404, new RestRequest().get( server.baseUri().toString() + "db/manage/server/console" ).getStatus() );
    }

    private HostnamePort getLocalHttpAddress()
    {
        ConnectorPortRegister connectorPortRegister = server.getDatabase().getGraph().getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class );
        return connectorPortRegister.getLocalAddress( "http" );
    }
}
