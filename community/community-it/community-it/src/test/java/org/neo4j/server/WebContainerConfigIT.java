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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.PortUtils;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

public class WebContainerConfigIT extends ExclusiveWebContainerTestBase
{
    private TestWebContainer testWebContainer;

    @After
    public void stopTheServer()
    {
        testWebContainer.shutdown();
    }

    @Test
    public void shouldRequireAuth() throws Exception
    {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withProperty( ServerSettings.http_auth_whitelist.name(), "" )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), TRUE )
                .build();

        var request = HttpRequest.newBuilder( testWebContainer.getBaseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 401 ) );
    }

    @Test
    public void shouldWhitelist() throws Exception
    {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withProperty( ServerSettings.http_auth_whitelist.name(), "/" )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), TRUE )
                .build();

        var request = HttpRequest.newBuilder( testWebContainer.getBaseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 200 ) );
    }

    @Test
    public void shouldBlacklistPaths() throws Exception
    {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withProperty( ServerSettings.http_paths_blacklist.name(), "/*" )
                .build();

        var request = HttpRequest.newBuilder( testWebContainer.getBaseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 403 ) );
    }

    @Test
    public void shouldPickUpAddressFromConfig() throws Exception
    {
        var nonDefaultAddress = new SocketAddress( "0.0.0.0", 0 );
        testWebContainer = CommunityWebContainerBuilder.builder().onAddress( nonDefaultAddress )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        GraphDatabaseAPI database = testWebContainer.getDefaultDatabase();
        var localHttpAddress = PortUtils.getConnectorAddress( database, "http" );
        assertNotEquals( HttpConnector.DEFAULT_PORT, localHttpAddress.getPort() );
        assertEquals( nonDefaultAddress.getHostname(), localHttpAddress.getHost() );

        var request = HttpRequest.newBuilder( testWebContainer.getBaseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 200 ) );
    }

    @Test
    public void shouldPickupRelativeUrisForDatabaseApi() throws Exception
    {
        var dbUri = "a/different/db/uri";

        testWebContainer = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withRelativeDatabaseApiPath( "/" + dbUri )
                .build();

        var uri = testWebContainer.getBaseUri() + dbUri + "/neo4j/tx/commit";
        var txRequest = HttpRequest.newBuilder( URI.create( uri ) )
                .header( ACCEPT, APPLICATION_JSON )
                .header( CONTENT_TYPE, APPLICATION_JSON )
                .POST( HttpRequest.BodyPublishers.ofString( "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" ) )
                .build();
        var txResponse = newHttpClient().send( txRequest, discarding() );
        assertEquals( 200, txResponse.statusCode() );

        var discoveryRequest = HttpRequest.newBuilder( testWebContainer.getBaseUri() ).GET().build();
        var discoveryResponse = newHttpClient().send( discoveryRequest, ofString() );
        assertEquals( 200, txResponse.statusCode() );
        assertThat( discoveryResponse.body(), containsString( dbUri ) );
    }

    @Test
    public void shouldPickupRelativeUrisForRestApi() throws Exception
    {
        var dbUri = "a/different/rest/api/path";

        testWebContainer = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withRelativeRestApiPath( "/" + dbUri )
                .build();

        var uri = testWebContainer.getBaseUri() + dbUri + "/transaction/commit";
        var txRequest = HttpRequest.newBuilder( URI.create( uri ) )
                .header( ACCEPT, APPLICATION_JSON )
                .header( CONTENT_TYPE, APPLICATION_JSON )
                .POST( HttpRequest.BodyPublishers.ofString( "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" ) )
                .build();
        var txResponse = newBuilder().followRedirects( HttpClient.Redirect.NORMAL ).build().send( txRequest, discarding() );
        System.out.println( txResponse );
        assertEquals( 200, txResponse.statusCode() );

        // however this legacy url should not be inside the discovery service.
        var discoveryRequest = HttpRequest.newBuilder( testWebContainer.getBaseUri() ).GET().build();
        var discoveryResponse = newHttpClient().send( discoveryRequest, ofString() );
        assertEquals( 200, txResponse.statusCode() );
        assertThat( discoveryResponse.body(), not( containsString( dbUri ) ) );
    }

    @Test
    public void shouldGenerateWADLWhenExplicitlyEnabledInConfig() throws Exception
    {
        testWebContainer = serverOnRandomPorts().withProperty( ServerSettings.wadl_enabled.name(), TRUE )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        var wadlUri = URI.create( testWebContainer.getBaseUri() + "application.wadl" );
        var request = HttpRequest.newBuilder( wadlUri ).GET().header( CONTENT_TYPE, WILDCARD ).build();
        var response = newHttpClient().send( request, ofString() );

        assertEquals( 200, response.statusCode() );
        assertEquals( "application/vnd.sun.wadl+xml", response.headers().allValues( "Content-Type" ).iterator().next() );
        assertThat( response.body(), containsString( "<application xmlns=\"http://wadl.dev.java.net/2009/02\">" ) );
    }

    @Test
    public void shouldNotGenerateWADLWhenNotExplicitlyEnabledInConfig() throws Exception
    {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        var uri = URI.create( testWebContainer.getBaseUri() + "application.wadl" );
        var request = HttpRequest.newBuilder( uri ).GET().header( CONTENT_TYPE, WILDCARD ).build();
        var response = newHttpClient().send( request, ofString() );

        assertEquals( 404, response.statusCode() );
    }

    @Test
    public void shouldNotGenerateWADLWhenExplicitlyDisabledInConfig() throws Exception
    {
        testWebContainer = serverOnRandomPorts().withProperty( ServerSettings.wadl_enabled.name(), FALSE )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        var warlUri = URI.create( testWebContainer.getBaseUri() + "application.wadl" );
        var request = HttpRequest.newBuilder( warlUri ).GET().header( CONTENT_TYPE, WILDCARD ).build();
        var response = newHttpClient().send( request, ofString() );

        assertEquals( 404, response.statusCode() );
    }
}
