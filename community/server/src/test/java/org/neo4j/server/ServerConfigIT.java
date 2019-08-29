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
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.PortUtils;
import org.neo4j.test.server.ExclusiveServerTestBase;

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
    public void shouldRequireAuth() throws Exception
    {
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withProperty( ServerSettings.http_auth_whitelist.name(), "" )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), TRUE )
                .build();
        server.start();

        var request = HttpRequest.newBuilder( server.baseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 401 ) );
    }

    @Test
    public void shouldWhitelist() throws Exception
    {
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withProperty( ServerSettings.http_auth_whitelist.name(), "/" )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), TRUE )
                .build();
        server.start();

        var request = HttpRequest.newBuilder( server.baseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 200 ) );
    }

    @Test
    public void shouldBlacklistPaths() throws Exception
    {
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withProperty( ServerSettings.http_paths_blacklist.name(), "/*" )
                .build();
        server.start();

        var request = HttpRequest.newBuilder( server.baseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 403 ) );
    }

    @Test
    public void shouldPickUpAddressFromConfig() throws Exception
    {
        var nonDefaultAddress = new SocketAddress( "0.0.0.0", 0 );
        server = server().onAddress( nonDefaultAddress )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        var localHttpAddress = PortUtils.getConnectorAddress( server.getDatabaseService().getDatabase(), "http" );
        assertNotEquals( HttpConnector.DEFAULT_PORT, localHttpAddress.getPort() );
        assertEquals( nonDefaultAddress.getHostname(), localHttpAddress.getHost() );

        var request = HttpRequest.newBuilder( server.baseUri() ).GET().build();
        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 200 ) );
    }

    @Test
    public void shouldPickupRelativeUrisForDatabaseApi() throws Exception
    {
        var dbUri = "a/different/db/uri";

        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withRelativeDatabaseApiPath( "/" + dbUri )
                .build();
        server.start();

        var uri = server.baseUri() + dbUri + "/neo4j/tx/commit";
        var txRequest = HttpRequest.newBuilder( URI.create( uri ) )
                .header( ACCEPT, APPLICATION_JSON )
                .header( CONTENT_TYPE, APPLICATION_JSON )
                .POST( HttpRequest.BodyPublishers.ofString( "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" ) )
                .build();
        var txResponse = newHttpClient().send( txRequest, discarding() );
        assertEquals( 200, txResponse.statusCode() );

        var discoveryRequest = HttpRequest.newBuilder( server.baseUri() ).GET().build();
        var discoveryResponse = newHttpClient().send( discoveryRequest, ofString() );
        assertEquals( 200, txResponse.statusCode() );
        assertThat( discoveryResponse.body(), containsString( dbUri ) );
    }

    @Test
    public void shouldPickupRelativeUrisForRestApi() throws Exception
    {
        var dbUri = "a/different/rest/api/path";

        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withRelativeRestApiPath( "/" + dbUri )
                .build();
        server.start();

        var uri = server.baseUri() + dbUri + "/transaction/commit";
        var txRequest = HttpRequest.newBuilder( URI.create( uri ) )
                .header( ACCEPT, APPLICATION_JSON )
                .header( CONTENT_TYPE, APPLICATION_JSON )
                .POST( HttpRequest.BodyPublishers.ofString( "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" ) )
                .build();
        var txResponse = newBuilder().followRedirects( HttpClient.Redirect.NORMAL ).build().send( txRequest, discarding() );
        System.out.println( txResponse );
        assertEquals( 200, txResponse.statusCode() );

        // however this legacy url should not be inside the discovery service.
        var discoveryRequest = HttpRequest.newBuilder( server.baseUri() ).GET().build();
        var discoveryResponse = newHttpClient().send( discoveryRequest, ofString() );
        assertEquals( 200, txResponse.statusCode() );
        assertThat( discoveryResponse.body(), not( containsString( dbUri ) ) );
    }

    @Test
    public void shouldGenerateWADLWhenExplicitlyEnabledInConfig() throws Exception
    {
        server = serverOnRandomPorts().withProperty( ServerSettings.wadl_enabled.name(), TRUE )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        var wadlUri = URI.create( server.baseUri() + "application.wadl" );
        var request = HttpRequest.newBuilder( wadlUri ).GET().header( CONTENT_TYPE, WILDCARD ).build();
        var response = newHttpClient().send( request, ofString() );

        assertEquals( 200, response.statusCode() );
        assertEquals( "application/vnd.sun.wadl+xml", response.headers().allValues( "Content-Type" ).iterator().next() );
        assertThat( response.body(), containsString( "<application xmlns=\"http://wadl.dev.java.net/2009/02\">" ) );
    }

    @Test
    public void shouldNotGenerateWADLWhenNotExplicitlyEnabledInConfig() throws Exception
    {
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        var uri = URI.create( server.baseUri() + "application.wadl" );
        var request = HttpRequest.newBuilder( uri ).GET().header( CONTENT_TYPE, WILDCARD ).build();
        var response = newHttpClient().send( request, ofString() );

        assertEquals( 404, response.statusCode() );
    }

    @Test
    public void shouldNotGenerateWADLWhenExplicitlyDisabledInConfig() throws Exception
    {
        server = serverOnRandomPorts().withProperty( ServerSettings.wadl_enabled.name(), FALSE )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        var warlUri = URI.create( server.baseUri() + "application.wadl" );
        var request = HttpRequest.newBuilder( warlUri ).GET().header( CONTENT_TYPE, WILDCARD ).build();
        var response = newHttpClient().send( request, ofString() );

        assertEquals( 404, response.statusCode() );
    }
}
