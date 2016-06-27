/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.discovery;

import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.test.server.EntityOutputFormat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiscoveryServiceTest
{
    private String baseUri;
    private HostnamePort boltAddress;
    private URI dataUri;
    private URI managementUri;

    @Before
    public void setUp() throws URISyntaxException
    {
        baseUri = "http://www.example.com";
        boltAddress = new HostnamePort( "example.com:7687" );
        dataUri = new URI( "/data" );
        managementUri = new URI( "/management" );
    }

    private Config mockConfig() throws URISyntaxException
    {
        Config mockConfig = mock( Config.class );
        when( mockConfig.get( GraphDatabaseSettings.auth_enabled ) ).thenReturn( false );
        when( mockConfig.get( GraphDatabaseSettings.bolt_advertised_address ) ).thenReturn( boltAddress );
        when( mockConfig.get( ServerSettings.management_api_path ) ).thenReturn( managementUri );
        when( mockConfig.get( ServerSettings.rest_api_path ) ).thenReturn( dataUri );
        return mockConfig;
    }

    private DiscoveryService testDiscoveryService() throws URISyntaxException
    {
        Config mockConfig = mockConfig();
        return new DiscoveryService( mockConfig, new EntityOutputFormat( new JsonFormat(), new URI(
                baseUri ), null ) );
    }

    @Test
    public void shouldReturnValidJSON() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument();
        String json = new String( (byte[]) response.getEntity() );

        assertNotNull( json );
        assertThat( json.length(), is( greaterThan( 0 ) ) );
        assertThat( json, is( not( "\"\"" ) ) );
        assertThat( json, is( not( "null" ) ) );
    }

    @Test
    public void shouldReturnBoltURI() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument();
        String json = new String( (byte[]) response.getEntity() );
        assertThat( json, containsString( "\"bolt\" : \"bolt://" + boltAddress + "\"" ) );
    }

    @Test
    public void shouldReturnDataURI() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument();
        String json = new String( (byte[]) response.getEntity() );
        assertThat( json, containsString( "\"data\" : \"" + baseUri + dataUri + "/\"" ) );
    }

    @Test
    public void shouldReturnManagementURI() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument();
        String json = new String( (byte[]) response.getEntity() );
        assertThat( json, containsString( "\"management\" : \"" + baseUri + managementUri + "/\"" ) );
    }

    @Test
    public void shouldReturnRedirectToAbsoluteAPIUsingOutputFormat() throws Exception
    {
        Config mockConfig = mock( Config.class );
        URI browserUri = new URI( "/browser/" );
        when( mockConfig.get( ServerSettings.browser_path ) ).thenReturn(
                browserUri );

        String baseUri = "http://www.example.com:5435";
        DiscoveryService ds = new DiscoveryService( mockConfig, new EntityOutputFormat( new JsonFormat(), new URI(
                baseUri ), null ) );

        Response response = ds.redirectToBrowser();

        assertThat( response.getMetadata().getFirst( "Location" ), is( (Object) new URI( "http://www.example" +
                ".com:5435/browser/" ) ) );
    }
}
