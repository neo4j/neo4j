/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import javax.ws.rs.core.MediaType;

import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.scripting.javascript.GlobalJavascriptInitializer;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityServerBuilder.server;
import static org.neo4j.test.server.HTTP.POST;

public class ServerConfigDocIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Test
    public void shouldPickUpPortFromConfig() throws Exception
    {
        final int NON_DEFAULT_PORT = 4321;

        server = server().onPort( NON_DEFAULT_PORT )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        assertEquals( NON_DEFAULT_PORT, server.getWebServerPort() );

        JaxRsResponse response = new RestRequest( server.baseUri() ).get();

        assertThat( response.getStatus(), is( 200 ) );
        response.close();
    }

    @Test
    public void shouldPickupRelativeUrisForWebAdminAndWebAdminRest() throws IOException
    {
        String webAdminDataUri = "/a/different/webadmin/data/uri/";
        String webAdminManagementUri = "/a/different/webadmin/management/uri/";

        server = server().withRelativeWebDataAdminUriPath( webAdminDataUri )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withRelativeWebAdminUriPath( webAdminManagementUri )
                .build();
        server.start();

        JaxRsResponse response = new RestRequest().get( "http://localhost:7474" + webAdminDataUri,
                MediaType.TEXT_HTML_TYPE );
        assertEquals( 200, response.getStatus() );

        response = new RestRequest().get( "http://localhost:7474" + webAdminManagementUri );
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGenerateWADLWhenExplicitlyEnabledInConfig() throws IOException
    {
        server = server().withProperty( Configurator.WADL_ENABLED, "true" )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        JaxRsResponse response = new RestRequest().get( "http://localhost:7474/application.wadl",
                MediaType.WILDCARD_TYPE );

        assertEquals( 200, response.getStatus() );
        assertEquals( "application/vnd.sun.wadl+xml", response.getHeaders().get( "Content-Type" ).iterator().next() );
        assertThat( response.getEntity(), containsString( "<application xmlns=\"http://wadl.dev.java" +
                                                          ".net/2009/02\">" ) );
    }

    @Test
    public void shouldNotGenerateWADLWhenNotExplicitlyEnabledInConfig() throws IOException
    {
        server = server()
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        JaxRsResponse response = new RestRequest().get( "http://localhost:7474/application.wadl",
                MediaType.WILDCARD_TYPE );

        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldNotGenerateWADLWhenExplicitlyDisabledInConfig() throws IOException
    {
        server = server().withProperty( Configurator.WADL_ENABLED, "false" )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        JaxRsResponse response = new RestRequest().get( "http://localhost:7474/application.wadl",
                MediaType.WILDCARD_TYPE );

        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldEnableWebadminByDefault() throws IOException
    {
        // Given
        server = server().usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() ).build();
        server.start();

        // When & then
        assertEquals( 200, new RestRequest().get( "http://localhost:7474/webadmin" ).getStatus() );
        assertEquals( 200, new RestRequest().get( "http://localhost:7474/db/manage/server/console" ).getStatus() );
    }

    @Test
    public void shouldDisableWebadminWhenAskedTo() throws IOException
    {
        // Given
        server = server().withProperty( ServerInternalSettings.webadmin_enabled.name(), "false" )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        // When & then
        assertEquals( 404, new RestRequest().get( "http://localhost:7474/webadmin" ).getStatus() );
        assertEquals( 404, new RestRequest().get( "http://localhost:7474/db/manage/server/console" ).getStatus() );
    }

    @Test
    public void shouldEnableRRDbWhenAskedTo() throws IOException
    {
        // Given
        server = server().withProperty( ServerInternalSettings.rrdb_enabled.name(), "true" )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        // When & then
        assertEquals( 200, new RestRequest().get( "http://localhost:7474/db/manage/server/monitor" ).getStatus() );
    }

    @Test
    public void shouldDisableRRDBByDefault() throws IOException
    {
        // Given
        server = server().usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() ).build();
        server.start();

        // When & then
        assertEquals( 404, new RestRequest().get( "http://localhost:7474/db/manage/server/monitor" ).getStatus() );
    }

    @Test
    public void shouldHaveSandboxingEnabledByDefault() throws Exception
    {
        // Given
        server = server()
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        String node = POST( server.baseUri().toASCIIString() + "db/data/node" ).location();

        // When
        JaxRsResponse response = new RestRequest().post( node + "/traverse/node", "{\n" +
                "  \"order\" : \"breadth_first\",\n" +
                "  \"return_filter\" : {\n" +
                "    \"body\" : \"position.getClass().getClassLoader()\",\n" +
                "    \"language\" : \"javascript\"\n" +
                "  },\n" +
                "  \"prune_evaluator\" : {\n" +
                "    \"body\" : \"position.getClass().getClassLoader()\",\n" +
                "    \"language\" : \"javascript\"\n" +
                "  },\n" +
                "  \"uniqueness\" : \"node_global\",\n" +
                "  \"relationships\" : [ {\n" +
                "    \"direction\" : \"all\",\n" +
                "    \"type\" : \"knows\"\n" +
                "  }, {\n" +
                "    \"direction\" : \"all\",\n" +
                "    \"type\" : \"loves\"\n" +
                "  } ],\n" +
                "  \"max_depth\" : 3\n" +
                "}", MediaType.APPLICATION_JSON_TYPE );

        // Then
        assertEquals( 400, response.getStatus() );
    }

    /*
     * We can't actually test that disabling sandboxing works, because of the set-once global nature of Rhino
     * security. Instead, we test here that changing it triggers the expected exception, letting us know that
     * the code that *would* have set it to disabled realizes it has already been set to sandboxed.
     *
     * This at least lets us know that the configuration attribute gets picked up and used.
     */
    @Test(expected = RuntimeException.class)
    public void shouldBeAbleToDisableSandboxing() throws Exception
    {
        // NOTE: This has to be initialized to sandboxed, because it can only be initialized once per JVM session,
        // and all other tests depend on it being sandboxed.
        GlobalJavascriptInitializer.initialize( GlobalJavascriptInitializer.Mode.SANDBOXED );

        server = server().withProperty( Configurator.SCRIPT_SANDBOXING_ENABLED_KEY, "false" )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        // When
        server.start();
    }
}
