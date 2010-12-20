/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.plugins.ReferenceNode;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class ExtensionListingFunctionalTest
{
    private NeoServer server;
    private FunctionalTestHelper functionalTestHelper;

    @Before
    public void startServer() throws IOException
    {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @After
    public void stopServer()
    {
        server.stop();
    }

    @Test
    public void datarootContainsReferenceToExtensions() throws Exception
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.dataUri() ).accept(
                MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        assertThat( response.getStatus(), equalTo( 200 ) );
        Map<String, Object> json = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        String extInfo = (String) json.get( "extensions-info" );
        assertNotNull( new URI( extInfo ) );
    }

    @Test
    public void canListAllAvailableServerExtensions() throws Exception
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.extensionUri() ).accept(
                MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        assertThat( response.getStatus(), equalTo( 200 ) );
        Map<String, Object> json = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        assertFalse( json.isEmpty() );
    }

    @Test
    public void canListExtensionMethodsForServerExtension() throws Exception
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.extensionUri() ).accept(
                MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        assertThat( response.getStatus(), equalTo( 200 ) );
        Map<String, Object> json = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        String refNodeService = (String) json.get( ReferenceNode.class.getSimpleName() );

        response = Client.create().resource( refNodeService ).accept(
                MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        assertThat( response.getStatus(), equalTo( 200 ) );
        json = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        System.out.println( json );
        json = (Map<String, Object>) json.get( "graphdb" );
        assertThat( json, hasKey( ReferenceNode.GET_REFERENCE_NODE ) );
    }
}
