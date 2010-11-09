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

package org.neo4j.server.webadmin;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.webadmin.rest.ExportService;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ExportFunctionalTest {
    public static NeoServer server;
    public static int serverPort;

    @BeforeClass
    public static void startServer() throws Exception {

        ServerTestUtils.nukeServer();
        ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldGet200ForProperRequest() throws Exception {
        Client client = Client.create();
        
        WebResource getResource = client.resource(NeoServer.server().managementApiUri() + ExportService.ROOT_PATH);

        ClientResponse getResponse = getResource.accept(MediaType.APPLICATION_JSON).post(ClientResponse.class);

        assertEquals(200, getResponse.getStatus());

    }
}
