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

package org.neo4j.server;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.ServerBuilder.server;

import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class ServerConfigTest {

    private static final int NON_DEFAULT_PORT = 54321;


    @Test
    public void shouldPickUpPortFromConfig() throws Exception {

        NeoServer server = server().withRandomDatabaseDir().withPassingStartupHealthcheck().onPort(
                NON_DEFAULT_PORT ).build();
        server.start();

        assertEquals(NON_DEFAULT_PORT, server.getWebServerPort());

        Client client = Client.create();
        ClientResponse response = client.resource(server.baseUri()).get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));

        server.stop();
    }
}
