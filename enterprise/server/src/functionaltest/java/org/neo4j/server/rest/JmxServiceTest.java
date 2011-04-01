/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.modules.DiscoveryModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.WebAdminModule;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class JmxServiceTest
{
    private NeoServerWithEmbeddedWebServer server;
    private FunctionalTestHelper functionalTestHelper;

    @Before
    public void setupServer() throws IOException {
        // FIXME: is it bad that we need all modules here in order to operate?
        // The reason we split the bootstrap class was to be able to load
        // different modules for different product lines... needs fixing.
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().withSpecificServerModules(
                DiscoveryModule.class, RESTApiModule.class, ManagementApiModule.class, ThirdPartyJAXRSModule.class,
                WebAdminModule.class ).build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
    }

    @After
    public void stopServer() {
        server.stop();
        server = null;
    }

    @Test
    public void shouldRespondWithTheWebAdminClientSettings() throws Exception {
        String url = functionalTestHelper.mangementUri() + "/server/jmx";
        ClientResponse resp = Client.create().resource(url).accept( MediaType.APPLICATION_JSON_TYPE ).get(ClientResponse.class);
        String json = resp.getEntity(String.class);

        assertEquals( json, 200, resp.getStatus() );
        assertThat( json, containsString( "resources" ) );
        assertThat( json, containsString( "jmx/domain/{domain}/{objectName}" ) );
    }
}
