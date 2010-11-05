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

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.ServerTestUtils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class GetIndexRootFunctionalTest {
    @BeforeClass
    public static void startServer() {
        ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();

    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldRespondWithIndexes() throws Exception {
        ClientResponse response = Client.create().resource(FunctionalTestUtil.indexUri()).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }

    // TODO More tests...
}
