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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.PrettyJSON;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.dummy.web.service.DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT;
import static org.junit.Assert.assertEquals;

import static org.neo4j.server.helpers.CommunityServerBuilder.server;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;

public class BatchOperationHeaderDocIT extends ExclusiveServerTestBase
{
    private NeoServer server;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void cleanTheDatabase() throws IOException
    {
        server = server().withThirdPartyJaxRsPackage( "org.dummy.web.service",
                DUMMY_WEB_SERVICE_MOUNT_POINT ).usingDatabaseDir( folder.getRoot().getAbsolutePath() ).build();
        server.start();
    }

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void shouldPassHeaders() throws Exception
    {
        String jsonData = new PrettyJSON()
                .array()
                .object()
                .key( "method" ).value( "GET" )
                .key( "to" ).value( "../.." + DUMMY_WEB_SERVICE_MOUNT_POINT + "/needs-auth-header" )
                .key( "body" ).object().endObject()
                .endObject()
                .endArray()
                .toString();

        JaxRsResponse response = new RestRequest( null, "user", "pass" )
                .post( "http://localhost:7474/db/data/batch", jsonData );

        assertEquals( 200, response.getStatus() );

        final List<Map<String, Object>> responseData = jsonToList( response.getEntity() );

        Map<String, Object> res = (Map<String, Object>) responseData.get( 0 ).get( "body" );

        /*
         * {
         *   Accept=[application/json],
         *   Content-Type=[application/json],
         *   Authorization=[Basic dXNlcjpwYXNz],
         *   User-Agent=[Java/1.6.0_27] <-- ignore that, it changes often
         *   Host=[localhost:7474],
         *   Connection=[keep-alive],
         *   Content-Length=[86]
         * }
         */
        assertEquals( "Basic dXNlcjpwYXNz", res.get( "Authorization" ) );
        assertEquals( "application/json", res.get( "Accept" ) );
        assertEquals( "application/json", res.get( "Content-Type" ) );
        assertEquals( "localhost:7474", res.get( "Host" ) );
        assertEquals( "keep-alive", res.get( "Connection" ) );
    }
}
