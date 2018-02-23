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
package org.neo4j.server.rest.transactional.integration;

import org.codehaus.jackson.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class ReadOnlyIT extends ExclusiveServerTestBase
{
    private NeoServer readOnlyServer;
    private HTTP.Builder http;

    @BeforeEach
    public void setup() throws IOException
    {
        ServerHelper.cleanTheDatabase( readOnlyServer );
        readOnlyServer = ServerHelper.createNonPersistentReadOnlyServer();
        http = HTTP.withBaseUri( readOnlyServer.baseUri() );
    }

    @AfterEach
    public void teardown()
    {
        if ( readOnlyServer != null )
        {
            readOnlyServer.stop();
        }
    }

    @Test
    public void shouldReturnReadOnlyStatusWhenCreatingNodes() throws Exception
    {
        // Given
        HTTP.Response response = http.POST( "db/data/transaction/commit",
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (node)' } ] }" ) );

        // Then
        JsonNode error = response.get( "errors" ).get( 0 );
        String code = error.get( "code" ).asText();
        String message = error.get( "message" ).asText();

        assertEquals( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", code );
        assertThat( message, containsString( "This is a read only Neo4j instance" ) );
    }

    @Test
    public void shouldReturnReadOnlyStatusWhenCreatingNodesWhichTransitivelyCreateTokens() throws Exception
    {
        // Given
        // When
        HTTP.Response response = http.POST( "db/data/transaction/commit",
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (node:Node)' } ] }" ) );

        // Then
        JsonNode error = response.get( "errors" ).get( 0 );
        String code = error.get( "code" ).asText();
        String message = error.get( "message" ).asText();

        assertEquals( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", code );
        assertThat( message, containsString( "This is a read only Neo4j instance" ) );
    }

}
