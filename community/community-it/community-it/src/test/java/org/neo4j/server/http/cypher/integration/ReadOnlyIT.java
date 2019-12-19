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
package org.neo4j.server.http.cypher.integration;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.helpers.WebContainerHelper.cleanTheDatabase;
import static org.neo4j.server.helpers.WebContainerHelper.createReadOnlyContainer;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class ReadOnlyIT extends ExclusiveWebContainerTestBase
{
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();
    private TestWebContainer readOnlyContainer;
    private HTTP.Builder http;

    @Before
    public void setup() throws Exception
    {
        cleanTheDatabase( readOnlyContainer );
        readOnlyContainer = createReadOnlyContainer( dir.homeDir() );
        http = HTTP.withBaseUri( readOnlyContainer.getBaseUri() );
    }

    @After
    public void teardown()
    {
        if ( readOnlyContainer != null )
        {
            readOnlyContainer.shutdown();
        }
    }

    @Test
    public void shouldReturnReadOnlyStatusWhenCreatingNodes() throws Exception
    {
        // Given
        HTTP.Response response = http.POST( txEndpoint(),
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
        HTTP.Response response = http.POST( txEndpoint(),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (node:Node)' } ] }" ) );

        // Then
        JsonNode error = response.get( "errors" ).get( 0 );
        String code = error.get( "code" ).asText();
        String message = error.get( "message" ).asText();

        assertEquals( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", code );
        assertThat( message, containsString( "This is a read only Neo4j instance" ) );
    }

}
