/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.Test;

import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class DefaultResponseFormatIT extends AbstractRestFunctionalTestBase
{
    HTTP.Builder http = HTTP.withBaseUri( container().getBaseUri() );

    @Test
    void testDefaultResponseFormatIsJson() throws JsonParseException
    {
        // Given
        HTTP.Response response = http.POST( txCommitUri(), HTTP.RawPayload.quotedJson( "{ 'statements': [ { 'statement': 'RETURN 1' } ] }" ) );

        // Then
        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response.header( HttpHeaders.CONTENT_TYPE ) ).isEqualTo( MediaType.APPLICATION_JSON );

        Map<String,Object> result = jsonToMap( response.rawContent() );
        assertThat( result.containsKey( "results" ) ).isTrue();
    }
}
