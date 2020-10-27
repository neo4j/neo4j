/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.ThreadLocalRandom;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.server.http.cypher.format.output.eventsource.EventSourceMessageBodyWriter;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

class JoltResultFormatIT extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( container().getBaseUri() )
                                          .withHeaders( HttpHeaders.ACCEPT, EventSourceMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE );

    private String commitResource;

    @BeforeEach
    void setUp()
    {
        // begin
        Response begin = http.POST( txUri() );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin );
        try
        {
            commitResource = begin.get( "info" ).get( "commit" ).asText();
        }
        catch ( JsonParseException e )
        {
            fail( "Exception caught when setting up test: " + e.getMessage() );
        }
        assertThat( commitResource ).isEqualTo( begin.location() + "/commit" );
    }

    @ParameterizedTest
    @CsvSource( {"true", "TRUE"} )
    void shouldReturnJoltInStrictFormat( String booleanString )
    {
        // See https://github.com/eclipse/jetty.project/issues/5446
        var cacheBuster = ";cacheBuster=" + ThreadLocalRandom.current().nextLong();
        // execute and commit
        var response =
                http.withHeaders( HttpHeaders.ACCEPT, EventSourceMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE + ";strict=" + booleanString + cacheBuster )
                    .POST( commitResource, queryAsJsonRow( "RETURN 1, 5.5, true" ) );

        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response.header( HttpHeaders.LOCATION ) );
        assertThat( response.rawContent() ).isEqualTo( "{\"header\":{\"fields\":[\"1\",\"5.5\",\"true\"]}}\n" +
                                                     "{\"data\":[{\"Z\":\"1\"},{\"R\":\"5.5\"},{\"?\":\"true\"}]}\n" +
                                                     "{\"summary\":{}}\n" +
                                                     "{\"info\":{\"commit\":\"" + commitResource + "\"}}\n" );
    }

    @Test
    void shouldReturnJoltInSparseFormat()
    {
        // execute and commit
        var response = http.withHeaders( HttpHeaders.ACCEPT, EventSourceMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE )
                              .POST( commitResource, queryAsJsonRow( "RETURN 1, 5.5, true" ) );

        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response.rawContent() ).isEqualTo( "{\"header\":{\"fields\":[\"1\",\"5.5\",\"true\"]}}\n" +
                                                     "{\"data\":[1,{\"R\":\"5.5\"},true]}\n" +
                                                     "{\"summary\":{}}\n" +
                                                     "{\"info\":{\"commit\":\"" + commitResource + "\"}}\n" );
    }

    private HTTP.RawPayload queryAsJsonRow( String query )
    {
        return quotedJson( "{ 'statements': [ { 'statement': '" + query + "' } ] }" );
    }
}
